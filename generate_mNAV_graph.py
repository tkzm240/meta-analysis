# === 必要パッケージ ===
import os, numpy as np, pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import statsmodels.api as sm
import gspread
from google.oauth2.service_account import Credentials
from IPython.display import display
from plotly.colors import sample_colorscale, hex_to_rgb
import warnings
from statsmodels.tools.sm_exceptions import IterationLimitWarning

# 収束警告は表示しない（必要なら外してください）
warnings.simplefilter("ignore", IterationLimitWarning)

# ===== 強調する q の上下（環境変数で上書き可）=====
HILO_MIN = float(os.getenv("HILO_MIN", 0.05))
HILO_MAX = float(os.getenv("HILO_MAX", 0.98))

# --------- 設定（環境変数で上書き可）---------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1OdhLsAZYVsFz5xcGeuuiuH7JoYyzz6AaG0j2A9Jw1_4").replace("Ja","zz")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "データシート")
KEY_PATH       = os.getenv("KEY_PATH", "/content/optimal-bivouac-471208-f4-ec84cb2443af.json")

# --------- Google Sheets 読み込み ---------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
raw = ws.get_all_values()

# --------- 前処理（重複ヘッダ対応）---------
orig = raw[0]
seen, headers = {}, []
for h in orig:
    if h in seen:
        seen[h]+=1; headers.append(f"{h}_{seen[h]}")
    else:
        seen[h]=0; headers.append(h if h else "Unnamed")
df = pd.DataFrame(raw[1:], columns=headers)

# --------- 列の特定（スクショ準拠：D,F,L,I）---------
# D: 1000株あたりのBTC（数量）
col_btc_per_1000 = next((c for c in df.columns if ("1000" in str(c) and "BTC" in str(c))), None)
if col_btc_per_1000 is None: col_btc_per_1000 = df.columns[3]

# F: BTC Price ($)
col_btc_price_usd = next((c for c in df.columns if ("BTC" in str(c) and ("Price" in str(c) or "価格" in str(c)) and "$" in str(c))), None)
if col_btc_price_usd is None: col_btc_price_usd = df.columns[5]

# L: BTC価格(万円)
col_btc_price_jpy_man = next((c for c in df.columns if (("BTC" in str(c)) or ("ビットコイン" in str(c))) and ("万円" in str(c))), None)
if col_btc_price_jpy_man is None: col_btc_price_jpy_man = df.columns[11]

# I: mNAV
col_mnav = next((c for c in df.columns if str(c).strip().lower()=="mnav" or "mnav" in str(c).lower()), None)
if col_mnav is None: col_mnav = df.columns[8]

# Date
date_col = next((c for c in df.columns if str(c).strip().lower()=="date"), df.columns[0])

# 株価列（任意・あれば使用：最新の基準株価計算で使う）
candidate_stock_cols = [c for c in df.columns if ('株価' in str(c)) or ('share' in str(c).lower() and 'price' in str(c).lower())]
stock_col = candidate_stock_cols[0] if candidate_stock_cols else None

# --------- クリーニング ---------
def clean_numeric_series(s: pd.Series):
    s = pd.Series(s).astype(str).str.strip()
    s = s.replace(['-', '—', '–', '', 'N/A', 'NA', '#N/A', '#VALUE!', '#DIV/0!', 'nan', 'None'], np.nan)
    s = s.str.replace(r'[,\s¥$]', '', regex=True)
    s = s.str.replace(r'^\((.*)\)$', r'-\1', regex=True)
    return s

def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(clean_numeric_series(series), errors='coerce')

# --------- 数値化 ---------
df[date_col]              = pd.to_datetime(df[date_col], errors="coerce")
df[col_mnav]              = to_float(df[col_mnav])
df[col_btc_per_1000]      = to_float(df[col_btc_per_1000])
df[col_btc_price_usd]     = to_float(df[col_btc_price_usd])
df[col_btc_price_jpy_man] = to_float(df[col_btc_price_jpy_man])
if stock_col is not None:
    df[stock_col] = to_float(df[stock_col])

# --------- 便利関数（最新値）---------
def last_valid_val(series):
    s = series.dropna()
    return s.iloc[-1] if len(s) else np.nan

latest_btc_usd       = float(last_valid_val(df[col_btc_price_usd]))
latest_btc_jpy       = float(last_valid_val(df[col_btc_price_jpy_man]) * 10000.0) if pd.notna(last_valid_val(df[col_btc_price_jpy_man])) else np.nan
latest_stock         = float(last_valid_val(df[stock_col])) if stock_col else np.nan
latest_btc_per1000   = float(last_valid_val(df[col_btc_per_1000]))

# --------- NAV（1000株あたり）計算（USD / JPY）---------
df["BTCNAV1000_USD"] = df[col_btc_per_1000] * df[col_btc_price_usd]                 # USD建て
df["BTCNAV1000_JPY"] = df[col_btc_per_1000] * (df[col_btc_price_jpy_man] * 10000.0) # 円建て

# --------- 有効データ抽出（mNAV用：BTC/1000 を hover に出すため同時に保持）---------
def make_valid(df_all, xcol):
    cols = [date_col, col_mnav, col_btc_per_1000, xcol]
    d = df_all[cols].copy().dropna()
    d = d[(d[col_mnav] > 0) & (d[xcol] > 0) & (d[col_btc_per_1000] > 0)]
    d["log_x"] = np.log10(d[xcol].astype(float))     # x は log10( BTC NAV/1000 [JPY or USD] )
    d["btc1000"] = d[col_btc_per_1000].astype(float) # hover用：保有数（BTC）
    d["y"]     = d[col_mnav].astype(float)           # mNAV（線形）
    d["log_y"] = np.log10(d["y"])
    return d

df_usd = make_valid(df, "BTCNAV1000_USD")
df_jpy = make_valid(df, "BTCNAV1000_JPY")

# --------- “⭐ 現在点”（mNAV用）---------
def latest_star(df_all, xcol):
    mask = df_all[date_col].notna() & df_all[col_mnav].notna() & df_all[xcol].notna() & df_all[col_btc_per_1000].notna()
    if not mask.any():
        return None
    idx = df_all.index[mask][-1]
    return {
        "date": pd.to_datetime(df_all.loc[idx, date_col]),
        "x_log": float(np.log10(df_all.loc[idx, xcol])),
        "y": float(df_all.loc[idx, col_mnav]),
        "btc1000": float(df_all.loc[idx, col_btc_per_1000]),
    }

pt_usd = latest_star(df, "BTCNAV1000_USD")
pt_jpy = latest_star(df, "BTCNAV1000_JPY")

# --------- 分位点回帰（共通） ----------
base_quantiles = sorted(set([0.01,0.03,0.05] + [round(q,2) for q in np.arange(0.1,1.0,0.1)] + [0.95,0.97,0.99]))

def fit_quantiles(d, q_list):
    X = sm.add_constant(pd.Series(d["log_x"].values, name="log_x"))
    y = pd.Series(d["log_y"].values, name="log_y")
    lines = {}
    for q in q_list:
        try:
            res = sm.QuantReg(y, X).fit(q=float(q), max_iter=5000)
            lines[float(q)] = res.params  # pandas.Series（'const','log_x'）
        except Exception:
            pass
    return lines

ql_usd = fit_quantiles(df_usd, base_quantiles)
ql_jpy = fit_quantiles(df_jpy, base_quantiles)

# ============================================================
# 基準株価（円/ mNAV 1）：最新行で算出 → 手法Aに使用
# ============================================================
quantiles = base_quantiles[:]  # 表/hover 用

def compute_baseline_price_yen(df_all, mnav_col, stock_col_name):
    if stock_col_name is None:
        return np.nan, None
    mask = df_all[mnav_col].notna() & df_all[stock_col_name].notna()
    if not mask.any():
        return np.nan, None
    idx = df_all.index[mask][-1]
    latest_mnav = float(df_all.loc[idx, mnav_col])
    latest_stock_y = float(df_all.loc[idx, stock_col_name])
    if not (np.isfinite(latest_mnav) and np.isfinite(latest_stock_y) and latest_mnav>0):
        return np.nan, None
    return latest_stock_y / latest_mnav, idx

baseline_price_yen, baseline_idx = compute_baseline_price_yen(df, col_mnav, stock_col)

# mNAV→手法A（価格）：現在 x での mNAV を予測 → 価格へ変換
def predict_mnav_at_xlog(qlines, xlog, q_list):
    if qlines is None or not np.isfinite(xlog): return None
    return {float(q): 10 ** (float(qlines[float(q)]["const"]) + float(qlines[float(q)]["log_x"]) * xlog)
            for q in q_list if float(q) in qlines}

preds_mnav_at_current = predict_mnav_at_xlog(ql_jpy, pt_jpy["x_log"], quantiles) if pt_jpy else None

# ============================================================
# 表（比較用）：現在 x における各 q の「株価（円）」を 2手法で並べる
#   Method A: mNAV × baseline_price_yen
#   Method B: 10**(log10(Price) 分位回帰の予測値)
# ============================================================
ENFORCE_MONOTONE_Q = True  # q方向の単調化（お好みで）

# ---- log10(株価) 用のデータ＆回帰（Method B） ----
def make_valid_price_df(df_all, date_col, stock_col, col_btc_per_1000, col_btc_price_jpy_man):
    if stock_col is None:
        raise ValueError("株価列（stock_col）が見つかりません。")
    nav1000_jpy = df_all[col_btc_per_1000] * (df_all[col_btc_price_jpy_man] * 10000.0)
    d = pd.DataFrame({
        "date": pd.to_datetime(df_all[date_col], errors="coerce"),
        "price_y": pd.to_numeric(df_all[stock_col], errors="coerce"),
        "nav1000": pd.to_numeric(nav1000_jpy, errors="coerce"),
        "btc1000": pd.to_numeric(df_all[col_btc_per_1000], errors="coerce"),
    }).dropna()
    d = d[(d["price_y"] > 0) & (d["nav1000"] > 0) & (d["btc1000"] > 0)]
    d["log_x"] = np.log10(d["nav1000"].astype(float))
    d["y"]     = np.log10(d["price_y"].astype(float))  # 目的変数は log10(Price)
    d["log_y"] = d["y"]
    return d

df_price = make_valid_price_df(df, date_col, stock_col, col_btc_per_1000, col_btc_price_jpy_man)

def latest_star_price(d):
    if d.empty: return None
    last = d.iloc[-1]
    return {"date": last["date"], "x_log": float(last["log_x"]), "y": float(last["y"]), "btc1000": float(last["btc1000"])}

pt_price = latest_star_price(df_price)

def fit_quantiles_logy(d, q_list):
    X = sm.add_constant(pd.Series(d["log_x"].values, name="log_x"))
    y = pd.Series(d["y"].values, name="log10_price")
    out = {}
    for q in q_list:
        try:
            res = sm.QuantReg(y, X).fit(q=float(q), max_iter=5000)
            out[float(q)] = res.params
        except Exception:
            pass
    return out

ql_price = fit_quantiles_logy(df_price, base_quantiles)

# 予測（Method B）：log10(Price)
def predict_logprice_at_xlog(qlines, xlog, q_list):
    if (qlines is None) or (not np.isfinite(xlog)):
        return None
    out = {}
    for q in q_list:
        qf = float(q)
        if qf not in qlines:
            continue
        a = float(qlines[qf]["const"])
        b = float(qlines[qf]["log_x"])
        out[qf] = a + b * xlog  # log10(Price)
    return out

preds_logp_now = predict_logprice_at_xlog(ql_price, pt_price["x_log"] if pt_price else np.nan, quantiles)

# ---- 比較テーブル作成（線形の“円価格”で統一） ----
def make_combined_price_table(preds_mnav, preds_log10p, baseline_price_y, q_list, currency="¥"):
    cols = [f"{q:.2f}" for q in q_list]
    rows = []

    # Method A
    if (preds_mnav is None) or (not np.isfinite(baseline_price_y)):
        row_a = ["" for _ in cols]
    else:
        row_a=[]
        for q in q_list:
            mnav = preds_mnav.get(float(q))
            v = mnav * baseline_price_y if (mnav is not None and np.isfinite(mnav)) else np.nan
            row_a.append(f"{currency}{v:,.0f}" if np.isfinite(v) else "")
    rows.append(["mNAV Regression"] + row_a)

    # Method B
    if preds_log10p is None:
        row_b = ["" for _ in cols]
    else:
        row_b=[]
        for q in q_list:
            lp = preds_log10p.get(float(q))
            v = 10**lp if (lp is not None and np.isfinite(lp)) else np.nan
            row_b.append(f"{currency}{v:,.0f}" if np.isfinite(v) else "")
    rows.append(["Stock-Price Regression"] + row_b)

    df_out = pd.DataFrame(rows, columns=["Method"]+cols)
    return df_out

print("📋 現在 x（= 最新の BTC NAV/1000sh）における各 q の“株価(円)”比較テーブル")
combined_table = make_combined_price_table(preds_mnav_at_current, preds_logp_now, baseline_price_yen, quantiles, currency="¥")
display(combined_table)

# ============================================================
# 可視化：連続グラデーション + 代表線 + ⭐
#  上：mNAV vs log10(NAV/1000)   … hover は「BTC/1000株の“保有数(BTC)”＋mNAV」
#  下：log10(Price) vs log10(NAV/1000) … hover は「BTC/1000株の“保有数(BTC)”＋log10 Price」
# ============================================================

def _qk(q): return float(round(float(q), 6))

def get_line_colors(hilo_min, hilo_max):
    return {
        _qk(hilo_min): "rgb(30,60,200)",   # blue
        _qk(0.50):     "rgb(0,140,0)",     # green
        _qk(hilo_max): "rgb(200,30,30)",   # red
    }

# ---- q方向補間でグラデーション（一般：yが線形量のとき）----
def add_smooth_gradient_bands(fig, xg, preds_grid, q_min=0.01, q_max=0.99,
                              colorscale="Turbo", alpha=0.22, dense_n=80):
    qs_known = sorted(float(q) for q in preds_grid.keys() if q_min <= float(q) <= q_max)
    if len(qs_known) < 2:
        return
    Y_known = np.vstack([np.asarray(preds_grid[q], float) for q in qs_known])  # shape=(K, X)
    qs_dense = np.linspace(qs_known[0], qs_known[-1], dense_n)
    XN = Y_known.shape[1]
    Y_dense = np.empty((len(qs_dense), XN), dtype=float)
    for j in range(XN):
        Y_dense[:, j] = np.interp(qs_dense, qs_known, Y_known[:, j])

    # ベース線（透明）
    fig.add_trace(go.Scattergl(
        x=xg, y=Y_dense[0], mode="lines",
        line=dict(width=0, color="rgba(0,0,0,0)"),
        hoverinfo="skip", showlegend=False
    ))
    for i in range(1, len(qs_dense)):
        q_mid = 0.5*(qs_dense[i-1] + qs_dense[i])
        col = sample_colorscale(colorscale, [q_mid])[0]
        if isinstance(col, str) and col.startswith("#"):
            r, g, b = hex_to_rgb(col)
        else:
            r, g, b = [int(v) for v in col[col.find("(")+1:col.find(")")].split(",")]
        fill_rgba = f"rgba({r},{g},{b},{alpha})"
        fig.add_trace(go.Scattergl(
            x=xg, y=Y_dense[i], mode="lines",
            line=dict(width=0, color="rgba(0,0,0,0)"),
            fill="tonexty", fillcolor=fill_rgba,
            hoverinfo="skip", showlegend=False
        ))

# ---- q方向補間（log10(価格) 用：単調化あり）----
def densify_preds_grid_logq(preds_grid, q_min=0.01, q_max=0.99, num=120, enforce_mono=ENFORCE_MONOTONE_Q):
    qs_known = sorted(float(q) for q in preds_grid.keys() if q_min <= float(q) <= q_max)
    if len(qs_known) < 2:
        if len(qs_known) == 0:
            return np.array([]), np.zeros((0, 0))
        y = np.asarray(preds_grid[qs_known[0]], float)
        return np.array(qs_known, float), np.vstack([y for _ in qs_known])
    Y_known = np.vstack([np.asarray(preds_grid[q], float) for q in qs_known])  # [K, X]
    qs_dense = np.linspace(q_min, q_max, num)
    XN = Y_known.shape[1]
    Y_dense = np.empty((len(qs_dense), XN), dtype=float)
    for j in range(XN):
        Y_dense[:, j] = np.interp(qs_dense, qs_known, Y_known[:, j])
    if enforce_mono:
        Y_dense = np.maximum.accumulate(Y_dense, axis=0)
    return qs_dense, Y_dense

def add_smooth_gradient_bands_log(fig, xg, preds_grid,
                                  q_min=0.01, q_max=0.99, num=120,
                                  colorscale="Turbo", alpha=0.24):
    qs, Y = densify_preds_grid_logq(preds_grid, q_min=q_min, q_max=q_max, num=num, enforce_mono=ENFORCE_MONOTONE_Q)
    if qs.size < 2:
        return
    fig.add_trace(go.Scattergl(
        x=xg, y=Y[0], mode="lines",
        line=dict(width=0, color="rgba(0,0,0,0)"),
        showlegend=False, hoverinfo="skip"
    ))
    for i in range(1, len(qs)):
        q_mid = 0.5*(qs[i-1] + qs[i])
        col = sample_colorscale(colorscale, [q_mid])[0]
        if isinstance(col, str) and col.startswith('#'):
            r, g, b = hex_to_rgb(col)
        else:
            r, g, b = [int(v) for v in col[col.find('(')+1:col.find(')')].split(',')]
        rgba = f"rgba({r},{g},{b},{alpha})"
        fig.add_trace(go.Scattergl(
            x=xg, y=Y[i], mode="lines",
            line=dict(width=0, color="rgba(0,0,0,0)"),
            fill="tonexty", fillcolor=rgba,
            showlegend=False, hoverinfo="skip"
        ))

# ---- mNAV 図 ----
def make_plot_axis(axis_name, d, qlines, star_pt, colorscale="Turbo",
                   quantiles_for_hover=None, hilo_min=HILO_MIN, hilo_max=HILO_MAX):
    if d.empty or qlines is None:
        return go.Figure().update_layout(title=f"{axis_name} (no data)")

    qs_dense = np.linspace(0.01, 0.99, 120)
    highlights = np.array([_qk(hilo_min), _qk(0.50), _qk(hilo_max)])
    qs_all   = np.unique(np.concatenate([qs_dense, highlights]))

    # 予測グリッド（mNAVを線形で）
    def _grid_preds(d, qlines_base, q_list):
        x_min, x_max = float(d["log_x"].min()), float(d["log_x"].max())
        x_grid = np.linspace(x_min, x_min + 1.3 * (x_max - x_min), 600)
        qparams = { _qk(k): v for (k,v) in (qlines_base or {}).items() }
        X = sm.add_constant(pd.Series(d["log_x"].values, name="log_x"))
        y = pd.Series(d["log_y"].values, name="log_mnav")
        preds = {}
        for q in q_list:
            qf = _qk(q)
            if qf not in qparams:
                qparams[qf] = sm.QuantReg(y, X).fit(q=qf, max_iter=5000).params
            p = qparams[qf]
            a = float(p.get("const", p.iloc[0] if len(p)>0 else 0.0))
            b = float(p.get("log_x", p.iloc[1] if len(p)>1 else 0.0))
            preds[qf] = 10 ** (a + b * x_grid)   # mNAV
        return x_grid, preds

    xg, preds_grid = _grid_preds(d, qlines, qs_all)

    # 実データ（hover：BTC/1000株の“保有数(BTC)”＋ mNAV）
    fig = go.Figure()
    fig.add_trace(go.Scattergl(
        x=d["log_x"], y=d["y"],
        customdata=d["btc1000"],
        mode="markers",
        marker=dict(size=5, opacity=0.7, color="rgba(80,120,255,0.9)"),
        name="Actual (mNAV)",
        hovertemplate=("BTC / 1,000 sh: %{customdata:,.4f} BTC<br>"
                       "mNAV: %{y:,.4f}<extra></extra>")
    ))

    # 背景グラデーション
    add_smooth_gradient_bands(fig, xg, preds_grid, q_min=0.01, q_max=0.99,
                              colorscale=colorscale, alpha=0.22, dense_n=80)

    # 代表線（q 値のみを表示）
    LINE_COLORS = get_line_colors(hilo_min, hilo_max)
    for q in highlights:
        y_line = preds_grid[_qk(q)]
        fig.add_trace(go.Scattergl(
            x=xg, y=y_line,
            mode="lines",
            line=dict(width=2.8, color=LINE_COLORS[_qk(q)]),
            name=f"q={q:.2f}",
            hovertemplate=("q={q:.2f}<br>"
                           "mNAV: %{y:,.4f}<extra></extra>").replace("{q:.2f}", f"{q:.2f}")
        ))

    # “全 q” まとめ hover（中央線に透明マーカー：q と mNAV 一覧）
    qs_hover = quantiles_for_hover or quantiles
    texts=[]
    for i in range(len(xg)):
        lines=[]
        for q in qs_hover:
            qn=_qk(q)
            if qn in preds_grid:
                lines.append(f"q={q:.2f}: mNAV {preds_grid[qn][i]:,.4f}")
        texts.append("<br>".join(lines))
    fig.add_trace(go.Scattergl(
        x=xg, y=preds_grid[_qk(0.50)],
        mode="markers", marker=dict(size=8, color="rgba(0,0,0,0)"),
        hovertemplate="%{text}<extra></extra>", text=texts, showlegend=False
    ))

    # ⭐（hover：BTC/1000株の保有量＋mNAV）
    if star_pt:
        fig.add_trace(go.Scattergl(
            x=[star_pt["x_log"]], y=[star_pt["y"]],
            mode="markers",
            marker=dict(symbol='star', size=16, line=dict(width=1, color='black'), color="yellow"),
            name="現在",
            hovertemplate=(f"📅: {star_pt['date'].strftime('%Y-%m-%d')}<br>"
                           f"BTC / 1,000 sh: {star_pt['btc1000']:,.4f} BTC<br>"
                           f"mNAV: {star_pt['y']:,.4f}<extra></extra>")
        ))

    fig.update_layout(
        title=f"mNAV vs log10(BTC NAV per 1,000 shares) [{axis_name}]（q={HILO_MIN:.2f}/0.50/{HILO_MAX:.2f}）",
        xaxis_title=f"log10( BTC NAV per 1,000 shares [{axis_name}] )",
        yaxis_title="mNAV",
        template="plotly_white",
        hovermode="x unified",
        width=1000, height=620,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

# ---- log10(価格) 図 ----
def make_plot_axis_price_log(d, qlines, star_pt, colorscale="Turbo",
                             hilo_min=HILO_MIN, hilo_max=HILO_MAX):
    if d.empty or qlines is None:
        return go.Figure().update_layout(title="log10(Price) vs log10(BTC NAV per 1,000 shares) (no data)")

    qs_dense = np.linspace(0.01, 0.99, 120)
    highlights = np.array([_qk(hilo_min), _qk(0.50), _qk(hilo_max)])
    qs_all = np.unique(np.concatenate([qs_dense, highlights]))

    # 予測グリッド（log10(Price)）
    def _grid_preds_log(d, qlines_base, q_list):
        x_min, x_max = float(d["log_x"].min()), float(d["log_x"].max())
        x_grid = np.linspace(x_min, x_min + 1.3 * (x_max - x_min), 600)
        qparams = { _qk(k): v for (k,v) in (qlines_base or {}).items() }
        X = sm.add_constant(pd.Series(d["log_x"].values, name="log_x"))
        y = pd.Series(d["y"].values, name="log10_price")
        preds = {}
        for q in q_list:
            qf = _qk(q)
            if qf not in qparams:
                qparams[qf] = sm.QuantReg(y, X).fit(q=qf, max_iter=5000).params
            p = qparams[qf]
            a = float(p.get("const", p.iloc[0] if len(p)>0 else 0.0))
            b = float(p.get("log_x", p.iloc[1] if len(p)>1 else 0.0))
            preds[qf] = a + b * x_grid        # log10(Price)
        return x_grid, preds

    xg, preds_grid_log = _grid_preds_log(d, qlines, qs_all)

    # 実データ：customdata = [BTC/1000, log10価格, 価格(¥)]
    fig = go.Figure()
    btc1000_actual = d["btc1000"].values
    log10_price_actual = d["y"].values           # 既に log10(Price)
    price_actual = 10.0 ** log10_price_actual
    fig.add_trace(go.Scattergl(
        x=d["log_x"], y=log10_price_actual,      # y も log10(Price)
        customdata=np.c_[btc1000_actual, log10_price_actual, price_actual],
        mode="markers",
        marker=dict(size=5, opacity=0.7, color="rgba(80,120,255,0.9)"),
        name="Actual (log10 Price)",
        hovertemplate=(
            "BTC / 1,000 sh: %{customdata[0]:,.4f} BTC<br>"
            "Price (¥): ¥%{customdata[2]:,.0f}<br>"
            "log10 Price (¥): %{customdata[1]:.4f}"
            "<extra></extra>"
        )
    ))

    # 背景グラデーション（log空間）
    add_smooth_gradient_bands_log(fig, xg, preds_grid_log, q_min=0.01, q_max=0.99, num=120,
                                  colorscale=colorscale, alpha=0.24)

    # 代表線：customdata = [log10価格, 価格(¥)]
    LINE_COLORS = get_line_colors(hilo_min, hilo_max)
    for q in highlights:
        y_line_log = preds_grid_log[_qk(q)]
        y_line_lin = 10.0 ** y_line_log
        fig.add_trace(go.Scattergl(
            x=xg, y=y_line_log,
            customdata=np.c_[y_line_log, y_line_lin],
            mode="lines",
            line=dict(width=2.8, color=LINE_COLORS[_qk(q)]),
            name=f"q={q:.2f}",
            hovertemplate=(
                f"q={q:.2f}<br>"
                "Price (¥): ¥%{customdata[1]:,.0f}<br>"
                "log10 Price (¥): %{customdata[0]:.4f}"
                "<extra></extra>"
            )
        ))

    # まとめ hover（中央線に透明マーカー：q ごとの価格と log10）
    qs_hover = base_quantiles
    texts=[]
    for i in range(len(xg)):
        lines=[]
        for q in qs_hover:
            qn=_qk(q)
            if qn in preds_grid_log:
                lp = preds_grid_log[qn][i]
                lines.append(f"q={q:.2f}: ¥{10**lp:,.0f}  (log10={lp:.4f})")
        texts.append("<br>".join(lines))
    fig.add_trace(go.Scattergl(
        x=xg, y=preds_grid_log[_qk(0.50)],
        mode="markers", marker=dict(size=8, color="rgba(0,0,0,0)"),
        hovertemplate="%{text}<extra></extra>", text=texts, showlegend=False
    ))

    # ⭐（hover：BTC/1000 と 価格/ log10価格）
    if star_pt:
        fig.add_trace(go.Scattergl(
            x=[star_pt["x_log"]], y=[star_pt["y"]],   # y は log10(Price)
            customdata=[[star_pt["btc1000"], star_pt["y"], 10.0**star_pt["y"]]],
            mode="markers",
            marker=dict(symbol='star', size=16, line=dict(width=1, color='black'), color="yellow"),
            name="現在 ",
            hovertemplate=(
                f"📅: {star_pt['date'].strftime('%Y-%m-%d')}<br>"
                "BTC / 1,000 sh: %{customdata[0]:,.4f} BTC<br>"
                "Price (¥): ¥%{customdata[2]:,.0f}<br>"
                "log10 Price (¥): %{customdata[1]:.4f}"
                "<extra></extra>"
            )
        ))

    fig.update_layout(
        title=f"log10(Price ¥) vs log10(BTC NAV per 1,000 shares) — q={hilo_min:.2f}/0.50/{hilo_max:.2f}",
        xaxis_title="log10( BTC NAV per 1,000 shares [JPY] )",
        yaxis_title="log10 Price (¥)",
        template="plotly_white",
        hovermode="x unified",
        width=1000, height=620,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig


# ===== 出力（グラフ） =====
fig_jpy = make_plot_axis("JPY", df_jpy, ql_jpy, pt_jpy, colorscale="Turbo")
fig_price_log = make_plot_axis_price_log(df_price, ql_price, pt_price, colorscale="Turbo")
pio.show(fig_jpy)
pio.show(fig_price_log)


# ===== 騰落率ライン（％）のデフォルト =====
UPPER_ERR = float(os.getenv("RELERR_UPPER", "100"))   # 例：+100%
LOWER_ERR = float(os.getenv("RELERR_LOWER", "-50"))   # 例：-50%

def make_relerr_mnav(d, qlines, star_pt=None, baseline_price_yen=np.nan):
    """
    図1用：mNAVの q=0.50 回帰からの相対誤差（%）を描く。
    hover: BTC/1000, Actual mNAV, Pred mNAV, Error%
    右上に、⭐位置で UPPER/LOWER を満たす mNAV と そこから換算した株価(円)を注記（基準株価があれば）。
    """
    if d.empty or (0.5 not in qlines):
        return go.Figure().update_layout(title="Relative Error from q=0.50 (mNAV)")

    a = float(qlines[0.5]["const"]); b = float(qlines[0.5]["log_x"])
    x_vals = d["log_x"].values
    y_actual = d["y"].values
    y_pred   = 10.0 ** (a + b * x_vals)
    err_pct  = 100.0 * (y_actual - y_pred) / y_pred
    btc1000  = d["btc1000"].values

    fig = go.Figure()
    fig.add_trace(go.Scattergl(
        x=x_vals, y=err_pct, mode="markers",
        marker=dict(size=6, color="rgba(220,60,60,0.9)"),
        name="Relative Error (mNAV)",
        customdata=np.c_[btc1000, y_actual, y_pred],
        hovertemplate=("BTC / 1,000 sh: %{customdata[0]:,.4f} BTC<br>"
                       "Actual mNAV: %{customdata[1]:,.4f}<br>"
                       "Pred mNAV (q=0.50): %{customdata[2]:,.4f}<br>"
                       "Error: %{y:+.2f}%<extra></extra>")
    ))
    fig.add_hline(y=0,        line=dict(color="gray",            dash="dash"))
    fig.add_hline(y=UPPER_ERR,line=dict(color="rgba(30,160,30,0.85)", dash="dot"))
    fig.add_hline(y=LOWER_ERR,line=dict(color="rgba(30,60,200,0.85)", dash="dot"))

    if (star_pt is not None) and np.isfinite(baseline_price_yen) and (baseline_price_yen > 0):
        x_star = float(star_pt["x_log"])
        mnav_pred_star = 10.0 ** (a + b * x_star)
        lines = []
        for pct, tag in [(UPPER_ERR, "UPPER"), (LOWER_ERR, "LOWER")]:
            mnav_target = mnav_pred_star * (1.0 + pct/100.0)
            price_yen   = mnav_target * baseline_price_yen
            lines.append(f"{tag} {pct:+.0f}% → mNAV {mnav_target:,.4f} / Price ¥{price_yen:,.0f}")
        fig.add_annotation(
            x=1, y=1, xref="paper", yref="paper", xanchor="right", yanchor="top",
            text="<br>".join(lines), showarrow=False,
            bgcolor="rgba(255,255,255,0.85)", bordercolor="rgba(0,0,0,0.2)", borderwidth=1
        )

    fig.update_layout(
        title="Relative Error from q=0.50 (mNAV)",
        xaxis_title="log10( BTC NAV per 1,000 shares [JPY] )",
        yaxis_title="Relative Error (%)",
        template="plotly_white",
        hovermode="closest"
    )
    return fig

def make_relerr_logprice(d, qlines, star_pt=None):
    """
    図2用：log10(Price) 回帰（q=0.50）からの“価格ベース”相対誤差（%）。
    hover: BTC/1000, Price(実測/予測), log10(実測/予測), Error%
    右上に⭐位置で UPPER/LOWER を満たす“価格(円)”を注記。
    """
    if d.empty or (0.5 not in qlines):
        return go.Figure().update_layout(title="Relative Error from q=0.50 (Price)")

    a = float(qlines[0.5]["const"]); b = float(qlines[0.5]["log_x"])
    x_vals      = d["log_x"].values
    logP_actual = d["y"].values
    P_actual    = 10.0 ** logP_actual
    logP_pred   = a + b * x_vals
    P_pred      = 10.0 ** logP_pred
    err_pct     = 100.0 * (P_actual - P_pred) / P_pred
    btc1000     = d["btc1000"].values

    fig = go.Figure()
    fig.add_trace(go.Scattergl(
        x=x_vals, y=err_pct, mode="markers",
        marker=dict(size=6, color="rgba(220,120,30,0.95)"),
        name="Relative Error (Price)",
        customdata=np.c_[btc1000, P_actual, P_pred, logP_actual, logP_pred],
        hovertemplate=("BTC / 1,000 sh: %{customdata[0]:,.4f} BTC<br>"
                       "Actual Price (¥): ¥%{customdata[1]:,.0f}<br>"
                       "Pred Price (¥, q=0.50): ¥%{customdata[2]:,.0f}<br>"
                       "log10 Actual: %{customdata[3]:.4f} / Pred: %{customdata[4]:.4f}<br>"
                       "Error: %{y:+.2f}%<extra></extra>")
    ))
    fig.add_hline(y=0,        line=dict(color="gray",            dash="dash"))
    fig.add_hline(y=UPPER_ERR,line=dict(color="rgba(30,160,30,0.85)", dash="dot"))
    fig.add_hline(y=LOWER_ERR,line=dict(color="rgba(30,60,200,0.85)", dash="dot"))

    if star_pt is not None:
        x_star = float(star_pt["x_log"])
        P_pred_star = 10.0 ** (a + b * x_star)
        lines = []
        for pct, tag in [(UPPER_ERR, "UPPER"), (LOWER_ERR, "LOWER")]:
            price_target = P_pred_star * (1.0 + pct/100.0)
            lines.append(f"{tag} {pct:+.0f}% → Price ¥{price_target:,.0f}")
        fig.add_annotation(
            x=1, y=1, xref="paper", yref="paper", xanchor="right", yanchor="top",
            text="<br>".join(lines), showarrow=False,
            bgcolor="rgba(255,255,255,0.85)", bordercolor="rgba(0,0,0,0.2)", borderwidth=1
        )

    fig.update_layout(
        title="Relative Error from q=0.50 (Price)",
        xaxis_title="log10( BTC NAV per 1,000 shares [JPY] )",
        yaxis_title="Relative Error (%)",
        template="plotly_white",
        hovermode="closest"
    )
    return fig

# ===== 既存：2つの主図 =====
fig_jpy       = make_plot_axis("JPY", df_jpy, ql_jpy, pt_jpy, colorscale="Turbo")
fig_price_log = make_plot_axis_price_log(df_price, ql_price, pt_price, colorscale="Turbo")

# ===== 新規：それぞれの騰落率図 =====
fig_rel_mnav  = make_relerr_mnav(df_jpy,   ql_jpy,   star_pt=pt_jpy,   baseline_price_yen=baseline_price_yen)
fig_rel_price = make_relerr_logprice(df_price, ql_price, star_pt=pt_price)

# --- サブプロット用 注記テキストを作るヘルパ ---
def _note_text_relerr_mnav(qlines, star_pt, baseline_price_yen):
    if (star_pt is None) or (0.5 not in qlines) or not np.isfinite(baseline_price_yen) or baseline_price_yen <= 0:
        return None
    a = float(qlines[0.5]["const"]); b = float(qlines[0.5]["log_x"])
    x_star = float(star_pt["x_log"])
    mnav_pred_star = 10.0 ** (a + b * x_star)
    lines = []
    for pct, tag in [(UPPER_ERR, "UPPER"), (LOWER_ERR, "LOWER")]:
        mnav_target = mnav_pred_star * (1.0 + pct/100.0)
        price_yen   = mnav_target * baseline_price_yen
        lines.append(f"{tag} {pct:+.0f}% → mNAV {mnav_target:,.4f} / Price ¥{price_yen:,.0f}")
    return "<br>".join(lines)

def _note_text_relerr_price(qlines, star_pt):
    if (star_pt is None) or (0.5 not in qlines):
        return None
    a = float(qlines[0.5]["const"]); b = float(qlines[0.5]["log_x"])
    x_star = float(star_pt["x_log"])
    P_pred_star = 10.0 ** (a + b * x_star)
    lines = []
    for pct, tag in [(UPPER_ERR, "UPPER"), (LOWER_ERR, "LOWER")]:
        price_target = P_pred_star * (1.0 + pct/100.0)
        lines.append(f"{tag} {pct:+.0f}% → Price ¥{price_target:,.0f}")
    return "<br>".join(lines)


# 左右の x 範囲をそろえるためのヘルパ
def _extended_xrange(d):
    x_min = float(d["log_x"].min())
    x_max = float(d["log_x"].max())
    return [x_min, x_min + 1.3*(x_max - x_min)]

from plotly.subplots import make_subplots

fig_grid = make_subplots(
    rows=2, cols=2,
    subplot_titles=[
        "図1: mNAV vs log10(NAV/1000)",
        "図1の騰落率（q=0.50基準）",
        "図2: log10(Price) vs log10(NAV/1000)",
        "図2の騰落率（q=0.50基準）"
    ],
    horizontal_spacing=0.08, vertical_spacing=0.12
)

# 左上：図1
for tr in fig_jpy.data:
    fig_grid.add_trace(tr, row=1, col=1)

# 右上：図1の騰落率
for tr in fig_rel_mnav.data:
    fig_grid.add_trace(tr, row=1, col=2)
fig_grid.add_hline(y=0,         row=1, col=2, line=dict(color="gray", dash="dash"))
fig_grid.add_hline(y=UPPER_ERR, row=1, col=2, line=dict(color="rgba(30,160,30,0.85)", dash="dot"))
fig_grid.add_hline(y=LOWER_ERR, row=1, col=2, line=dict(color="rgba(30,60,200,0.85)", dash="dot"))
note1 = _note_text_relerr_mnav(ql_jpy, pt_jpy, baseline_price_yen)
if note1:
    fig_grid.add_annotation(
        row=1, col=2,
        x=1, y=1, xref="x domain", yref="y domain",
        xanchor="right", yanchor="top",
        text=note1, showarrow=False,
        bgcolor="rgba(255,255,255,0.85)", bordercolor="rgba(0,0,0,0.2)", borderwidth=1
    )

# 左下：図2
for tr in fig_price_log.data:
    fig_grid.add_trace(tr, row=2, col=1)

# 右下：図2の騰落率
for tr in fig_rel_price.data:
    fig_grid.add_trace(tr, row=2, col=2)
fig_grid.add_hline(y=0,         row=2, col=2, line=dict(color="gray", dash="dash"))
fig_grid.add_hline(y=UPPER_ERR, row=2, col=2, line=dict(color="rgba(30,160,30,0.85)", dash="dot"))
fig_grid.add_hline(y=LOWER_ERR, row=2, col=2, line=dict(color="rgba(30,60,200,0.85)", dash="dot"))
note2 = _note_text_relerr_price(ql_price, pt_price)
if note2:
    fig_grid.add_annotation(
        row=2, col=2,
        x=1, y=1, xref="x domain", yref="y domain",
        xanchor="right", yanchor="top",
        text=note2, showarrow=False,
        bgcolor="rgba(255,255,255,0.85)", bordercolor="rgba(0,0,0,0.2)", borderwidth=1
    )

# 軸ラベル
fig_grid.update_xaxes(title_text="log10( BTC NAV per 1,000 shares [JPY] )", row=1, col=1)
fig_grid.update_yaxes(title_text="mNAV",                          row=1, col=1)
fig_grid.update_xaxes(title_text="log10( BTC NAV per 1,000 shares [JPY] )", row=1, col=2)
fig_grid.update_yaxes(title_text="Relative Error (%)",            row=1, col=2)
fig_grid.update_xaxes(title_text="log10( BTC NAV per 1,000 shares [JPY] )", row=2, col=1)
fig_grid.update_yaxes(title_text="log10 Price (¥)",               row=2, col=1)
fig_grid.update_xaxes(title_text="log10( BTC NAV per 1,000 shares [JPY] )", row=2, col=2)
fig_grid.update_yaxes(title_text="Relative Error (%)",            row=2, col=2)

# 横軸レンジを上下で統一（見た目のズレ防止）
def _extended_xrange(d):
    x_min = float(d["log_x"].min()); x_max = float(d["log_x"].max())
    return [x_min, x_min + 1.3*(x_max - x_min)]
xr_top    = _extended_xrange(df_jpy)
xr_bottom = _extended_xrange(df_price)
fig_grid.update_xaxes(range=xr_top,    row=1, col=1)
fig_grid.update_xaxes(range=xr_top,    row=1, col=2)
fig_grid.update_xaxes(range=xr_bottom, row=2, col=1)
fig_grid.update_xaxes(range=xr_bottom, row=2, col=2)

# それぞれの段（上段／下段）で x をリンク（ズーム・パンを同期）
# 上段：右上 (x2) を 左上 (x) にリンク
fig_grid.update_xaxes(matches="x", row=1, col=2)
# 下段：右下 (x4) を 左下 (x3) にリンク
fig_grid.update_xaxes(matches="x3", row=2, col=2)


# スパイクで縦カーソルも揃える（各パネル内）
fig_grid.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor", spikethickness=1)

# 全体レイアウト（unified でも “サブプロット内” で統一）
fig_grid.update_layout(
    title="Meta-Analysis",
    template="plotly_white",
    width=1200, height=1000,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1)
)

# ---------- ここから追記（凡例の重複排除＆順序整理） ----------
def tidy_legend(fig):
    """
    同名の凡例は最初だけ表示(showlegend=True)、以降は非表示にする。
    さらに、凡例の並び順を希望順に近づける（trace の順序のみで制御）。
    """
    # ① 同名の凡例をデデュープ
    seen = set()
    for tr in fig.data:
        nm = getattr(tr, "name", None)
        if not nm:
            continue
        if nm in seen:
            tr.showlegend = False
        else:
            tr.showlegend = True
            tr.legendgroup = nm  # グループ化（クリック折り畳みで利く）
            seen.add(nm)

    # ② 凡例の並び順（優先度）を決める
    #   ※存在しない名前は末尾に回ります
    desired_order = [
        "q=0.98", "q=0.50", "q=0.05",
        "Actual (mNAV)", "Actual (log10 Price)",
        "Relative Error (mNAV)", "Relative Error (Price)",
        "現在 ⭐",
    ]
    prio = {name: i for i, name in enumerate(desired_order)}

    # data を並べ替え（プロット表示には影響なし／凡例順だけが変わる）
    fig.data = tuple(sorted(fig.data, key=lambda tr: prio.get(getattr(tr, "name", ""), 999)))

    # ③ 凡例の見た目
    fig.update_layout(
        legend=dict(
            orientation="h",               # 横並び
            yanchor="bottom", y=1.03,
            xanchor="center", x=0.5,
            traceorder="normal",           # 並べ替え結果をそのまま
            itemsizing="constant",
            font=dict(size=12),
        )
    )

tidy_legend(fig_grid)
pio.show(fig_grid)


# ------------------------------
# 成果物の書き出し（PNG/HTML/表/README差し替え/モバイルページ）
# ※ 事前に以下4図を作ってある前提：
#   fig_jpy        : 図1 mNAV vs log10(NAV/1000 [JPY])
#   fig_price_log  : 図2 log10(Price) vs log10(NAV/1000 [JPY])
#   fig_rel_mnav   : 図1の騰落率（q=0.50基準）
#   fig_rel_price  : 図2の騰落率（q=0.50基準）
# ------------------------------
os.makedirs("assets", exist_ok=True)
os.makedirs("docs", exist_ok=True)

# 4図をまとめて定義（タイトル・ファイル名・表示名）
figs = [
    {
        "fig": fig_jpy,
        "png": "assets/fig1.png",
        "html": "docs/fig1.html",
        "label": "Chart 1: mNAV vs log10(NAV/1000) [JPY]",
        "anchor": "fig1"
    },
    {
        "fig": fig_price_log,
        "png": "assets/fig2.png",
        "html": "docs/fig2.html",
        "label": "Chart 2: log10(Price) vs log10(NAV/1000) [JPY]",
        "anchor": "fig2"
    },
    {
        "fig": fig_rel_mnav,
        "png": "assets/fig3.png",
        "html": "docs/fig3.html",
        "label": "Chart 3: Relative Error from q=0.50 (mNAV)",
        "anchor": "fig3"
    },
    {
        "fig": fig_rel_price,
        "png": "assets/fig4.png",
        "html": "docs/fig4.html",
        "label": "Chart 4: Relative Error from q=0.50 (Price)",
        "anchor": "fig4"
    },
]

# 画像（PNG）… kaleido が必要（pip install -U kaleido）
for item in figs:
    pio.write_image(item["fig"], item["png"], width=1200, height=720, scale=2)

# インタラクティブ HTML
for item in figs:
    item["fig"].write_html(item["html"], include_plotlyjs="cdn", full_html=True)

# 既存の Summary テーブル（df_summary_disp）がある場合はそれを利用（なければ空でOK）
try:
    summary_md = df_summary_disp.to_markdown(index=False)
except Exception:
    summary_md = "_(no summary table)_"

with open("assets/summary.md", "w", encoding="utf-8") as f:
    f.write(summary_md)

# README 差し替え
from datetime import datetime, timezone, timedelta
JST = timezone(timedelta(hours=9))
ts = datetime.now(JST).strftime("%Y-%m-%d %H:%M (%Z)")

# あなたの Pages ルート（要自分のリポに合わせて変更）
PAGES_URL = "https://tkzm240.github.io/meta-analysis"

# 各図ブロック（リンク→PNGの順）を生成
chart_blocks = []
for item in figs:
    chart_blocks.append(
        f"[Open interactive {item['label']}]({PAGES_URL}/{os.path.basename(item['html'])})\n\n"
        f"![{item['anchor']}]({item['png']})"
    )
charts_md = "\n\n".join(chart_blocks)

block = f"""
**Last update (JST):** {ts}

### Summary
{summary_md}

### Charts
{charts_md}
""".strip()

def replace_between_markers(text, start, end, replacement):
    import re
    pattern = re.compile(rf"({re.escape(start)})(.*)({re.escape(end)})", flags=re.DOTALL)
    return pattern.sub(rf"\\1\n{replacement}\n\\3", text)

readme_path = "README.md"
start_marker = "<!--REPORT:START-->"
end_marker   = "<!--REPORT:END-->"

if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        readme = f.read()

    # マーカーが無ければ末尾に作る
    if start_marker not in readme or end_marker not in readme:
        readme = readme.rstrip() + f"\n\n{start_marker}\n{block}\n{end_marker}\n"

    new_readme = replace_between_markers(readme, start_marker, end_marker, block)
    if new_readme != readme:
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(new_readme)
        print("README updated for 4 charts.")
    else:
        print("README unchanged.")
else:
    # README が無い場合は新規生成
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(f"# Meta Analysis\n\n{start_marker}\n{block}\n{end_marker}\n")
    print("README created.")

# モバイル用ダッシュボード（docs/index.html）
def _safe_html_table(df):
    try:
        return df.to_html(index=False, classes="tbl", border=0, escape=False)
    except Exception:
        return "<p>(no summary)</p>"

table_html = _safe_html_table(df_summary_disp if 'df_summary_disp' in globals() else pd.DataFrame())

fig_snippets = []
for item in figs:
    fig_snippets.append(
        f"""<section class="card">
  <h2 style="margin:0 0 8px;font-size:16px;">{item['label']}</h2>
  {item['fig'].to_html(include_plotlyjs=False, full_html=False)}
  <div style="margin-top:6px;"><a href="{os.path.basename(item['html'])}" target="_blank">Open interactive</a></div>
</section>"""
    )
figs_html = "\n\n".join(fig_snippets)

index_html = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover" />
<title>Meta Analysis — Daily</title>
<link rel="preconnect" href="https://cdn.plot.ly">
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ margin:16px; font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Noto Sans JP", sans-serif; line-height:1.5; }}
  header {{ display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap; }}
  h1 {{ font-size:20px; margin:0 0 6px; }}
  .meta {{ font-size:12px; opacity:.8; }}
  .btns a {{ display:inline-block; padding:10px 14px; border-radius:10px; text-decoration:none; border:1px solid #ccc; margin-left:8px; }}
  .card {{ background:rgba(0,0,0,.03); padding:12px; border-radius:12px; margin:14px 0; }}
  .tbl {{ width:100%; border-collapse:collapse; font-size:14px; }}
  .tbl th, .tbl td {{ padding:10px; border-bottom:1px solid rgba(0,0,0,.1); text-align:right; }}
  .tbl th:first-child, .tbl td:first-child {{ text-align:left; }}
  .sticky-refresh {{ position: fixed; right: 12px; bottom: 12px; z-index: 999; }}
</style>
</head>
<body>
<header>
  <div>
    <h1>Meta Analysis — Daily</h1>
    <div class="meta">Last update (JST): <b>{ts}</b></div>
  </div>
  <div class="btns">
    <a href="../actions" target="_blank">更新（Actions）</a>
    <a href="index.html?nocache={int(datetime.now().timestamp())}">強制リロード</a>
  </div>
</header>

<section class="card">
  <h2 style="margin:0 0 8px;font-size:16px;">Summary</h2>
  {table_html}
</section>

{figs_html}

<a class="sticky-refresh" href="index.html?nocache={int(datetime.now().timestamp())}" title="Refresh">🔄</a>
</body>
</html>"""

with open("docs/index.html", "w", encoding="utf-8") as f:
    f.write(index_html)
print("docs/index.html written with 4 charts.")



