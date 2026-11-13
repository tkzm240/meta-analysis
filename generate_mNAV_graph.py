# generate_mNAV_graph.py
# README に「Summary表 → 図4枚（各図リンク付）」のみを書き出し、docs/ にインタラクティブ図を保存
# 変更点：
# - Bitcoin価格（USD/JPY）と株価（JPY）は公式データAPIから取得
# - 取得に失敗した場合は保存データを使用し、そのデータ日を明記

import csv
import json
import os, re
from html import escape
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

# ===== Plotly =====
import plotly.graph_objects as go
import kaleido
from plotly.colors import sample_colorscale, hex_to_rgb
from plotly.offline import get_plotlyjs_version

# ===== 解析パッケージ =====
import statsmodels.api as sm

# ================== 設定（Actions から環境変数で上書き可） ==================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1OdhLsAZYVsFz5xcGeuuiuH7JoYyzz6AaG0j2A9Jw1_4")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME") or "データシート"
KEY_PATH       = os.getenv("KEY_PATH", "service_account.json")
PAGES_URL      = os.getenv("PAGES_URL", "https://tkzm240.github.io/meta-analysis")
FETCH_FROM_SITE = os.getenv("FETCH_FROM_SITE", "1")  # "1" のときサイトから取得を試みる
RSI_METHOD = os.getenv("RSI_METHOD", "cutler").lower()          # "cutler" or "wilder"
RSI_INCLUDE_CURR_WEEK = os.getenv("RSI_INCLUDE_CURR_WEEK", "1") # "1": 今週の暫定値も含める

# 強調する q（代表線）
HILO_MIN = float(os.getenv("HILO_MIN", 0.02))
HILO_MAX = float(os.getenv("HILO_MAX", 0.98))

# 背景グラデーション帯（qレンジ）
Q_MIN_SHADE = float(os.getenv("Q_MIN_SHADE", "0.005"))
Q_MAX_SHADE = float(os.getenv("Q_MAX_SHADE", "0.995"))

# 騰落率ライン（％）
UPPER_ERR = float(os.getenv("RELERR_UPPER", "100"))  # 例：+100%
LOWER_ERR = float(os.getenv("RELERR_LOWER", "-50"))  # 例：-50%

# ================== Google Sheets / 再取得CSV 読み込み ==================
data_csv = os.getenv("DATA_CSV")
if data_csv:
    with open(data_csv, encoding="utf-8-sig", newline="") as source:
        raw = list(csv.reader(source))
else:
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    sa_json = os.getenv("GSA_JSON")
    if sa_json:
        creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(KEY_PATH, scopes=scopes)
    ws = gspread.authorize(creds).open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    raw = ws.get_all_values()
if len(raw) < 4 or len(raw[0]) < 14:
    raise ValueError("Expected the existing 14-column data schema and at least 3 observations")

# ================== 前処理（重複ヘッダ対応） ==================
orig = raw[0]
seen, headers = {}, []
for h in orig:
    if h in seen:
        seen[h]+=1; headers.append(f"{h}_{seen[h]}")
    else:
        seen[h]=0; headers.append(h if h else "Unnamed")
df = pd.DataFrame([row + [""] * (len(headers) - len(row)) for row in raw[1:]], columns=headers)

# ================== 列の特定（D,F,L,I 想定） ==================
col_btc_per_1000      = next((c for c in df.columns if ("1000" in str(c) and "BTC" in str(c))), df.columns[3])
col_btc_price_usd     = next((c for c in df.columns if ("BTC" in str(c) and ("Price" in str(c) or "価格" in str(c)) and "$" in str(c))), df.columns[5])
col_btc_price_jpy_man = next((c for c in df.columns if (("BTC" in str(c)) or ("ビットコイン" in str(c))) and ("万円" in str(c))), df.columns[11])
col_mnav              = next((c for c in df.columns if str(c).strip().lower()=="mnav" or "mnav" in str(c).lower()), df.columns[8])
date_col              = next((c for c in df.columns if str(c).strip().lower()=="date"), df.columns[0])

# 株価列（あれば使用）
candidate_stock_cols = [c for c in df.columns if ('株価' in str(c)) or ('share' in str(c).lower() and 'price' in str(c).lower())]
stock_col = candidate_stock_cols[0] if candidate_stock_cols else None

# ================== クリーニング（シート用） ==================
def clean_numeric_series(s: pd.Series):
    s = pd.Series(s).astype(str).str.strip()
    s = s.replace(['-', '—', '–', '', 'N/A', 'NA', '#N/A', '#VALUE!', '#DIV/0!', 'nan', 'None'], np.nan)
    s = s.str.replace(r'[,\s¥$]', '', regex=True)
    s = s.str.replace(r'^\((.*)\)$', r'-\1', regex=True)
    return s

def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(clean_numeric_series(series), errors='coerce')

df[date_col]              = pd.to_datetime(df[date_col], format="mixed", errors="coerce")
df[col_mnav]              = to_float(df[col_mnav])
df[col_btc_per_1000]      = to_float(df[col_btc_per_1000])
df[col_btc_price_usd]     = to_float(df[col_btc_price_usd])
df[col_btc_price_jpy_man] = to_float(df[col_btc_price_jpy_man])
if stock_col is not None:
    df[stock_col] = to_float(df[stock_col])
# Re-runs can leave multiple physical rows in an existing sheet. A date is one
# daily observation, so repeated rows must not change regression weighting.
df = (df.sort_values(date_col, kind="stable", na_position="first")
      .drop_duplicates(subset=[date_col], keep="last").reset_index(drop=True))
history_date = df[date_col].max()
if pd.isna(history_date):
    raise ValueError("No valid dates in the history; refusing to publish an empty report")
history_as_of = history_date.strftime("%Y-%m-%d")

# ================== NAV（1000株あたり）計算 ==================
df["BTCNAV1000_USD"] = df[col_btc_per_1000] * df[col_btc_price_usd]
df["BTCNAV1000_JPY"] = df[col_btc_per_1000] * (df[col_btc_price_jpy_man] * 10000.0)

# ================== 有効データ抽出 ==================
def make_valid(df_all, xcol):
    cols = [date_col, col_mnav, col_btc_per_1000, xcol]
    d = df_all[cols].copy().dropna()
    d = d[np.isfinite(d[[col_mnav, col_btc_per_1000, xcol]]).all(axis=1)]
    d = d[(d[col_mnav] > 0) & (d[xcol] > 0) & (d[col_btc_per_1000] > 0)]
    d["log_x"]  = np.log10(d[xcol].astype(float))
    d["btc1000"]= d[col_btc_per_1000].astype(float)
    d["y"]      = d[col_mnav].astype(float)
    d["log_y"]  = np.log10(d["y"])
    return d

df_usd = make_valid(df, "BTCNAV1000_USD")
df_jpy = make_valid(df, "BTCNAV1000_JPY")
if len(df_jpy) < 3 or df_jpy["log_x"].nunique() < 2:
    raise ValueError("Insufficient valid JPY history for regression; report was not replaced")
history_as_of = df_jpy[date_col].max().strftime("%Y-%m-%d")

def latest_star(df_all, xcol):
    mask = (df_all[date_col].notna()
            & np.isfinite(df_all[[col_mnav, xcol, col_btc_per_1000]]).all(axis=1)
            & (df_all[[col_mnav, xcol, col_btc_per_1000]] > 0).all(axis=1))
    if not mask.any(): return None
    idx = df_all.index[mask][-1]
    return {
        "date": pd.to_datetime(df_all.loc[idx, date_col]),
        "x_log": float(np.log10(df_all.loc[idx, xcol])),
        "y": float(df_all.loc[idx, col_mnav]),
        "btc1000": float(df_all.loc[idx, col_btc_per_1000]),
    }

pt_usd = latest_star(df, "BTCNAV1000_USD")
pt_jpy = latest_star(df, "BTCNAV1000_JPY")

# ================== 分位点回帰 ==================
base_quantiles = sorted(set(
    [0.01, 0.02, 0.03, 0.05] +
    [round(q, 2) for q in np.arange(0.10, 1.00, 0.10)] +
    [0.95, 0.97, 0.98, 0.99]
))
quantiles = base_quantiles[:]

def fit_quantiles(d, q_list):
    if d is None or len(d)==0: return {}
    X = sm.add_constant(pd.Series(d["log_x"].values, name="log_x"))
    y = pd.Series(d["log_y"].values, name="log_y")
    lines = {}
    for q in q_list:
        try:
            res = sm.QuantReg(y, X).fit(q=float(q), max_iter=5000)
            lines[float(q)] = res.params
        except Exception:
            pass
    return lines

ql_usd = fit_quantiles(df_usd, base_quantiles)
ql_jpy = fit_quantiles(df_jpy, base_quantiles)
if 0.5 not in ql_jpy:
    raise ValueError("Median NAV regression failed; report was not replaced")

# ================== 基準株価（円 / mNAV 1） ==================
def compute_baseline_price_yen(df_all, mnav_col, stock_col_name):
    if stock_col_name is None: return np.nan, None
    mask = df_all[mnav_col].notna() & df_all[stock_col_name].notna()
    if not mask.any(): return np.nan, None
    idx = df_all.index[mask][-1]
    latest_mnav   = float(df_all.loc[idx, mnav_col])
    latest_stockY = float(df_all.loc[idx, stock_col_name])
    if not (np.isfinite(latest_mnav) and np.isfinite(latest_stockY) and latest_mnav>0):
        return np.nan, None
    return latest_stockY / latest_mnav, idx

baseline_price_yen, baseline_idx = compute_baseline_price_yen(df, col_mnav, stock_col)

# === 最新点の「mNAV の q=0.50 からの乖離率(%)」 ===
def latest_mnav_deviation_pct(df_jpy, qlines):
    try:
        if df_jpy is None or len(df_jpy) == 0 or (0.5 not in qlines):
            return np.nan
        a = float(qlines[0.5]["const"]); b = float(qlines[0.5]["log_x"])
        x = float(df_jpy["log_x"].iloc[-1])
        y_actual = float(df_jpy["y"].iloc[-1])    # 実測 mNAV
        y_pred   = 10.0 ** (a + b * x)            # q=0.50 回帰の mNAV
        return float(100.0 * (y_actual - y_pred) / y_pred)
    except Exception:
        return np.nan

# === 週足RSI(14)：Cutler/Wilder 両対応（SBIに合わせるなら "cutler" 推奨） ===
def _rsi_wilder_series(close_w: pd.Series, n: int = 14) -> pd.Series:
    close_w = close_w.dropna().astype(float)
    if len(close_w) < n + 1:
        return pd.Series(np.nan, index=close_w.index)

    delta = close_w.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    # 初期平均はSMA
    ag = gain.rolling(n, min_periods=n).mean()
    al = loss.rolling(n, min_periods=n).mean()

    rsi = pd.Series(np.nan, index=close_w.index)
    valid_idx = np.where(~ag.isna() & ~al.isna())[0]
    if len(valid_idx) == 0:
        return rsi
    i0 = valid_idx[0]

    avg_gain = ag.iloc[i0]
    avg_loss = al.iloc[i0]
    rs = np.inf if avg_loss == 0 else (avg_gain / avg_loss)
    rsi.iloc[i0] = 100.0 - 100.0 / (1.0 + rs)

    # Wilder 再帰（平滑）
    for j in range(i0 + 1, len(close_w)):
        cg = gain.iloc[j] if not np.isnan(gain.iloc[j]) else 0.0
        cl = loss.iloc[j] if not np.isnan(loss.iloc[j]) else 0.0
        avg_gain = (avg_gain * (n - 1) + cg) / n
        avg_loss = (avg_loss * (n - 1) + cl) / n
        rs = np.inf if avg_loss == 0 else (avg_gain / avg_loss)
        rsi.iloc[j] = 100.0 - 100.0 / (1.0 + rs)
    return rsi


def _rsi_cutler_series(close_w: pd.Series, n: int = 14) -> pd.Series:
    """期間内の上昇幅合計 / (上昇+下落)合計 ×100（Cutler）。日本の証券アプリで一般的。"""
    close_w = close_w.dropna().astype(float)
    if len(close_w) < n + 1:
        return pd.Series(np.nan, index=close_w.index)

    delta = close_w.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    sum_gain = gain.rolling(n, min_periods=n).sum()
    sum_loss = loss.rolling(n, min_periods=n).sum()
    denom = (sum_gain + sum_loss).replace(0, np.nan)
    rsi = 100.0 * (sum_gain / denom)
    return rsi


def compute_weekly_rsi_14_from_sheet(date_series, close_series,
                                     override_today=np.nan,
                                     override_date=None,
                                     include_current_week=False,
                                     method: str = "cutler"):
    """
    戻り値: (最新RSI(float), is_provisional(bool))
    method: "cutler"（推奨・SBIに近い） or "wilder"
    include_current_week=False なら“完成済み週のみ”（SBIに合わせやすい）
    """
    try:
        s = pd.Series(pd.to_numeric(close_series, errors="coerce").values,
                      index=pd.to_datetime(date_series, errors="coerce")).dropna()

        # 平日のみ（0=Mon..4=Fri）。土日除外。祝日はデータが無ければ自然に除外されます。
        s = s[s.index.weekday < 5]
        s.index = s.index.normalize()
        s = s.sort_index()
        s = s[~s.index.duplicated(keep="last")]

        # JST 今日
        today = pd.Timestamp.now(tz="Asia/Tokyo").normalize().tz_localize(None)

        # 今週を暫定で入れる（週途中の暫定株価を使いたい場合）
        if include_current_week and np.isfinite(override_today) and override_date is not None:
            quote_date = pd.Timestamp(override_date).tz_localize(None).normalize()
            if quote_date.weekday() < 5:
                s.loc[quote_date] = float(override_today)
            s = s.sort_index()

        # 週足：金曜終値ベース。金曜が休みなら、その週の最終営業日の値が“last”になります
        w = s.resample("W-FRI").last().dropna()

        if not include_current_week:
            # “完成済み週のみ”に揃える
            this_week_fri = (today + pd.offsets.Week(weekday=4)).normalize()
            w = w[w.index < this_week_fri]

        if method.lower() == "wilder":
            rsi_ser = _rsi_wilder_series(w, n=14)
        else:
            rsi_ser = _rsi_cutler_series(w, n=14)

        rsi_ser = rsi_ser.dropna()
        val = float(rsi_ser.iloc[-1]) if len(rsi_ser) else np.nan
        week_start = today - pd.Timedelta(days=today.weekday())
        is_prov = include_current_week and today.weekday() < 4 and s.index.max() >= week_start
        return val, is_prov
    except Exception:
        return np.nan, False

# === 現在の x_log における「q毎の予測株価(¥)リスト」を取得（MethodB優先, Aへフォールバック） ===
# === 現在の x_log における「q毎の予測株価(¥)」: mNAV優先 ===
def quantile_prices_at_current(q_list, preds_logp_now, preds_mnav_now, baseline_price_yen, prefer="mnav"):
    out = []
    prefer = (prefer or "mnav").lower()

    def from_mnav(qk):
        if (preds_mnav_now is not None and qk in preds_mnav_now
                and np.isfinite(preds_mnav_now[qk]) and np.isfinite(baseline_price_yen)):
            return float(preds_mnav_now[qk]) * float(baseline_price_yen)
        return np.nan

    def from_price(qk):
        if preds_logp_now is not None and qk in preds_logp_now and np.isfinite(preds_logp_now[qk]):
            return 10 ** float(preds_logp_now[qk])
        return np.nan

    for q in q_list:
        qk = float(f"{q:.2f}")
        if prefer == "mnav":
            v = from_mnav(qk)
            if not np.isfinite(v):
                v = from_price(qk)
        else:
            v = from_price(qk)
            if not np.isfinite(v):
                v = from_mnav(qk)
        out.append(v if np.isfinite(v) else np.nan)
    return out


def fmt_price_list_yen(vals):
    return " / ".join([ (f"¥{v:,.0f}" if np.isfinite(v) else "—") for v in vals ])

# ================== 公式APIから価格取得 ==================
def fetch_site_latest_values(enable=True):
    """Return exact numeric API prices, with an explicit source date."""
    result = {"btc_usd": None, "btc_jpy": None, "share_jpy": None, "as_of_date": None}
    if not enable:
        return result
    try:
        from metaplanet_data import fetch_latest
        latest = fetch_latest()
        for target, source in (("btc_usd", "btc_price_usd"),
                               ("btc_jpy", "btc_price_jpy"),
                               ("share_jpy", "share_price_jpy")):
            value = latest.get(source)
            if value is not None and np.isfinite(float(value)) and float(value) > 0:
                result[target] = float(value)
        result["as_of_date"] = latest.get("as_of_date")
    except Exception as exc:
        print(f"[WARN] Official price fetch failed; using dated stored values: {type(exc).__name__}: {exc}")
    return result

site_vals = fetch_site_latest_values(FETCH_FROM_SITE == "1")

# ===== README用：箇条書きメトリクス（Bitcoin価格／株価） =====
def latest_value(df_all, col):
    """dateが最大の行の値を返す（NaNは除外）。"""
    if col is None:
        return np.nan, None
    mask = df_all[date_col].notna() & df_all[col].notna()
    if not mask.any():
        return np.nan, None
    idx = df_all.loc[mask, date_col].idxmax()
    return float(df_all.loc[idx, col]), pd.to_datetime(df_all.loc[idx, date_col])

# シートの最新値（フォールバック用）
btc_usd_sheet, btc_usd_date = latest_value(df, col_btc_price_usd)
btc_jpy_man_sheet, btc_jpy_date = latest_value(df, col_btc_price_jpy_man)
btc_jpy_sheet        = (btc_jpy_man_sheet * 10000.0) if np.isfinite(btc_jpy_man_sheet) else np.nan
stock_price_sheet, stock_price_date = latest_value(df, stock_col)

# headline に使う表示値（サイト優先 → シート）
btc_usd_disp   = site_vals["btc_usd"]   if site_vals["btc_usd"]   is not None else btc_usd_sheet
btc_jpy_disp   = site_vals["btc_jpy"]   if site_vals["btc_jpy"]   is not None else btc_jpy_sheet
stock_yen_disp = site_vals["share_jpy"] if site_vals["share_jpy"] is not None else stock_price_sheet
stock_display_date = site_vals["as_of_date"] if site_vals["share_jpy"] is not None else stock_price_date

def price_source_label(api_value, stored_date):
    source_date = site_vals["as_of_date"] if api_value is not None else stored_date
    date_label = pd.Timestamp(source_date).strftime("%Y-%m-%d") if source_date is not None else "不明"
    source_label = "公式データ" if api_value is not None else "保存データ"
    return f"{date_label}・{source_label}"

# baseline_price_yen がNaNでも落ちないように保険
bpe = baseline_price_yen if (baseline_price_yen is not None and np.isfinite(baseline_price_yen)) else np.nan

# 箇条書きテキスト
headline_lines = []
if np.isfinite(btc_usd_disp) or np.isfinite(btc_jpy_disp):
    line = "・Bitcoin価格: "
    if np.isfinite(btc_usd_disp):
        line += f"${btc_usd_disp:,.0f}［{price_source_label(site_vals['btc_usd'], btc_usd_date)}］"
    if np.isfinite(btc_jpy_disp):
        line += f"（¥{btc_jpy_disp:,.0f}［{price_source_label(site_vals['btc_jpy'], btc_jpy_date)}］）"
    headline_lines.append(line)

if np.isfinite(stock_yen_disp) or np.isfinite(bpe):
    line = "・株価: "
    if np.isfinite(stock_yen_disp):
        line += f"¥{stock_yen_disp:,.0f}［{price_source_label(site_vals['share_jpy'], stock_price_date)}］"
    if np.isfinite(bpe):
        line += f"（mNAV=1: ¥{bpe:,.0f}）"
    headline_lines.append(line)

# GitHubで確実に改行されるように "  \n"（行末2スペース＋改行）で連結
headline_md = "  \n".join(headline_lines) if headline_lines else ""

# ================== 以降は従来どおり（図・テーブル生成） ==================
def make_valid_price_df(df_all, date_col, stock_col, col_btc_per_1000, col_btc_price_jpy_man):
    if stock_col is None:
        return pd.DataFrame()
    nav1000_jpy = df_all[col_btc_per_1000] * (df_all[col_btc_price_jpy_man] * 10000.0)
    d = pd.DataFrame({
        "date": pd.to_datetime(df_all[date_col], errors="coerce"),
        "price_y": pd.to_numeric(df_all[stock_col], errors="coerce"),
        "nav1000": pd.to_numeric(nav1000_jpy, errors="coerce"),
        "btc1000": pd.to_numeric(df_all[col_btc_per_1000], errors="coerce"),
    }).dropna()
    d = d[np.isfinite(d[["price_y", "nav1000", "btc1000"]]).all(axis=1)]
    d = d[(d["price_y"] > 0) & (d["nav1000"] > 0) & (d["btc1000"] > 0)]
    d["log_x"] = np.log10(d["nav1000"].astype(float))
    d["y"]     = np.log10(d["price_y"].astype(float))
    d["log_y"] = d["y"]
    return d

df_price = make_valid_price_df(df, date_col, stock_col, col_btc_per_1000, col_btc_price_jpy_man)
if len(df_price) < 3 or df_price["log_x"].nunique() < 2:
    raise ValueError("Insufficient valid stock-price history for regression; report was not replaced")

def fit_quantiles_logy(d, q_list):
    if d is None or len(d)==0: return {}
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
if 0.5 not in ql_price:
    raise ValueError("Median stock-price regression failed; report was not replaced")

def predict_mnav_at_xlog(qlines, xlog, q_list):
    if not qlines or not np.isfinite(xlog): return None
    out = {}
    for q in q_list:
        qf = float(q)
        if qf not in qlines: continue
        a = float(qlines[qf]["const"]); b = float(qlines[qf]["log_x"])
        out[qf] = 10 ** (a + b * xlog)
    return out

def predict_logprice_at_xlog(qlines, xlog, q_list):
    if not qlines or not np.isfinite(xlog): return None
    out = {}
    for q in q_list:
        qf = float(q)
        if qf not in qlines: continue
        a = float(qlines[qf]["const"]); b = float(qlines[qf]["log_x"])
        out[qf] = a + b * xlog  # log10(Price)
    return out

pt_price = (lambda d: None if d is None or len(d)==0 else {
    "date": d.iloc[-1]["date"], "x_log": float(d.iloc[-1]["log_x"]),
    "y": float(d.iloc[-1]["y"]), "btc1000": float(d.iloc[-1]["btc1000"])
})(df_price)

# ここは pt_price 定義の直後に置く
preds_mnav_at_current = predict_mnav_at_xlog(ql_jpy,   pt_jpy["x_log"]   if pt_jpy   else np.nan, base_quantiles)
preds_logp_now        = predict_logprice_at_xlog(ql_price, pt_price["x_log"] if pt_price else np.nan, base_quantiles)

# --- 乖離率（Chart3相当：mNAVのq=0.50から） ---
dev_pct = latest_mnav_deviation_pct(df_jpy, ql_jpy)

# --- 週足RSI(14)：シート株価から。週途中は headline の株価で暫定上書き ---
if stock_col is not None:
    weekly_rsi, rsi_prov = compute_weekly_rsi_14_from_sheet(
        df[date_col],
        df[stock_col],
        override_today=(stock_yen_disp if RSI_INCLUDE_CURR_WEEK == "1" else np.nan),
        override_date=stock_display_date,
        include_current_week=(RSI_INCLUDE_CURR_WEEK == "1"),
        method=RSI_METHOD
    )
else:
    weekly_rsi, rsi_prov = (np.nan, False)


# --- 買い/売りターゲット価格（q=0.05/0.03/0.02 と q=0.95/0.97/0.98） ---
buy_qs  = [0.05, 0.03, 0.02]
sell_qs = [0.95, 0.97, 0.98]
buy_prices  = quantile_prices_at_current(buy_qs,  preds_logp_now, preds_mnav_at_current, baseline_price_yen)
sell_prices = quantile_prices_at_current(sell_qs, preds_logp_now, preds_mnav_at_current, baseline_price_yen)
buy_prices_txt  = fmt_price_list_yen(buy_prices)
sell_prices_txt = fmt_price_list_yen(sell_prices)

# --- シグナル判定（段階付き）
#  買い条件: RSI<=50 または 乖離<=-50%
#  売り条件: RSI>=90 または 乖離>=+100%
cond_buy  = (np.isfinite(weekly_rsi) and weekly_rsi <= 50) or (np.isfinite(dev_pct) and dev_pct <= -50)
cond_sell = (np.isfinite(weekly_rsi) and weekly_rsi >= 90) or (np.isfinite(dev_pct) and dev_pct >= 100)

strong_buy  = (np.isfinite(weekly_rsi) and weekly_rsi <= 50) and (np.isfinite(dev_pct) and dev_pct <= -50)
strong_sell = (np.isfinite(weekly_rsi) and weekly_rsi >= 90) and (np.isfinite(dev_pct) and dev_pct >= 100)

if strong_sell:
    signal_txt, signal_emoji, signal_rank = "強売り", "🔴", 3
elif strong_buy:
    signal_txt, signal_emoji, signal_rank = "強買い", "🟣", 3
elif cond_sell:
    signal_txt, signal_emoji, signal_rank = "売り", "🟠", 2
elif cond_buy:
    signal_txt, signal_emoji, signal_rank = "買い", "🔵", 2
else:
    signal_txt, signal_emoji, signal_rank = "中立", "🟢", 1

# --- Signals 表示用テキスト ---
signals_lines = []
if np.isfinite(weekly_rsi):
    label = "RSI(週足,14)"
    if rsi_prov: label += "（暫定）"
    signals_lines.append(f"・{label}: {weekly_rsi:.1f}")
if np.isfinite(dev_pct):
    signals_lines.append(f"・乖離率 (mNAV vs q=0.50): {dev_pct:+.0f}%")
signals_lines.append(
    f"・Signal: {signal_emoji} {signal_txt}  \n"
    f"｜買いライン(q=0.05/0.03/0.02): {buy_prices_txt}  \n"
    f"｜売りライン(q=0.95/0.97/0.98): {sell_prices_txt}"
)
signals_md = "  \n".join(signals_lines)

# ================== Summary（比較テーブル） ==================
def make_combined_price_table(preds_mnav, preds_log10p, baseline_price_y, q_list, currency="¥"):
    cols = [f"{q:.2f}" for q in q_list]
    rows = []

    # Method A: mNAV 回帰 → 価格換算
    if (preds_mnav is None) or (not np.isfinite(baseline_price_y)):
        row_a = ["" for _ in cols]
    else:
        row_a=[]
        for q in q_list:
            mnav = preds_mnav.get(float(q))
            v = mnav * baseline_price_y if (mnav is not None and np.isfinite(mnav)) else np.nan
            row_a.append(f"{currency}{v:,.0f}" if np.isfinite(v) else "")
    rows.append(["mNAV Regression"] + row_a)

    # Method B: 価格回帰
    if preds_log10p is None:
        row_b = ["" for _ in cols]
    else:
        row_b=[]
        for q in q_list:
            lp = preds_log10p.get(float(q))
            v = 10**lp if (lp is not None and np.isfinite(lp)) else np.nan
            row_b.append(f"{currency}{v:,.0f}" if np.isfinite(v) else "")
    rows.append(["Stock-Price Regression"] + row_b)

    return pd.DataFrame(rows, columns=["Method"]+cols)

combined_table  = make_combined_price_table(preds_mnav_at_current, preds_logp_now, baseline_price_yen, quantiles, currency="¥")
df_summary_disp = combined_table.copy()

# ================== 可視化（4図） ==================
def _qk(q): return float(round(float(q), 6))

def get_line_colors(hilo_min, hilo_max):
    return {_qk(hilo_min): "rgb(30,60,200)", _qk(0.50): "rgb(0,140,0)", _qk(hilo_max): "rgb(200,30,30)"}

def add_smooth_gradient_bands(fig, xg, preds_grid, q_min=0.01, q_max=0.99, colorscale="Turbo", alpha=0.22, dense_n=80):
    qs_known = sorted(float(q) for q in preds_grid.keys() if q_min <= float(q) <= q_max)
    if len(qs_known) < 2: return
    Y_known = np.vstack([np.asarray(preds_grid[q], float) for q in qs_known])
    qs_dense = np.linspace(qs_known[0], qs_known[-1], dense_n)
    XN = Y_known.shape[1]
    Y_dense = np.empty((len(qs_dense), XN), dtype=float)
    for j in range(XN):
        Y_dense[:, j] = np.interp(qs_dense, qs_known, Y_known[:, j])
    fig.add_trace(go.Scattergl(x=xg, y=Y_dense[0], mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"), hoverinfo="skip", showlegend=False))
    for i in range(1, len(qs_dense)):
        q_mid = 0.5*(qs_dense[i-1] + qs_dense[i])
        col = sample_colorscale(colorscale, [q_mid])[0]
        if isinstance(col, str) and col.startswith("#"):
            r, g, b = hex_to_rgb(col)
        else:
            r, g, b = [int(v) for v in col[col.find("(")+1:col.find(")")].split(",")]
        fig.add_trace(go.Scattergl(x=xg, y=Y_dense[i], mode="lines",
                                   line=dict(width=0, color="rgba(0,0,0,0)"),
                                   fill="tonexty", fillcolor=f"rgba({r},{g},{b},{alpha})",
                                   hoverinfo="skip", showlegend=False))

def densify_preds_grid_logq(preds_grid, q_min=0.01, q_max=0.99, num=120, enforce_mono=True):
    qs_known = sorted(float(q) for q in preds_grid.keys() if q_min <= float(q) <= q_max)
    if len(qs_known) < 2:
        if len(qs_known) == 0:
            return np.array([]), np.zeros((0, 0))
        y = np.asarray(preds_grid[qs_known[0]], float)
        return np.array(qs_known, float), np.vstack([y for _ in qs_known])
    Y_known = np.vstack([np.asarray(preds_grid[q], float) for q in qs_known])
    qs_dense = np.linspace(q_min, q_max, num)
    XN = Y_known.shape[1]
    Y_dense = np.empty((len(qs_dense), XN), dtype=float)
    for j in range(XN):
        Y_dense[:, j] = np.interp(qs_dense, qs_known, Y_known[:, j])
    if enforce_mono:
        Y_dense = np.maximum.accumulate(Y_dense, axis=0)
    return qs_dense, Y_dense

def add_smooth_gradient_bands_log(fig, xg, preds_grid, q_min=0.01, q_max=0.99, num=120, colorscale="Turbo", alpha=0.24):
    qs, Y = densify_preds_grid_logq(preds_grid, q_min=q_min, q_max=q_max, num=num, enforce_mono=True)
    if qs.size < 2: return
    fig.add_trace(go.Scattergl(x=xg, y=Y[0], mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"), showlegend=False, hoverinfo="skip"))
    for i in range(1, len(qs)):
        q_mid = 0.5*(qs[i-1] + qs[i])
        col = sample_colorscale(colorscale, [q_mid])[0]
        if isinstance(col, str) and col.startswith('#'):
            r, g, b = hex_to_rgb(col)
        else:
            r, g, b = [int(v) for v in col[col.find('(')+1:col.find(')')].split(',')]
        fig.add_trace(go.Scattergl(x=xg, y=Y[i], mode="lines",
                                   line=dict(width=0, color="rgba(0,0,0,0)"),
                                   fill="tonexty", fillcolor=f"rgba({r},{g},{b},{alpha})",
                                   showlegend=False, hoverinfo="skip"))

def make_plot_axis(axis_name, d, qlines, star_pt, colorscale="Turbo",
                   quantiles_for_hover=None, hilo_min=HILO_MIN, hilo_max=HILO_MAX):
    if d is None or len(d)==0 or not qlines:
        return go.Figure().update_layout(title=f"{axis_name} (no data)")
    qs_dense = np.linspace(Q_MIN_SHADE, Q_MAX_SHADE, 120)
    highlights = np.array([_qk(hilo_min), _qk(0.50), _qk(hilo_max)])
    qs_all   = np.unique(np.concatenate([qs_dense, highlights]))

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
            preds[qf] = 10 ** (a + b * x_grid)
        return x_grid, preds

    xg, preds_grid = _grid_preds(d, qlines, qs_all)

    fig = go.Figure()
    fig.add_trace(go.Scattergl(
        x=d["log_x"], y=d["y"], customdata=d["btc1000"], mode="markers",
        marker=dict(size=5, opacity=0.7, color="rgba(80,120,255,0.9)"),
        name="Actual (mNAV)",
        hovertemplate=("BTC / 1,000 sh: %{customdata:,.4f} BTC<br>mNAV: %{y:,.4f}<extra></extra>")
    ))

    add_smooth_gradient_bands(
        fig, xg, preds_grid,
        q_min=Q_MIN_SHADE, q_max=Q_MAX_SHADE,
        colorscale=colorscale, alpha=0.22, dense_n=80)

    LINE_COLORS = get_line_colors(hilo_min, hilo_max)
    for q in highlights:
        y_line = preds_grid[_qk(q)]
        fig.add_trace(go.Scattergl(
            x=xg, y=y_line, mode="lines",
            line=dict(width=2.8, color=LINE_COLORS[_qk(q)]),
            name=f"q={q:.2f}",
            hovertemplate=("q={q:.2f}<br>mNAV: %{y:,.4f}<extra></extra>").replace("{q:.2f}", f"{q:.2f}")
        ))

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

def make_plot_axis_price_log(d, qlines, star_pt, colorscale="Turbo",
                             hilo_min=HILO_MIN, hilo_max=HILO_MAX):
    if d is None or len(d)==0 or not qlines:
        return go.Figure().update_layout(title="log10(Price) vs log10(BTC NAV per 1,000 shares) (no data)")
    qs_dense = np.linspace(0.01, 0.99, 120)
    highlights = np.array([_qk(hilo_min), _qk(0.50), _qk(hilo_max)])
    qs_all = np.unique(np.concatenate([qs_dense, highlights]))

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
            preds[qf] = a + b * x_grid   # log10(Price)
        return x_grid, preds

    xg, preds_grid_log = _grid_preds_log(d, qlines, qs_all)

    fig = go.Figure()
    btc1000_actual = d["btc1000"].values
    log10_price_actual = d["y"].values
    price_actual = 10.0 ** log10_price_actual
    fig.add_trace(go.Scattergl(
        x=d["log_x"], y=log10_price_actual,
        customdata=np.c_[btc1000_actual, log10_price_actual, price_actual],
        mode="markers",
        marker=dict(size=5, opacity=0.7, color="rgba(80,120,255,0.9)"),
        name="Actual (log10 Price)",
        hovertemplate=("BTC / 1,000 sh: %{customdata[0]:,.4f} BTC<br>"
                       "Price (¥): ¥%{customdata[2]:,.0f}<br>"
                       "log10 Price (¥): %{customdata[1]:.4f}<extra></extra>")
    ))

    add_smooth_gradient_bands_log(
        fig, xg, preds_grid_log,
        q_min=Q_MIN_SHADE, q_max=Q_MAX_SHADE, num=120,
        colorscale=colorscale, alpha=0.24)

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
            hovertemplate=(f"q={q:.2f}<br>"
                           "Price (¥): ¥%{customdata[1]:,.0f}<br>"
                           "log10 Price (¥): %{customdata[0]:.4f}<extra></extra>")
        ))

    texts=[]
    for i in range(len(xg)):
        lines=[]
        for q in base_quantiles:
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

    if star_pt:
        fig.add_trace(go.Scattergl(
            x=[star_pt["x_log"]], y=[star_pt["y"]],
            customdata=[[star_pt["btc1000"], star_pt["y"], 10.0**star_pt["y"]]],
            mode="markers",
            marker=dict(symbol='star', size=16, line=dict(width=1, color='black'), color="yellow"),
            name="現在",
            hovertemplate=(f"📅: {star_pt['date'].strftime('%Y-%m-%d')}<br>"
                           "BTC / 1,000 sh: %{customdata[0]:,.4f} BTC<br>"
                           "Price (¥): ¥%{customdata[2]:,.0f}<br>"
                           "log10 Price (¥): %{customdata[1]:.4f}<extra></extra>")
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

def make_relerr_mnav(d, qlines, star_pt=None, baseline_price_yen=np.nan):
    if d is None or len(d)==0 or (0.5 not in qlines):
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
    if d is None or len(d)==0 or (0.5 not in qlines):
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

# ---- 図生成（4つ） ----
fig_jpy       = make_plot_axis("JPY", df_jpy, ql_jpy, pt_jpy, colorscale="Turbo")
fig_price_log = make_plot_axis_price_log(df_price, ql_price, pt_price, colorscale="Turbo")
fig_rel_mnav  = make_relerr_mnav(df_jpy, ql_jpy, star_pt=pt_jpy, baseline_price_yen=baseline_price_yen)
fig_rel_price = make_relerr_logprice(df_price, ql_price, star_pt=pt_price)

# ================== 出力（PNG/HTML/README/index.html） ==================
os.makedirs("assets", exist_ok=True)
os.makedirs("docs",   exist_ok=True)

figs = [
    {"fig": fig_jpy,       "png": "assets/fig1.png", "html": "docs/fig1.html", "label": "Chart 1: mNAV vs log10(NAV/1000) [JPY]"},
    {"fig": fig_price_log, "png": "assets/fig2.png", "html": "docs/fig2.html", "label": "Chart 2: log10(Price) vs log10(NAV/1000) [JPY]"},
    {"fig": fig_rel_mnav,  "png": "assets/fig3.png", "html": "docs/fig3.html", "label": "Chart 3: Relative Error from q=0.50 (mNAV)"},
    {"fig": fig_rel_price, "png": "assets/fig4.png", "html": "docs/fig4.html", "label": "Chart 4: Relative Error from q=0.50 (Price)"},
]

# PNG（要: kaleido）
for item in figs:
    # These charts contain no TeX. Disable the external MathJax request so
    # static export works offline and cannot hang on an unrelated CDN.
    png = kaleido.calc_fig_sync(
        item["fig"],
        opts={"format": "png", "width": 1200, "height": 720, "scale": 2},
        kopts={"mathjax": False},
    )
    with open(item["png"], "wb") as output:
        output.write(png)

# インタラクティブ HTML
for item in figs:
    item["fig"].write_html(item["html"], include_plotlyjs="cdn", full_html=True)

# ===== Summary（表）→ Markdown 作成 =====
def _to_markdown_safe(df_in):
    try:
        return df_in.to_markdown(index=False)
    except Exception:
        return df_in.to_string(index=False)

_df_src    = df_summary_disp if isinstance(df_summary_disp, pd.DataFrame) else pd.DataFrame([{"Message":"(no data)"}])
summary_md = _to_markdown_safe(_df_src)

with open("assets/summary.md", "w", encoding="utf-8") as f:
    f.write(summary_md)

# タイムスタンプ
JST = timezone(timedelta(hours=9))
ts  = datetime.now(JST).strftime("%Y-%m-%d %H:%M (%Z)")

# 図リンク + PNG（figs / PAGES_URL は前段で定義済み）
chart_blocks = []
for i, item in enumerate(figs, start=1):
    chart_blocks.append(
        f"[Open interactive {item['label']}]({PAGES_URL}/{os.path.basename(item['html'])})\n\n"
        f"![fig{i}]({item['png']})"
    )
charts_md = "\n\n".join(chart_blocks)

# README 本文
block = (
    f"**Last update (JST):** {ts}\n\n"
    f"**Historical data through:** {history_as_of}\n\n"
    f"### Summary\n"
    f"{headline_md}\n\n"
    f"### Signals\n"
    f"{signals_md}\n\n"
    f"{summary_md}\n\n"
    f"### Charts\n{charts_md}"
)


# ===== README を再生成 =====
readme_path  = "README.md"
start_marker = "<!--REPORT:START-->"
end_marker   = "<!--REPORT:END-->"
preface = "# meta-analysis\n\n"

report = f"{start_marker}\n{block}\n{end_marker}"
if os.path.exists(readme_path):
    with open(readme_path, encoding="utf-8") as existing:
        previous = existing.read()
    if start_marker in previous and end_marker in previous:
        before, remainder = previous.split(start_marker, 1)
        _, after = remainder.split(end_marker, 1)
        new_readme = before + report + after
    else:
        new_readme = previous.rstrip() + "\n\n" + report + "\n"
else:
    new_readme = preface + report + "\n"
with open(readme_path, "w", encoding="utf-8") as f:
    f.write(new_readme)
print("README overwritten.")

# ================== モバイル用ダッシュボード（docs/index.html） ==================
def _safe_html_table(df_in):
    try:
        return df_in.to_html(index=False, classes="tbl", border=0, escape=False)
    except Exception:
        return "<p>(no summary)</p>"

table_html = _safe_html_table(_df_src)

sections = []
for item in figs:
    sections.append(
        f"""<section class="card">
  <h2 style="margin:0 0 8px;font-size:16px;">{item['label']}</h2>
  {item['fig'].to_html(include_plotlyjs=False, full_html=False)}
  <div style="margin-top:6px;"><a href="{os.path.basename(item['html'])}" target="_blank">Open interactive</a></div>
</section>"""
    )
figs_html = "\n\n".join(sections)

index_html = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover" />
<title>Meta Analysis — Daily</title>
<link rel="preconnect" href="https://cdn.plot.ly">
<script src="https://cdn.plot.ly/plotly-{get_plotlyjs_version()}.min.js"></script>
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
    <div class="meta">Historical data through: <b>{history_as_of}</b></div>
  </div>
  <div class="btns">
    <a href="../actions" target="_blank">更新（Actions）</a>
    <a href="index.html?nocache={int(datetime.now().timestamp())}">強制リロード</a>
  </div>
</header>

<section class="card">
  <h2 style="margin:0 0 8px;font-size:16px;">Summary</h2>
  <p>{'<br>'.join(escape(line) for line in headline_lines)}</p>
  {table_html}
</section>

{figs_html}

<a class="sticky-refresh" href="index.html?nocache={int(datetime.now().timestamp())}" title="Refresh">🔄</a>
</body>
</html>"""

with open("docs/index.html", "w", encoding="utf-8") as f:
    f.write(index_html)
print("docs/index.html written with 4 charts.")
