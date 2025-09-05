# =========================
# 必要なら最初にインストール（Colab向け）
# !pip -q install gspread google-auth selenium plotly statsmodels scipy
# !apt-get -y update > /dev/null
# !apt-get -y install chromium-chromedriver > /dev/null
# =========================

!apt-get -y update > /dev/null
!apt-get -y install chromium-chromedriver > /dev/null
!pip -q install selenium gspread google-auth plotly statsmodels scipy


import os, re, time, json, sys
import numpy as np
import pandas as pd
from datetime import datetime

# ===== Plotly =====
import plotly.graph_objects as go
import plotly.io as pio

# ===== Sheets: サービスアカウント認証 =====
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
# ★ あなたのサービスアカウントJSONへのパス
KEY_PATH = "/content/optimal-bivouac-471208-f4-ec84cb2443af.json"

creds = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)

# ===== 解析パッケージ =====
import statsmodels.api as sm
from scipy.optimize import curve_fit
from sklearn.metrics import r2_score

# ===== Selenium（公式サイトの現在値取得用）=====
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


# ------------------------------
# 設定
# ------------------------------
# スプレッドシート
SPREADSHEET_ID = "1OdhLsAZYVsFz5xcGeuuiuH7JoYyzz6AaG0j2A9Jw1_4"
WORKSHEET_NAME = "データシート"

# 表示や計算パラメータ
quantile_lower = 0.005
quantile_upper = 0.995
quantiles = [quantile_lower, 0.5, quantile_upper]

upper_pct = 0.96     # +96%
lower_pct = -0.45    # -45%
mult_factors = [0.5, 2.0]  # ★ 中央値×係数ライン

# Plotly色
colors = {quantile_lower: 'blue', 0.5: 'green', quantile_upper: 'red'}


# ------------------------------
# Sheets から履歴データ読み込み
# ------------------------------
ws = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
raw = ws.get_all_values()

# 重複ヘッダ対策
original_headers = raw[0]
seen, headers = {}, []
for h in original_headers:
    if h in seen:
        seen[h] += 1
        headers.append(f"{h}_{seen[h]}")
    else:
        seen[h] = 0
        headers.append(h if h else "Unnamed")

df = pd.DataFrame(raw[1:], columns=headers)

# 列名推定
date_col = 'Date'
mnav_col = next((c for c in df.columns if 'mnav' in c.lower()), None)
btc_col  = next((c for c in df.columns if 'BTC保有量' in c), None)
price_col = next((c for c in df.columns if '株価' in c), None)

# 整形
df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
df[mnav_col] = pd.to_numeric(df[mnav_col], errors='coerce')
df[btc_col]  = pd.to_numeric(df[btc_col], errors='coerce')

# 株価列（fair price推定用）
latest_price = np.nan
if price_col is not None:
    try:
        latest_price = (
            df[price_col]
              .astype(str).replace('#N/A', np.nan).dropna()
              .str.replace(',', '', regex=False)
              .astype(float).iloc[-1]
        )
    except Exception:
        latest_price = np.nan

df_valid = df.dropna(subset=[date_col, mnav_col, btc_col])
df_valid = df_valid[(df_valid[mnav_col] > 0) & (df_valid[btc_col] > 0)]

# 特徴量
df_valid["log_btc"] = np.log10(df_valid[btc_col])
df_valid["log_mnav"] = np.log10(df_valid[mnav_col])
df_valid["mnav"] = df_valid[mnav_col]
df_valid["btc_ratio"] = df_valid[btc_col] / 21_000_000


# ------------------------------
# モデル1: y = 10^(a * x^b)
# ------------------------------
def power_exp_model(x, a, b):
    return 10 ** (a * x ** b)

mask = df_valid["btc_ratio"] > 1e-6
x_fit = df_valid["btc_ratio"][mask]
y_fit = df_valid[mnav_col][mask]

a_fit, b_fit = curve_fit(power_exp_model, x_fit, y_fit, maxfev=10000)[0]
df_valid["pred_power_exp"] = power_exp_model(df_valid["btc_ratio"], a_fit, b_fit)
r2_val = r2_score(y_fit, power_exp_model(x_fit, a_fit, b_fit))

df_valid["error_pct_curvefit"] = 100 * (df_valid[mnav_col] - df_valid["pred_power_exp"]) / df_valid["pred_power_exp"]


# ------------------------------
# 分位点回帰（log-log）
# ------------------------------
X_log = sm.add_constant(df_valid["log_btc"])
quantile_lines = {q: sm.QuantReg(df_valid["log_mnav"], X_log).fit(q=q).params for q in quantiles}

# 中央線に対する誤差（%）
median_pred_log = quantile_lines[0.5]["const"] + quantile_lines[0.5]["log_btc"] * df_valid["log_btc"]
df_valid["error_pct_quantile"] = 100 * (10 ** (df_valid["log_mnav"] - median_pred_log) - 1)


# ------------------------------
# 公式サイトから“現在の mNAV / BTC Holdings”を取得
# ------------------------------
def _to_number(s):
    if s is None:
        return None
    t = s.strip().replace(",", "").replace("¥","").replace("$","").replace("₿","").strip()
    m = re.match(r'^(-?\d+(\.\d+)?)([KMB])?$', t)
    if not m:
        try:
            return float(t)
        except:
            return None
    x = float(m.group(1))
    unit = m.group(3)
    if unit == 'K': x *= 1e3
    if unit == 'M': x *= 1e6
    if unit == 'B': x *= 1e9
    return x

def fetch_current_mnav_and_btc():
    url = "https://metaplanet.jp/en/analytics"
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)
    try:
        driver.get(url)

        def get_card_value(title):
            card_xpath = f"//div[contains(@class,'rounded') and .//*[text()='{title}']]"
            card = wait.until(EC.presence_of_element_located((By.XPATH, card_xpath)))
            # 通貨/数値テキスト
            try:
                val_elem = card.find_element(By.XPATH, ".//*[contains(text(),'¥') or contains(text(),'$') or contains(text(),'₿')]")
            except:
                val_elem = card.find_element(By.XPATH, ".//*[not(*) and normalize-space(text())!='']")
            return val_elem.text.strip()

        btc_text  = get_card_value("BTC Holdings")
        mnav_text = get_card_value("mNAV")
        return _to_number(mnav_text), _to_number(btc_text)
    finally:
        driver.quit()

current_mnav, current_btc = fetch_current_mnav_and_btc()
if current_mnav is None or current_btc is None or current_mnav <= 0 or current_btc <= 0:
    # フォールバック：シートの最新レコード
    last = df_valid.iloc[-1]
    current_mnav = float(last[mnav_col])
    current_btc  = float(last[btc_col])
    current_label_date = last[date_col].strftime("%Y-%m-%d") + " (fallback)"
else:
    current_label_date = datetime.now().strftime("%Y-%m-%d") + " (live)"

current_log_btc = np.log10(current_btc)


# ------------------------------
# 拡張軸と各曲線の準備（Plotly 図1）
# ------------------------------
x_ext = np.linspace(df_valid["log_btc"].min(), np.log10(100000), 500)
btc_ext = 10 ** x_ext

# 分位点ライン（MNAV）
predictions_ext = {
    q: 10 ** (quantile_lines[q]["const"] + quantile_lines[q]["log_btc"] * x_ext)
    for q in quantiles
}
# 中央値×係数ライン
median_line_ext = predictions_ext[0.5]
mult_predictions_ext = {m: median_line_ext * m for m in mult_factors}

# 実データMNAVの補間（hover用）
actual_mnav_interp = np.interp(x_ext, df_valid["log_btc"], df_valid[mnav_col])

# fair_price（MNAV→株価換算）
fair_price = np.nan
if np.isfinite(latest_price) and latest_price > 0:
    try:
        latest_mnav_for_fp = float(df_valid[mnav_col].dropna().iloc[-1])
        if latest_mnav_for_fp > 0:
            fair_price = latest_price / latest_mnav_for_fp
    except Exception:
        pass

stock_price_predictions_ext = {}
stock_price_mult_predictions_ext = {}
if np.isfinite(fair_price):
    stock_price_predictions_ext = {q: predictions_ext[q] * fair_price for q in quantiles}
    stock_price_mult_predictions_ext = {m: mult_predictions_ext[m] * fair_price for m in mult_factors}

# Hoverテキスト（株価換算含む場合）
hover_text_with_price = []
for i, btc in enumerate(btc_ext):
    lines = [f"BTC: {btc:,.0f} BTC<br>"]
    for q in quantiles:
        if np.isfinite(fair_price):
            lines.append(
                f"q={q:.2f} MNAV: {predictions_ext[q][i]:,.2f} "
                f"(価格: ¥{stock_price_predictions_ext[q][i]:,.0f})<br>"
            )
        else:
            lines.append(f"q={q:.2f} MNAV: {predictions_ext[q][i]:,.2f}<br>")
    for m in mult_factors:
        if np.isfinite(fair_price):
            lines.append(
                f"{m:.1f}× Median MNAV: {mult_predictions_ext[m][i]:,.2f} "
                f"(価格: ¥{stock_price_mult_predictions_ext[m][i]:,.0f})<br>"
            )
        else:
            lines.append(f"{m:.1f}× Median MNAV: {mult_predictions_ext[m][i]:,.2f}<br>")
    lines.append(f"Actual MNAV: {actual_mnav_interp[i]:,.2f}")
    hover_text_with_price.append("".join(lines))



# =========================
# まとめ表（qライン & Median×係数）— 1表・価格つき・Type除去
# =========================

# --- fair_price をより堅牢に算出（直近で 価格 と mNAV の両方がある行を探す） ---
fair_price = np.nan
if price_col is not None:
    # 価格列を数値化
    price_series = (
        df[price_col]
        .astype(str)
        .str.replace('¥','', regex=False)
        .str.replace(',','', regex=False)
        .replace({'#N/A': np.nan, '': np.nan, 'nan': np.nan})
    )
    price_series = pd.to_numeric(price_series, errors='coerce')

    # 直近の valid 行（価格とmNAVが両方あって mNAV>0）
    valid_idx = df.index[(~price_series.isna()) & (~df[mnav_col].isna()) & (pd.to_numeric(df[mnav_col], errors='coerce')>0)]
    if len(valid_idx) > 0:
        idx = valid_idx[-1]
        fair_price = float(price_series.loc[idx]) / float(df.loc[idx, mnav_col])

# --- 表データ作成（現在のBTC＝⭐の値で計算） ---
btc_today = float(current_btc)
log_btc_today = np.log10(btc_today)

# 中央線パラメータ
a_50 = float(quantile_lines[0.5]["const"])
b_50 = float(quantile_lines[0.5]["log_btc"])
mnav_median_today = 10 ** (a_50 + b_50 * log_btc_today)

rows = []

# qごと
for q in quantiles:
    a_q = float(quantile_lines[q]["const"])
    b_q = float(quantile_lines[q]["log_btc"])
    pred_mnav = 10 ** (a_q + b_q * log_btc_today)
    pred_price = pred_mnav * fair_price if np.isfinite(fair_price) else np.nan
    rows.append({
        "Key": f"q = {q:.3f}",
        "BTC (holdings)": btc_today,
        "MNAV (pred)": pred_mnav,
        "Price (¥)": pred_price
    })

# 中央値×係数
for m in mult_factors:
    pred_mnav = mnav_median_today * m
    pred_price = pred_mnav * fair_price if np.isfinite(fair_price) else np.nan
    rows.append({
        "Key": f"{m:.2f} × median",
        "BTC (holdings)": btc_today,
        "MNAV (pred)": pred_mnav,
        "Price (¥)": pred_price
    })

# --- 表を整形して1つだけ表示 ---
df_summary = pd.DataFrame(rows, columns=["Key","BTC (holdings)","MNAV (pred)","Price (¥)"])

def _fmt(x, digits=2):
    if pd.isna(x): return ""
    try: return f"{x:,.{digits}f}"
    except: return str(x)

df_summary_disp = df_summary.copy()
df_summary_disp["BTC (holdings)"] = df_summary_disp["BTC (holdings)"].apply(lambda v: _fmt(v, 4))
df_summary_disp["MNAV (pred)"]    = df_summary_disp["MNAV (pred)"].apply(lambda v: _fmt(v, 4))
df_summary_disp["Price (¥)"]      = df_summary_disp["Price (¥)"].apply(lambda v: _fmt(v, 0))

print(f"📋 Summary at current BTC holdings (⭐ = {btc_today:,.4f} BTC, {current_label_date})")
display(df_summary_disp)

# 価格が空欄のときの補足
if not np.isfinite(fair_price):
    print("※ Price(¥) は、シート内で『株価』と『mNAV』が同時に得られる最新行が見つからず計算できませんでした。")
    print("  シートに両方の列がそろっている最新行があれば自動で表示されます。")

# ------------------------------
# 図1（Plotly）：MNAV vs log10(BTC) + 分位点/中央値×係数 + ⭐(現在値)
# ------------------------------
fig = go.Figure()

# 実データ
fig.add_trace(go.Scattergl(
    x=df_valid["log_btc"],
    y=df_valid[mnav_col],
    mode='markers',
    marker=dict(color='gray', size=4, opacity=0.6),
    name='実データ'
))

# 分位点ライン
for q in quantiles:
    fig.add_trace(go.Scattergl(
        x=x_ext,
        y=predictions_ext[q],
        mode='lines',
        name=f"q={q:.2f} MNAV",
        line=dict(color=colors.get(q, None), width=2)
    ))

# 中央値×係数ライン
for m in mult_factors:
    fig.add_trace(go.Scattergl(
        x=x_ext,
        y=mult_predictions_ext[m],
        mode='lines',
        name=f"{m:.1f}× Median (MNAV)",
        line=dict(dash='dot', width=2)
    ))

# Hover情報（透明マーカー）
fig.add_trace(go.Scattergl(
    x=x_ext,
    y=predictions_ext[0.5],
    mode='markers',
    marker=dict(size=8, color='rgba(0,0,0,0)'),
    name='hover-info',
    hovertemplate="%{text}<extra></extra>",
    text=hover_text_with_price,
    showlegend=False
))

# ⭐（現在値）
fig.add_trace(go.Scattergl(
    x=[current_log_btc],
    y=[current_mnav],
    mode='markers',
    name=f"現在 ⭐ ({current_label_date})",
    marker=dict(symbol='star', size=14, color='yellow', line=dict(width=1, color='black')),
    hovertemplate=("📅: " + current_label_date +
                   "<br>log10(BTC): %{x:.4f}" +
                   "<br>mNAV: %{y:,.2f}<extra></extra>")
))
fig.add_annotation(
    x=current_log_btc, y=current_mnav,
    text=f"{current_label_date}",
    showarrow=True, arrowhead=2, ax=20, ay=-25
)

fig.update_layout(
    title="MNAV vs log10(BTC Holdings) — qライン & Median×係数（⭐=現在の公式値）",
    xaxis_title="log10(BTC Holdings)",
    yaxis_title="MNAV",
    hovermode='x unified',
    template='plotly_white',
    width=1000,
    height=600
)
pio.show(fig)


# ------------------------------
# 図2（Plotly）：中央値からの相対誤差（±%ガイド）
# ------------------------------
a_50 = quantile_lines[0.5]["const"]
b_50 = quantile_lines[0.5]["log_btc"]

log_btc_values = df_valid["log_btc"].values
mnav_values    = df_valid["mnav"].values
predicted_mnav = 10 ** (a_50 + b_50 * log_btc_values)

upper_mnav = predicted_mnav * (1 + upper_pct)
lower_mnav = predicted_mnav * (1 + lower_pct)

error_rate = 100 * (mnav_values - predicted_mnav) / predicted_mnav

hover_text = [
    f"BTC: {10**lb:,.0f}<br>Actual: {mv:.3f}<br>Predicted: {pm:.3f}<br>Error: {er:+.2f}%"
    for lb, mv, pm, er in zip(log_btc_values, mnav_values, predicted_mnav, error_rate)
]

upper_text = [f"BTC: {10**lb:,.0f}<br>Upper Bound mNAV (+{int(upper_pct*100)}%): {um:.3f}"
              for lb, um in zip(log_btc_values, upper_mnav)]
lower_text = [f"BTC: {10**lb:,.0f}<br>Lower Bound mNAV ({int(lower_pct*100)}%): {lm:.3f}"
              for lb, lm in zip(log_btc_values, lower_mnav)]

fig_error = go.Figure()

fig_error.add_trace(go.Scattergl(
    x=log_btc_values,
    y=error_rate,
    mode='markers',
    marker=dict(color='red', size=6),
    name='Relative Error',
    text=hover_text,
    hovertemplate="%{text}<extra></extra>"
))

fig_error.add_hline(y=0, line=dict(color='gray', dash='dash'))

fig_error.add_trace(go.Scattergl(
    x=log_btc_values,
    y=100 * (upper_mnav - predicted_mnav) / predicted_mnav,
    mode='lines',
    line=dict(color='blue', dash='dot'),
    name=f"Upper Bound (+{int(upper_pct*100)}%)",
    text=upper_text,
    hovertemplate="%{text}<extra></extra>"
))
fig_error.add_trace(go.Scattergl(
    x=log_btc_values,
    y=100 * (lower_mnav - predicted_mnav) / predicted_mnav,
    mode='lines',
    line=dict(color='green', dash='dot'),
    name=f"Lower Bound ({int(lower_pct*100)}%)",
    text=lower_text,
    hovertemplate="%{text}<extra></extra>"
))

fig_error.update_layout(
    title=f"Relative Error from Median (±{int(upper_pct*100)}%, {int(lower_pct*100)}%)",
    xaxis_title="log10(BTC Holdings)",
    yaxis_title="Relative Error (%)",
    template='plotly_white',
    width=1000,
    height=500,
    hovermode='closest'
)


pio.show(fig_error)

