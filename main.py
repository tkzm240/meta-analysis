import os, json, sys, time, re
from datetime import datetime

# ===== Selenium =====
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# ===== gspread / service account =====
import gspread
from google.oauth2.service_account import Credentials

# === 環境変数から設定を読み込み ===
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "データシート")
SA_JSON = os.environ["GSA_JSON"]  # サービスアカウントのJSON（GitHub Secretに入れる）

# 1) サービスアカウントの鍵をファイルに書き出す
with open("credentials.json", "w") as f:
    f.write(SA_JSON)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
gc = gspread.authorize(creds)

# ---------- カード名の候補（EN / JP 両対応） ----------
CARD_TITLES = {
    "Bitcoin Price": ["Bitcoin Price", "BTC価格"],
    "Share Price": ["Share Price", "株価"],
    "BTC Holdings": ["BTC Holdings", "BTC保有数", "BTC保有量"],
    "Bitcoin NAV": ["Bitcoin NAV", "BTC NAV"],
    "Debt Outstanding": ["Debt Outstanding", "未払い債務"],
    "Market Cap": ["Market Cap", "企業価値"],
    "mNAV": ["mNAV"],
    "Bitcoin per 1,000 Shares": ["Bitcoin per 1,000 Shares", "1000株あたりのBTC"],
}

def build_card_xpath(names):
    """候補名のどれかに一致するカードをORで特定"""
    parts = [f".//*[normalize-space(text())='{n}']" for n in names]
    # rounded がカードのコンテナに付く（サイトのDOM前提）
    return " | ".join([f"//div[contains(@class,'rounded') and ({p})]" for p in parts])

def find_card(driver, wait, card_name):
    names = CARD_TITLES.get(card_name, [card_name])
    xpath = build_card_xpath(names)
    return wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

def click_yen_button(driver, wait, card_name):
    """そのカードの中の ¥ ボタンを押す（存在しないカードは無視）"""
    cards_with_yen_button = [
        "Bitcoin NAV", "Debt Outstanding", "Market Cap", "Bitcoin Price", "Share Price"
    ]
    if card_name not in cards_with_yen_button:
        return
    try:
        card = find_card(driver, wait, card_name)
        driver.execute_script("arguments[0].scrollIntoView();", card)
        time.sleep(0.4)
        yen_btn = card.find_element(By.XPATH, ".//button[normalize-space(text())='¥']")
        yen_btn.click()
        # ボタン押下後にカード内テキストへ ¥ が現れるまで待機
        WebDriverWait(driver, 5).until(lambda d: "¥" in card.text)
        print(f"¥ clicked: {card_name}")
    except Exception as e:
        print(f"[WARN] yen click failed: {card_name} {e}")

def extract_card_value(driver, wait, card_name):
    """
    カード本体を掴んで card.text から正規表現で数値を抽出。
    - 通貨系: 先頭の `$`/`¥`/`₿` + 数字（K/M/B/億 を許容）
    - mNAV:  '74.18x' を優先的に拾う
    """
    try:
        card = find_card(driver, wait, card_name)
        driver.execute_script("arguments[0].scrollIntoView();", card)
        time.sleep(0.3)
        txt = card.text

        if card_name == "mNAV":
            m = re.search(r"\b(\d+(?:\.\d+)?)x\b", txt)
            if m:
                return m.group(1)
            # フォールバック：単なる実数
            m = re.search(r"\b\d+(?:\.\d+)?\b", txt)
            return m.group(0) if m else None

        # 通貨あり（K/M/B/億 を末尾に許容）
        m = re.search(r"(?:[$¥₿]\s*)\d{1,3}(?:,\d{3})*(?:\.\d+)?(?:[KMB億])?", txt)
        return m.group(0).replace(" ", "") if m else None
    except Exception as e:
        print(f"[WARN] extract failed: {card_name} {e}")
        return None

# ---------- クリーナー ----------
def clean_number(val):
    """
    一般用：通貨/記号を外して float へ。
    - 末尾 'B' はサイトが「B（billion）」を単位として表示するケースで **文字だけ落として数値はそのまま**（互換維持）
    - 末尾 '億' も **文字だけ外す**（= 億円単位の数値を維持）
    """
    try:
        s = val.replace("¥", "").replace("$", "").replace("₿", "").replace(",", "").strip()
        # 末尾サフィックスの処理（スケールしない）
        if s.endswith(("B", "b", "億")):
            s = s[:-1]
        return float(s)
    except Exception:
        return None

def clean_btc_usd(val):
    """
    Bitcoin Price（USD）専用：$112,371 や $110.9K / $1.23M を数値（ドル）へ。
    """
    try:
        s = val.replace("$", "").replace(",", "").strip().lower()
        mult = 1.0
        if s.endswith("k"):
            mult, s = 1_000, s[:-1]
        elif s.endswith("m"):
            mult, s = 1_000_000, s[:-1]
        return float(s) * mult
    except Exception:
        return None

def clean_btc_jpy_to_manye(val):
    """
    Bitcoin Price（JPY）→ **万円** で返す。
    - 例: '¥16.61M' -> 16.61 * 100 = 1661 （万円）
    - 例: '¥12,345,678' -> 12345678 / 10000 = 1234.5678 （万円）
    """
    try:
        s = val.replace("¥", "").replace(",", "").strip().lower()
        if s.endswith("m"):
            return float(s[:-1]) * 100.0
        # サフィックス無し（生の円）の場合は 万円へ換算
        return float(s) / 10000.0
    except Exception:
        return None

def get_next_time_value(ws):
    try:
        time_col = ws.col_values(2)[1:]  # ヘッダ除く
        if not time_col:
            return 1
        return int(time_col[-1].strip()) + 1
    except:
        return 1

def calc_shares_oku(btc_holdings, btc_per_1000):
    try:
        if not btc_holdings or not btc_per_1000:
            return ""
        # (BTC保有量 / 1000株あたりBTC) * 1000 = 発行済株式数
        # それを「億株」表示に
        return round((btc_holdings / btc_per_1000) * 1000.0 / 100_000_000.0, 6)
    except:
        return ""

def run():
    # ----- シート接続 -----
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # ----- Chrome -----
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    chrome_path = os.environ.get("CHROME_PATH")
    if chrome_path:
        options.binary_location = chrome_path

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    target_cards = [
        "Bitcoin Price", "Share Price", "BTC Holdings", "Bitcoin NAV",
        "Debt Outstanding", "Market Cap", "mNAV", "Bitcoin per 1,000 Shares"
    ]
    # 1周目は USD が欲しいカード
    cards_force_usd = ["Bitcoin Price"]

    try:
        driver.get("https://metaplanet.jp/en/analytics")

        results = {}
        # まずは Yen 切り替えが必要なカードは ¥ を押してから取得
        for name in target_cards:
            if name not in cards_force_usd:
                click_yen_button(driver, wait, name)
            results[name] = extract_card_value(driver, wait, name)
            time.sleep(0.5)

        # Bitcoin Price のみ JPY 版も別途取得
        click_yen_button(driver, wait, "Bitcoin Price")
        btc_price_jpy_text = extract_card_value(driver, wait, "Bitcoin Price")
    finally:
        driver.quit()

    # ----- クリーニング -----
    now = datetime.now()
    date_str = now.strftime("%Y/%m/%d")
    time_val = get_next_time_value(ws)

    btc_holdings  = clean_number(results.get("BTC Holdings") or "")
    btc_per_1000  = clean_number(results.get("Bitcoin per 1,000 Shares") or "")
    share_price   = clean_number(results.get("Share Price") or "")
    btc_price_usd = clean_btc_usd(results.get("Bitcoin Price") or "")
    market_cap    = clean_number(results.get("Market Cap") or "")
    btc_nav       = clean_number(results.get("Bitcoin NAV") or "")
    debt          = clean_number(results.get("Debt Outstanding") or "")
    mnav          = clean_number(results.get("mNAV") or "")
    btc_price_jpy_for_sheet = clean_btc_jpy_to_manye(btc_price_jpy_text) if btc_price_jpy_text else None

    usd_jpy_manual = ""  # 任意で手入力
    shares_outstanding_oku = calc_shares_oku(btc_holdings, btc_per_1000)

    # ----- ヘッダ確認 -----
    header = ws.row_values(1)
    needed_headers = [
        "Date","Time","BTC保有量","1000株当たりBTC","株価(円)[SBI]","BTC Price ($)",
        "Market Cap","BTCNAV","Debt","mNAV","real_mNAV","BTC価格(円)",
        "ドル円(JPY/USD)","発行済み株式数(億)"
    ]
    if len(header) < len(needed_headers):
        ws.update('A1', [header + needed_headers[len(header):]])

    # ----- 追記 -----
    next_row = len(ws.get_all_values()) + 1
    row = [
        date_str, time_val, btc_holdings, btc_per_1000, share_price, btc_price_usd,
        market_cap, btc_nav, debt,
        f'=IFERROR((G{next_row} + I{next_row}) / H{next_row},"")',
        mnav, btc_price_jpy_for_sheet, usd_jpy_manual, shares_outstanding_oku
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    print("✅ daily append done")

if __name__ == "__main__":
    run()
