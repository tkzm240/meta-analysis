import os, json, sys, time
from datetime import datetime

# ===== Selenium =====
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

chrome_path = os.environ.get("CHROME_PATH")
if chrome_path:
    options.binary_location = chrome_path
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

def click_yen_button(driver, wait, card_name):
    cards_with_yen_button = ["Bitcoin NAV", "Debt Outstanding", "Market Cap", "Bitcoin Price"]
    if card_name not in cards_with_yen_button:
        return
    try:
        card_xpath = f"//div[contains(@class, 'rounded') and .//*[text()='{card_name}']]"
        card = wait.until(EC.presence_of_element_located((By.XPATH, card_xpath)))
        driver.execute_script("arguments[0].scrollIntoView();", card)
        time.sleep(0.5)
        yen_button = card.find_element(By.XPATH, ".//button[normalize-space(text())='¥']")
        yen_button.click()
        WebDriverWait(driver, 5).until(lambda d: "¥" in card.text)
        print(f"¥ clicked: {card_name}")
    except Exception as e:
        print(f"[WARN] yen click failed: {card_name} {e}")

def extract_card_value(driver, wait, card_name):
    try:
        if card_name == "Bitcoin Price":
            value_elem = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[contains(@class, 'rounded')][1]//*[contains(text(),'$') or contains(text(),'¥')]")
            ))
            return value_elem.text.strip()

        card_xpath = f"//div[contains(@class, 'rounded') and .//*[text()='{card_name}']]"
        card = wait.until(EC.presence_of_element_located((By.XPATH, card_xpath)))
        driver.execute_script("arguments[0].scrollIntoView();", card)
        time.sleep(0.3)

        if card_name == "mNAV":
            value_elem = card.find_element(
                By.XPATH,
                ".//*[not(*) and string-length(normalize-space(text()))>0 "
                "and not(contains(text(),'¥')) and not(contains(text(),'$')) and not(contains(text(),'₿'))]"
            )
        else:
            value_elem = card.find_element(
                By.XPATH,
                ".//*[contains(text(),'¥') or contains(text(),'₿') or contains(text(),'$')]"
            )
        return value_elem.text.strip()
    except Exception as e:
        print(f"[WARN] extract failed: {card_name} {e}")
        return None

def clean_number(val):
    try:
        val = val.replace("¥","").replace("₿","").replace("$","").replace(",","").strip()
        if val.endswith("B"):
            return float(val[:-1])
        return float(val)
    except Exception:
        return None

def clean_btc_usd_from_k(val):
    try:
        s = val.replace("$","").replace(",","").strip().lower()
        if s.endswith("k"):
            return float(s[:-1]) * 1_000
        if s.endswith("m"):
            return float(s[:-1]) * 1_000_000
        return float(s)
    except Exception:
        return None

def clean_btc_jpy_from_M(val):
    try:
        s = val.replace("¥","").replace(",","").strip()
        if s.endswith("M"):
            s = s[:-1].strip()
        return float(s) * 100.0
    except Exception:
        return None

def get_next_time_value(ws):
    try:
        time_col = ws.col_values(2)[1:]
        if not time_col: return 1
        return int(time_col[-1].strip()) + 1
    except:
        return 1

def calc_shares_oku(btc_holdings, btc_per_1000):
    try:
        if not btc_holdings or not btc_per_1000: return ""
        return round((btc_holdings / btc_per_1000) * 1000.0 / 100_000_000.0, 6)
    except:
        return ""

def run():
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    target_cards = [
        "Bitcoin Price", "Share Price", "BTC Holdings", "Bitcoin NAV",
        "Debt Outstanding", "Market Cap", "mNAV", "Bitcoin per 1,000 Shares"
    ]
    cards_force_usd = ["Bitcoin Price"]

    try:
        driver.get("https://metaplanet.jp/en/analytics")
        results = {}
        for name in target_cards:
            if name not in cards_force_usd:
                click_yen_button(driver, wait, name)
            results[name] = extract_card_value(driver, wait, name)
            time.sleep(0.5)

        click_yen_button(driver, wait, "Bitcoin Price")
        btc_price_jpy_text = extract_card_value(driver, wait, "Bitcoin Price")
    finally:
        driver.quit()

    now = datetime.now()
    date_str = now.strftime("%Y/%m/%d")
    time_val = get_next_time_value(ws)

    btc_holdings  = clean_number(results.get("BTC Holdings") or "")
    btc_per_1000  = clean_number(results.get("Bitcoin per 1,000 Shares") or "")
    share_price   = clean_number(results.get("Share Price") or "")
    btc_price_usd = clean_btc_usd_from_k(results.get("Bitcoin Price") or "")
    market_cap    = clean_number(results.get("Market Cap") or "")
    btc_nav       = clean_number(results.get("Bitcoin NAV") or "")
    debt          = clean_number(results.get("Debt Outstanding") or "")
    mnav          = clean_number(results.get("mNAV") or "")
    btc_price_jpy_for_sheet = clean_btc_jpy_from_M(btc_price_jpy_text) if btc_price_jpy_text else None

    usd_jpy_manual = ""
    shares_outstanding_oku = calc_shares_oku(btc_holdings, btc_per_1000)

    header = ws.row_values(1)
    needed_headers = [
        "Date","Time","BTC保有量","1000株当たりBTC","株価(円)[SBI]","BTC Price ($)",
        "Market Cap","BTCNAV","Debt","mNAV","real_mNAV","BTC価格(円)",
        "ドル円(JPY/USD)","発行済み株式数(億)"
    ]
    if len(header) < len(needed_headers):
        ws.update('A1', [header + needed_headers[len(header):]])

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
