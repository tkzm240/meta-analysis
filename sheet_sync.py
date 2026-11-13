"""Date-keyed updates of the legacy A:N table without moving user cells."""
from datetime import datetime, timezone
import math

HEADERS = ["Date", "Time", "BTC保有量", "1000株当たりBTC", "株価(円)[SBI]",
           "BTC Price ($)", "Market Cap", "BTCNAV", "Debt", "mNAV", "real_mNAV",
           "BTC価格(万円)", "ドル円(JPY/USD)", "発行済み株式数(億)"]


def date_key(value):
    text = str(value).strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError(f"Unsupported sheet date: {text!r}")


def scaled(record, key, divisor=1):
    value = record.get(key)
    if value is None:
        return ""
    value = float(value) / divisor
    if not math.isfinite(value):
        raise ValueError(f"Non-finite {key} on {record['date']}")
    return value


def data_row(record, sequence, row_number=None):
    """Legacy money columns G:I are billion JPY; L is ten-thousand JPY."""
    cap, nav, debt = (scaled(record, key, 1e9) for key in
                      ("market_cap_jpy", "btc_nav_jpy", "debt_jpy"))
    legacy = (cap + debt) / nav if cap != "" and debt != "" and nav not in ("", 0) else ""
    if row_number is not None:
        r = row_number
        legacy = f'=IF(COUNT(G{r}:I{r})=3,IFERROR((G{r}+I{r})/H{r},""),"")'
    return [record["date"].replace("-", "/"), sequence,
            scaled(record, "btc_holdings"), scaled(record, "btc_per_1000"),
            scaled(record, "share_price_jpy"), scaled(record, "btc_price_usd"),
            cap, nav, debt, legacy, scaled(record, "mnav"),
            scaled(record, "btc_price_jpy", 1e4),
            scaled(record, "usd_jpy") if row_number is None else "",  # Live M remains manual.
            scaled(record, "diluted_shares", 1e8)]


def equal_cell(a, b):
    if str(a) == str(b):
        return True
    try:
        return math.isclose(float(a), float(b), rel_tol=1e-10, abs_tol=1e-10)
    except (ValueError, TypeError):
        return False


def plan_updates(existing, records, force=False):
    if not records:
        raise ValueError("Refusing to update from empty history")
    dates = [date_key(r["date"]) for r in records]
    if dates != sorted(set(dates)):
        raise ValueError("Source dates must be unique and ascending")
    updates, by_date = [], {}
    maximum_sequence = 0
    if existing:
        header = existing[0]
        if len(header) < 14 or header[0].strip().lower() != "date" or header[1].strip().lower() != "time":
            raise ValueError(f"Expected the existing 14-column Date/Time table in A:N; no cells changed; observed headers={header[:14]!r}")
        if "1000" not in header[3] or header[9].strip().lower() != "mnav":
            raise ValueError(f"Unexpected A:N column layout; no cells changed; observed headers={header[:14]!r}")
    else:
        updates.append({"range": "A1:N1", "values": [HEADERS]})
    for n, values in enumerate(existing[1:], 2):
        row = list(values[:14]) + [""] * max(0, 14 - len(values))
        if not any(str(v).strip() for v in row):
            continue
        key = date_key(row[0])
        by_date.setdefault(key, []).append((n, row))
        try:
            maximum_sequence = max(maximum_sequence, int(row[1]))
        except (ValueError, TypeError):
            pass
    next_row = max(2, len(existing) + 1)
    changed, added = 0, 0
    for record in records:
        matches = by_date.get(record["date"], [])
        if not matches:
            maximum_sequence += 1
            row = data_row(record, maximum_sequence, next_row)
            updates.append({"range": f"A{next_row}:N{next_row}", "values": [row]})
            next_row += 1
            added += 1
        for n, old in matches:
            row = data_row(record, old[1], n)
            row[0] = old[0]  # Retain original date presentation and row identity.
            if old[12] != "":
                row[12] = old[12]  # Column M was explicitly reserved for manual FX.
            if force or not all(equal_cell(a, b) for a, b in zip(old, row)):
                updates.append({"range": f"A{n}:N{n}", "values": [row]})
                changed += 1
    return updates, {"updated": changed, "added": added, "source_days": len(records),
                     "duplicate_existing_dates": sum(len(v) - 1 for v in by_date.values()),
                     "required_rows": max(len(existing), next_row - 1)}


def sync_worksheet(spreadsheet, worksheet, records, mode="sync"):
    existing = worksheet.get("A:N", value_render_option="FORMULA", date_time_render_option="FORMATTED_STRING")
    updates, report = plan_updates(existing, records, force=mode == "rebuild")
    if not updates:
        report["verified"] = True
        return report
    # A single recovery snapshot is retained; never put private sheet copies in artifacts.
    backup_name = worksheet.title[:65] + "_before_api_recovery"
    titles = {w.title for w in spreadsheet.worksheets()}
    if report["updated"] and backup_name not in titles:
        spreadsheet.duplicate_sheet(worksheet.id, new_sheet_name=backup_name)
        report["backup_sheet"] = backup_name
    if report["required_rows"] > worksheet.row_count or worksheet.col_count < 14:
        worksheet.resize(rows=max(worksheet.row_count, report["required_rows"]),
                         cols=max(14, worksheet.col_count))
    worksheet.batch_update(updates, value_input_option="USER_ENTERED")
    actual = worksheet.get("A:N", value_render_option="FORMULA", date_time_render_option="FORMATTED_STRING")
    for update in updates:
        n = int(update["range"].split(":")[0][1:]) - 1
        wanted = update["values"][0]
        observed = list(actual[n]) if n < len(actual) else []
        observed += [""] * (14 - len(observed))
        if n == 0:
            matches = all(equal_cell(a, b) for a, b in zip(wanted, observed))
        else:
            matches = date_key(wanted[0]) == date_key(observed[0]) and all(
                equal_cell(a, b) for a, b in zip(wanted[1:], observed[1:]))
        if not matches:
            raise RuntimeError(f"Sheet verification failed at row {n + 1}")
    report["verified"] = True
    return report

