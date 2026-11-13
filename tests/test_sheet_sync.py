import copy
import unittest

from sheet_sync import HEADERS, data_row, plan_updates, sync_worksheet


LEGACY_HEADERS = [
    "g", "Time", "BTC保有量", "1000株当たりのBTC", "株価(円)【SBI】",
    "BTC Price ($) : https://jp.investing.com/crypto/bitcoin/historical-data",
    "MarketCap", "BTCNAV", "Debt", "mNAV", "real_mNAV", "BTC価格(万円）",
    "ドル円", "株式数(億）",
]


def record(day="2026-11-13"):
    return {"date": day, "btc_holdings": 44000, "btc_per_1000": .028,
            "share_price_jpy": 219, "btc_price_usd": 68000,
            "btc_price_jpy": 11000000, "market_cap_jpy": 280e9,
            "btc_nav_jpy": 480e9, "debt_jpy": 120e9, "mnav": .87,
            "usd_jpy": 160, "diluted_shares": 1.5e9}


class SheetTests(unittest.TestCase):
    def test_units_and_legacy_vs_official_mnav(self):
        row = data_row(record(), 1)
        self.assertEqual(row[6:9], [280, 480, 120])
        self.assertAlmostEqual(row[9], 400 / 480)
        self.assertEqual(row[10:14], [.87, 1100, 160, 15])

    def test_missing_debt_does_not_become_zero(self):
        source = record()
        source["debt_jpy"] = None
        self.assertEqual(data_row(source, 1)[8:10], ["", ""])
        self.assertIn("COUNT", data_row(source, 1, 2)[9])

    def test_new_sheet_rows_keep_manual_fx_blank_but_csv_has_source_fx(self):
        source = record()
        self.assertEqual(data_row(source, 1)[12], 160)
        changes, _ = plan_updates([HEADERS], [source])
        self.assertEqual(changes[0]["values"][0][12], "")
        old = [HEADERS, data_row(source, 1, 2)]
        source["usd_jpy"] = 161
        changes, _ = plan_updates(old, [source])
        self.assertEqual(changes, [])

    def test_rerun_no_duplicates_and_preserves_manual_fx(self):
        row = data_row(record(), 42, 2)
        row[12] = '=100+61'
        old = [HEADERS, row]
        changes, report = plan_updates(old, [record()])
        self.assertEqual(changes, [])
        self.assertEqual(report["added"], 0)
        new = record()
        new["share_price_jpy"] = 220
        changes, _ = plan_updates(old, [new])
        self.assertEqual(changes[0]["values"][0][12], '=100+61')
        self.assertEqual(changes[0]["values"][0][1], 42)

    def test_backfill_does_not_move_existing_rows_or_extra_columns(self):
        old = [HEADERS + ["notes"], data_row(record(), 3, 2) + ["keep this"]]
        original = copy.deepcopy(old)
        changes, report = plan_updates(old, [record("2026-11-12"), record()])
        self.assertEqual(old, original)
        self.assertEqual(report["added"], 1)
        self.assertEqual(changes[0]["range"], "A3:N3")
        self.assertEqual(len(changes[0]["values"][0]), 14)

    def test_existing_duplicate_dates_all_repaired_without_new_duplicates(self):
        old = [HEADERS, data_row(record(), 1, 2), data_row(record(), 2, 3)]
        old[1][2] = old[2][2] = ""
        changes, report = plan_updates(old, [record()])
        self.assertEqual(report["updated"], 2)
        self.assertEqual(report["added"], 0)
        self.assertEqual(report["duplicate_existing_dates"], 1)

    def test_production_legacy_header_is_accepted_and_preserved(self):
        old = [list(LEGACY_HEADERS), data_row(record(), 1, 2)]
        old[1][2] = ""
        original = copy.deepcopy(old)
        changes, report = plan_updates(old, [record()])
        self.assertEqual(report["updated"], 1)
        self.assertEqual(report["added"], 0)
        self.assertEqual([change["range"] for change in changes], ["A2:N2"])
        self.assertEqual(changes[0]["values"][0][2], 44000)
        self.assertEqual(old, original)
        self.assertEqual(old[0], LEGACY_HEADERS)

    def test_legacy_date_header_does_not_allow_wrong_financial_columns(self):
        for column, wrong_label in ((3, "BTC Price"), (9, "Market Cap")):
            with self.subTest(column=column):
                header = list(LEGACY_HEADERS)
                header[column] = wrong_label
                with self.assertRaisesRegex(ValueError, "Unexpected A:N column layout"):
                    plan_updates([header, data_row(record(), 1, 2)], [record()])

    def test_legacy_header_still_rejects_invalid_row_dates(self):
        old = [list(LEGACY_HEADERS), data_row(record(), 1, 2)]
        old[1][0] = "not a date"
        with self.assertRaisesRegex(ValueError, "Unsupported sheet date"):
            plan_updates(old, [record()])

    def test_rejects_unrecognized_layout_and_source_before_writing(self):
        for source in ([], [record(), record()], [record(), record("2026-11-12")]):
            with self.assertRaises(ValueError):
                plan_updates([HEADERS], source)
        with self.assertRaises(ValueError):
            plan_updates([["unrelated sheet"]], [record()])

    def test_backup_precedes_mutation_and_readback_is_verified(self):
        events = []
        class Worksheet:
            title, id, row_count, col_count = "data", 1, 1000, 20
            def __init__(self):
                self.rows = [HEADERS, data_row(record(), 1, 2)]
                self.rows[1][2] = ""
            def get(self, *args, **kwargs):
                return copy.deepcopy(self.rows)
            def batch_update(self, updates, **kwargs):
                events.append("write")
                for update in updates:
                    n = int(update["range"].split(":")[0][1:]) - 1
                    self.rows[n] = list(update["values"][0])
                    self.rows[n][0] = "2026/11/13"  # Sheets may change date presentation.
        ws = Worksheet()
        class Spreadsheet:
            def worksheets(self):
                return [ws]
            def duplicate_sheet(self, *args, **kwargs):
                events.append("backup")
        report = sync_worksheet(Spreadsheet(), ws, [record()])
        self.assertEqual(events, ["backup", "write"])
        self.assertTrue(report["verified"])


if __name__ == "__main__":
    unittest.main()

