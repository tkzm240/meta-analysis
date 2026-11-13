"""Regression tests for currency, share basis, and source completeness."""
import copy
import unittest

from metaplanet_data import SourceDataError, fetch_latest, normalize_history


def fixture():
    # The provider's 'original' series misleadingly keeps debt/cash/BTC in USD.
    days = ["2025-12-18", "2025-12-19", "2025-12-20"]
    usd = {
        "dates": days,
        "stock_prices": [2, 2, 2],
        "btc_prices": [100_000, 100_000, 100_000],
        "btc_balance": [10, 10, 10],
        "btc_per_share": [.0001, .0001, .0001],
        "btc_per_diluted_share": [.00005, .00005, .00005],
        "total_outstanding_shares": [100_000, 100_000, 100_000],
        "diluted_shares_outstanding": [200_000, 200_000, 200_000],
        "market_cap_basic": [200_000, 200_000, 200_000],
        "holdings_value": [1_000_000, 1_000_000, 1_000_000],
        "debt": [100_000, 100_000, 100_000],
        "cash_balance": [50_000, 50_000, 50_000],
        "use_ev": [True, True, True],
    }
    original = copy.deepcopy(usd)
    original.update({
        "stock_prices": [200, 250, 400],
        "market_cap_basic": [20_000_000, 25_000_000, 40_000_000],
        "holdings_value": [100_000_000, 125_000_000, 200_000_000],
        "historical_fx_rates": [.01, .008, .005],
    })
    return {
        "timestamp": "2025-12-20T09:00:00+00:00",
        "companies": {"3350.T": {
            "historicalData": usd,
            "historicalDataOriginal": original,
            "currencyMetadata": {"originalCurrency": "JPY", "targetCurrency": "USD"},
            "processedMetrics": {"preferredStocks": [{
                "currency": "JPY", "notionalMillions": 90,
                "history": [{"effective_date": "2025-12-19", "currency": "JPY", "notional_millions": 10}],
            }]},
        }},
    }


class DataTests(unittest.TestCase):
    def test_uses_daily_fx_and_preserves_diluted_basis(self):
        rows = normalize_history(fixture())
        self.assertEqual(rows[0]["btc_price_jpy"], 10_000_000)
        self.assertEqual(rows[2]["btc_price_jpy"], 20_000_000)
        self.assertEqual(rows[2]["debt_jpy"], 20_000_000)
        self.assertEqual(rows[2]["share_price_jpy"], 400)
        self.assertEqual(rows[2]["btc_per_1000"], .05)
        self.assertEqual(rows[2]["btc_per_1000_basic"], .1)
        self.assertEqual(rows[2]["diluted_shares"], 200_000)
        self.assertEqual(rows[2]["usd_jpy"], 200)

    def test_preferred_starts_on_effective_date_not_current_amount(self):
        rows = normalize_history(fixture())
        self.assertEqual([r["preferred_jpy"] for r in rows], [0, 10_000_000, 10_000_000])
        self.assertAlmostEqual(rows[0]["mnav"], .25)
        self.assertAlmostEqual(rows[1]["mnav"], .33)
        self.assertAlmostEqual(rows[2]["mnav"], .30)
        self.assertEqual([r["mnav_legacy"] for r in rows], [.3, .3, .3])

    def test_missing_debt_after_adoption_stops_update(self):
        payload = fixture()
        payload["companies"]["3350.T"]["historicalData"]["debt"][0] = None
        with self.assertRaises(SourceDataError): normalize_history(payload)

    def test_null_preadoption_balances_are_preserved(self):
        payload = fixture()
        c = payload["companies"]["3350.T"]
        fields = ("btc_balance", "btc_per_share", "btc_per_diluted_share", "total_outstanding_shares",
                  "diluted_shares_outstanding", "market_cap_basic", "holdings_value", "debt", "cash_balance")
        for key in fields:
            c["historicalData"][key][0] = None
            c["historicalDataOriginal"][key][0] = None
        c["historicalData"]["use_ev"][0] = None
        row = normalize_history(payload)[0]
        for key in ("debt_jpy", "btc_holdings", "mnav", "mnav_legacy"):
            self.assertIsNone(row[key])

    def test_missing_latest_required_fields_stops_update(self):
        for section, field in (("historicalData", "btc_per_diluted_share"), ("historicalData", "debt"),
                               ("historicalData", "diluted_shares_outstanding"),
                               ("historicalDataOriginal", "historical_fx_rates"),
                               ("historicalDataOriginal", "market_cap_basic"),
                               ("historicalDataOriginal", "holdings_value")):
            with self.subTest(field=field):
                payload = fixture()
                payload["companies"]["3350.T"][section][field][-1] = None
                with self.assertRaises(SourceDataError): normalize_history(payload)

    def test_unknown_preferred_history_is_not_flat_filled(self):
        payload = fixture()
        payload["companies"]["3350.T"]["processedMetrics"]["preferredStocks"][0]["history"] = []
        with self.assertRaises(SourceDataError): normalize_history(payload)

    def test_unknown_mnav_definition_stops_update(self):
        payload = fixture()
        payload["companies"]["3350.T"]["historicalData"]["use_ev"][0] = None
        with self.assertRaises(SourceDataError): normalize_history(payload)

    def test_zero_btc_has_no_nav_multiple(self):
        payload = fixture()
        company = payload["companies"]["3350.T"]
        for key in ("btc_balance", "btc_per_share", "btc_per_diluted_share", "holdings_value"):
            company["historicalData"][key][0] = 0
        company["historicalDataOriginal"]["holdings_value"][0] = 0
        row = normalize_history(payload)[0]
        self.assertEqual(row["btc_holdings"], 0)
        self.assertIsNone(row["mnav"])
        self.assertIsNone(row["mnav_legacy"])

    def test_latest_keeps_provider_day(self):
        row = fetch_latest(fixture())
        self.assertEqual(row["as_of_date"], "2025-12-20")
        self.assertEqual(row["source_timestamp"], "2025-12-20T09:00:00+00:00")

    def test_rejects_misaligned_missing_or_nonfinite_values(self):
        for kind in ("short", "missing", "nan", "string", "negative", "zero_fx", "currency", "jpy_mismatch", "shares"):
            with self.subTest(kind=kind):
                payload = fixture()
                c = payload["companies"]["3350.T"]
                if kind == "short": c["historicalData"]["debt"].pop()
                if kind == "missing": del c["historicalData"]["debt"]
                if kind == "nan": c["historicalData"]["debt"][0] = float("nan")
                if kind == "string": c["historicalData"]["btc_prices"][0] = "$100K"
                if kind == "negative": c["historicalData"]["btc_balance"][0] = -1
                if kind == "zero_fx": c["historicalDataOriginal"]["historical_fx_rates"][0] = 0
                if kind == "currency": c["currencyMetadata"]["originalCurrency"] = "EUR"
                if kind == "jpy_mismatch": c["historicalDataOriginal"]["stock_prices"][0] = 2
                if kind == "shares": c["historicalData"]["btc_per_diluted_share"][0] *= 2
                with self.assertRaises(SourceDataError): normalize_history(payload)

    def test_rejects_incomplete_dates(self):
        for dates in ([], ["2025-12-18"] * 3, ["2025-12-17", "2025-12-19", "2025-12-20"],
                      ["2025-12-20", "2025-12-19", "2025-12-18"], ["bad", "2025-12-19", "2025-12-20"]):
            with self.subTest(dates=dates):
                payload = fixture()
                c = payload["companies"]["3350.T"]
                c["historicalData"]["dates"] = dates
                c["historicalDataOriginal"]["dates"] = dates
                with self.assertRaises(SourceDataError): normalize_history(payload)


if __name__ == "__main__":
    unittest.main()
