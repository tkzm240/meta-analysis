# データ取得元と互換性

## 取得元

旧URL `https://metaplanet.jp/en/analytics` は、現在 `https://analytics.metaplanet.jp/?lang=en` にリダイレクトされます。新サイトは StrategyTracker のアプリケーションです。旧サイトの「rounded」カードや「¥」ボタンを探す Selenium の処理では取得できなくなっています。

修正版は、現在の公式画面自身が読み込む公開JSONを利用します。

- 最新版の参照: `https://data.strategytracker.com/latest.json`
- Metaplanetの版固定データ: `https://data.strategytracker.com/3350_T.v{version}.json`
- 対象銘柄: `3350.T`

取得にはログインや秘密鍵は不要です。公式サイトを Referer/Origin として指定します。サイトが将来この公開データを変更・廃止した場合には対応が必要です。

2026-11-14 JSTの検証時点では、2024-04-08〜2026-11-13の950日分を取得できました。保有BTCが正の期間は2024-04-23以降の935日分です。初日の保有量等は未設定で、その後の購入前の期間は保有量ゼロです。NAV倍率を計算できない15日分は欠損のまま扱います。

## 数値と単位

| 正規化フィールド | 取得元・意味 |
| --- | --- |
| `btc_holdings` | `historicalData.btc_balance`（BTC） |
| `btc_per_1000` | `btc_per_diluted_share × 1000`（希薄化後1000株当たりBTC） |
| `diluted_shares` | `diluted_shares_outstanding`（株） |
| `basic_shares` | `total_outstanding_shares`（普通株式数） |
| `share_price_jpy` | `historicalDataOriginal.stock_prices`（円） |
| `btc_price_usd` | `historicalData.btc_prices`（米ドル） |
| `btc_price_jpy` | USDのBTC価格 ÷ 当日の `historical_fx_rates`（円） |
| `market_cap_jpy` | `historicalDataOriginal.market_cap_basic`（普通株式の時価総額、円） |
| `btc_nav_jpy` | `historicalDataOriginal.holdings_value`（保有BTCの評価額、円） |
| `debt_jpy`, `cash_jpy` | USDの債務・現金 ÷ 当日の為替レート（円） |
| `usd_jpy` | `1 / historical_fx_rates`（1米ドル当たり円） |
| `preferred_jpy` | 優先株の適用日別額面金額（円） |
| `mnav_legacy` | `(普通株時価総額 + 債務) / BTC評価額` |
| `mnav` | 公式のEV倍率。`use_ev`が真なら `(普通株時価総額 + 債務 + 優先株 - 現金) / BTC評価額` |

**注意:** `historicalDataOriginal` は完全な円建てデータではありません。BTC価格・債務・現金は同セクションでも米ドルのままです。`historical_fx_rates` は「1円当たり米ドル」であり、逆数を使います。円・ドルの値を混在させない検証を行っています。

旧グラフの2026-02-25の `1000株当たりBTC = 0.02355382` は、新データの希薄化後の値 `0.02355381800314` と一致し、普通株式ベースの値とは一致しません。このため既存列の希薄化後という意味を維持します。既存シートのN列名は「発行済み株式数(億)」ですが、旧スクリプトがD列から逆算していた値も希薄化後株数です。

既存シートのG〜I列は十億円、L列は万円、N列は億株として出力します。`metaplanet_data.py` 自体は円・ドル・株の基本単位を返し、シートへの変換を別処理に分けています。既存J列の算式を維持し、K列には現在の公式EV定義の倍率を記録します。この2種類は優先株や現金が存在する場合に一致しません。

優先株は現在額を過去に遡って適用せず、ソースの `effective_date` と `notional_millions` を使用します。MERCURYはソース上2025-12-19からの額面金額236.1億円です。履歴を確認できない優先株が追加されてBTC保有期間の公式EV倍率を計算できない場合は、既存値を空欄で上書きせず、書き込み前にエラーで停止します。

## 復元の範囲と更新

この再取得は**現在の公式サイトが公開する日次履歴**です。旧プログラムが各日17:00 JSTに観測した瞬間の価格を完全再現するものではありません。過去のソース訂正や株数・株価の調整も含まれます。休日の値も提供元の日次系列をそのまま使い、独自の補間や前日値の補完はしません。スナップショットの更新時刻とデータの日付を分けて保存し、取得日を過去データの日付として付け直しません。

毎回全日次履歴を取得するため、更新停止中の欠損だけでなく、提供元による過去データの訂正も反映できます。既存シートの退避・日付単位の更新は同期処理側で行います。

各取得で配列長、連続・一意な日付、数値、通貨、円ドル変換、株数とBTC/株の関係を検証します。古いスナップショット、不正な値や構造がある場合は書き込み前に失敗させます。
