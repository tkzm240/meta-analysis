# データ取得の復旧

取得元が `https://analytics.metaplanet.jp/` に移転したため、画面のカードを読む処理を、同サイトが利用する公開JSONを取得する処理に変更しました。

## 復旧対象

- 公開元の全履歴を日付で照合し、既存行を修復・不足日を追加します。
- 2026年11月14日の検証時は2024-04-08〜2026-11-13の950日。BTC保有開始後の分析対象は935日です。
- 購入前の欠損は空欄のままです。取得元が現在提供する日次履歴であり、過去の17時の画面表示をそのまま再現するものではありません。
- 既存のA:Nの列順と計算方法を維持します。手入力のM列、O列以降、既存行の位置を保存します。M列の空欄には自動入力せず、取得元の為替は出力CSVに記録します。
- 最初の既存値修正前に、同じスプレッドシート内へ `<シート名>_before_api_recovery` を複製します。このバックアップは一度だけ作成し、以後上書きしません。
- 同日の追加を防ぎ、既存の重複行は同じ値へ修復します。グラフでは日付ごとに最後の1行を使います。
- 欠損・単位不整合・古い取得元・列構成の不一致は、書き込み前にエラーとして終了します。

単位と定義の詳細は [DATA_SOURCE.md](DATA_SOURCE.md) を参照してください。

## 既存設定

GitHub ActionsのSecretsは既存の `SPREADSHEET_ID`、`WORKSHEET_NAME`、`GSA_JSON` を使います。新たな認証情報は不要です。取得先シートにそのサービスアカウントの編集権限が必要です。

- `daily-run`: 毎日17:00 JST (`0 8 * * *`)、変更なし。
- `mnav-hourly`: 毎日10〜17時の毎時 (`0 1-8 * * *`)、変更なし。
- 日次処理の成功後にもグラフを更新します。
- 関連コードをmainへ反映すると、初回の履歴修復を自動実行します。
- GitHubの混雑による実行遅延はあり得ます。

## 反映と確認

1. 修正版をリポジトリのmainへ反映します。付属の `meta-analysis-fix.patch` はコード・テスト・説明書・ワークフローの変更を含みます。生成済み画像や秘密情報は含みません。
2. Actionsの `daily-run` が起動し、テスト、データ検証、バックアップ、既存シートの修復を行います。
3. `Restore history and update sheet` のログで `verified: true`、対象日数、追加・修正件数を確認します。
4. 続く `mnav-hourly` の成功と公開ページの `Historical data through` を確認します。生成日時だけで最新データと判断しないでください。
5. 再実行は Actions → daily-run → Run workflow → `sync`。全日付を強制的に再書き込みする場合は `rebuild` を選びます。どちらも既存の手入力欄を保存します。

```sh
git apply --check /path/to/meta-analysis-fix.patch
git apply /path/to/meta-analysis-fix.patch
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python main.py --dry-run --output-dir data
git add main.py metaplanet_data.py sheet_sync.py generate_mNAV_graph.py requirements.txt tests .github/workflows .gitignore DATA_SOURCE.md RECOVERY.md
git commit -m "Restore official Metaplanet history and scheduled updates"
git push origin main
```

本番反映前にリポジトリ側で別の修正が入っている場合、`git apply --check` が停止します。差分を確認して統合してください。

## ローカル検証

```sh
python main.py --dry-run --output-dir data
```

認証なしで取得元の全データと `history.csv`、`provenance.json` を出力します。CSVだけでグラフを生成する場合は `DATA_CSV` にCSVのパス、`FETCH_FROM_SITE=0` を指定して `python generate_mNAV_graph.py` を実行します。PNG出力にはChromeが必要です。

本番シートの読み書きとActions実行の確認は、実際のGitHub接続と既存Secretsが利用できる環境で行ってください。
