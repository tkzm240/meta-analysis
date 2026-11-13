"""Recover and synchronize Metaplanet's complete official daily history."""
import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path

from metaplanet_data import fetch_snapshot, normalize_history
from sheet_sync import HEADERS, data_row, sync_worksheet

JST = timezone(timedelta(hours=9))


def export_history(payload, records, output_dir):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2).encode('utf-8')
    (output / 'source.json').write_bytes(raw)
    with (output / 'history.csv').open('w', newline='', encoding='utf-8-sig') as stream:
        writer = csv.writer(stream)
        writer.writerow(HEADERS)
        for index, record in enumerate(records, 1):
            writer.writerow(data_row(record, index))
    metadata = {'source': payload.get('_source', {}), 'rows': len(records),
                'first_date': records[0]['date'], 'last_date': records[-1]['date'],
                'sha256': hashlib.sha256(raw).hexdigest(),
                'units': {'G:I': 'billion JPY', 'L': 'ten-thousand JPY', 'N': '100 million diluted shares'},
                'definitions': {'D': 'BTC per 1,000 diluted shares',
                                'J': '(basic market cap + debt) / BTC NAV (legacy)',
                                'K': 'official enterprise value / BTC NAV'},
                'null_policy': 'Unavailable source values remain blank; no interpolation.'}
    (output / 'provenance.json').write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding='utf-8')
    return metadata


def connect_sheet():
    import gspread
    from google.oauth2.service_account import Credentials
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    if os.environ.get('GSA_JSON'):
        credentials = Credentials.from_service_account_info(json.loads(os.environ['GSA_JSON']), scopes=scope)
    elif os.environ.get('KEY_PATH'):
        credentials = Credentials.from_service_account_file(os.environ['KEY_PATH'], scopes=scope)
    else:
        raise ValueError('GSA_JSON or KEY_PATH must be configured')
    spreadsheet = gspread.authorize(credentials).open_by_key(os.environ['SPREADSHEET_ID'])
    worksheet = spreadsheet.worksheet(os.environ.get('WORKSHEET_NAME') or 'データシート')
    return spreadsheet, worksheet


def run(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('sync', 'rebuild'), default='sync')
    parser.add_argument('--output-dir', default='data')
    parser.add_argument('--dry-run', action='store_true', help='Download and validate without Google credentials or writes')
    parser.add_argument('--source-json', type=Path, help='Replay a downloaded source snapshot (dry-run only)')
    args = parser.parse_args(argv)
    if args.source_json and not args.dry_run:
        parser.error('--source-json requires --dry-run; production always retrieves the source')
    payload = json.loads(args.source_json.read_text(encoding='utf-8')) if args.source_json else fetch_snapshot()
    records = normalize_history(payload)
    if not records:
        raise ValueError('Official history is empty; no sheet cells were changed')
    last_date = datetime.strptime(records[-1]['date'], '%Y-%m-%d').date()
    age = (datetime.now(JST).date() - last_date).days
    if not args.source_json and not 0 <= age <= 3:
        raise ValueError(f'Source history is stale or future-dated ({last_date}); no sheet cells were changed')
    metadata = export_history(payload, records, args.output_dir)
    print(f"Validated {len(records)} days: {metadata['first_date']} through {metadata['last_date']}")
    if args.dry_run:
        print('Dry run: Google Sheets unchanged')
        return
    spreadsheet, worksheet = connect_sheet()
    report = sync_worksheet(spreadsheet, worksheet, records, mode=args.mode)
    (Path(args.output_dir) / 'sync_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    run()
