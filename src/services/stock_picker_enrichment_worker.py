# -*- coding: utf-8 -*-
"""Subprocess entrypoint for stock picker enrichment."""

from __future__ import annotations

import argparse
import json
from datetime import date

from src.services.stock_picker_service import StockPickerService


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock picker enrichment in a subprocess")
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--query-id", type=str, required=True)
    parser.add_argument("--strategy-json", type=str, required=True)
    parser.add_argument("--scan-date", type=str, required=True)
    parser.add_argument("--llm-review-limit", type=int, required=True)
    parser.add_argument("--market-snapshot-json", type=str, required=True)
    parser.add_argument("--us-snapshot-json", type=str, required=True)
    parser.add_argument("--send-notification", type=str, default="0")
    args = parser.parse_args()

    StockPickerService._run_enrichment_worker(
        run_id=args.run_id,
        query_id=args.query_id,
        strategy=json.loads(args.strategy_json),
        scan_date=date.fromisoformat(args.scan_date),
        llm_review_limit=args.llm_review_limit,
        market_snapshot=json.loads(args.market_snapshot_json),
        us_snapshot=json.loads(args.us_snapshot_json),
        send_notification=args.send_notification == "1",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
