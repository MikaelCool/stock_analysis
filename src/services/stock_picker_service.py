# -*- coding: utf-8 -*-
"""A-share stock picking service."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import ast
from difflib import SequenceMatcher
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import yaml
from sqlalchemy import and_, desc, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from data_provider import DataFetcherManager
from data_provider.baostock_fetcher import BaostockFetcher
from src.agent.skills.base import load_skills_from_directory
from src.analyzer import GeminiAnalyzer
from src.config import get_config
from src.core.trading_calendar import get_effective_trading_date
from src.notification import NotificationService
from src.search_service import SearchResponse, SearchResult, SearchService
from src.storage import (
    DatabaseManager,
    NewsIntel,
    StockDaily,
    StockSelectionBacktest,
    StockSelectionCandidate,
    StockSelectionOptimization,
    StockSelectionRun,
)

logger = logging.getLogger(__name__)

BACKTEST_HORIZONS = (1, 3, 5, 10)
_BUILTIN_STRATEGY_DIR = Path(__file__).resolve().parents[2] / "strategies"
_ENRICHMENT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs" / "picker_enrichment"
_MAX_SELECTED_CANDIDATES = 10
_MAX_REPORT_CANDIDATES = 5
_RANKING_PREFILTER_CANDIDATES = 5
CORE_SCHEDULED_STRATEGIES = ("swing_trend_follow", "main_force_breakout", "shanliu_theme_flow")
_PICKER_STRATEGY_PRESETS: Dict[str, Dict[str, Any]] = {
    "mainboard_swing_master": {
        "scan_family": "mainboard_swing_master",
        "priority": 5,
        "params": {
            "min_score_threshold": 72.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 4.0,
            "max_ma5_distance_pct": 2.0,
            "market_score_floor": 50.0,
            "preferred_setup_type": "",
            "pullback_min_score_threshold": 70.0,
            "pullback_volume_spike_multiplier": 1.4,
            "pullback_max_ma20_distance_pct": 3.5,
            "pullback_max_ma5_distance_pct": 1.8,
            "pullback_market_score_floor": 48.0,
            "breakout_min_score_threshold": 74.0,
            "breakout_volume_spike_multiplier": 1.8,
            "breakout_max_ma20_distance_pct": 3.0,
            "breakout_max_ma5_distance_pct": 2.2,
            "breakout_market_score_floor": 55.0,
            "trend_follow_min_score_threshold": 72.0,
            "trend_follow_volume_spike_multiplier": 1.2,
            "trend_follow_max_ma20_distance_pct": 12.0,
            "trend_follow_max_ma5_distance_pct": 3.5,
            "trend_follow_market_score_floor": 48.0,
        },
    },
    "swing_trend_follow": {
        "scan_family": "mainboard_swing_master",
        "priority": 3,
        "params": {
            "min_score_threshold": 73.0,
            "trend_follow_min_score_threshold": 73.0,
            "trend_follow_volume_spike_multiplier": 1.15,
            "trend_follow_max_ma20_distance_pct": 13.0,
            "trend_follow_max_ma5_distance_pct": 3.8,
            "trend_follow_market_score_floor": 46.0,
            "ma_gap_balance_limit_pct": 2.6,
            "ma_gap_min_pct": 0.08,
            "preferred_setup_type": "trend_follow",
        },
    },
    "main_force_breakout": {
        "scan_family": "mainboard_swing_master",
        "priority": 4,
        "params": {
            "min_score_threshold": 74.0,
            "main_force_min_score_threshold": 74.0,
            "main_force_cross_volume_multiplier": 1.25,
            "main_force_pullback_max_ma20_distance_pct": 3.0,
            "main_force_shrink_volume_ratio": 1.05,
            "main_force_limitup_lookback_days": 60,
            "main_force_market_score_floor": 48.0,
            "preferred_setup_type": "main_force_breakout",
        },
    },
    "shanliu_theme_flow": {
        "scan_family": "mainboard_swing_master",
        "priority": 6,
        "params": {
            "min_score_threshold": 58.0,
            "shanliu_min_score_threshold": 76.0,
            "shanliu_volume_spike_multiplier": 1.25,
            "shanliu_amount_ratio_min": 1.05,
            "shanliu_market_score_floor": 44.0,
            "shanliu_theme_required_score_delta": 4.0,
            "preferred_setup_type": "shanliu",
        },
    },
    "bull_trend": {
        "scan_family": "shrink_pullback",
        "priority": 10,
        "params": {
            "min_score_threshold": 70.0,
            "volume_spike_multiplier": 1.4,
            "max_ma20_distance_pct": 4.0,
            "max_ma5_distance_pct": 2.0,
            "market_score_floor": 48.0,
            "preferred_setup_type": "pullback",
        },
    },
    "ma_golden_cross": {
        "scan_family": "ma_golden_cross",
        "priority": 20,
        "params": {
            "min_score_threshold": 70.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 3.5,
            "max_ma5_distance_pct": 2.2,
            "market_score_floor": 50.0,
            "preferred_setup_type": "breakout",
        },
    },
    "shrink_pullback": {
        "scan_family": "shrink_pullback",
        "priority": 30,
        "params": {
            "min_score_threshold": 68.0,
            "volume_spike_multiplier": 1.5,
            "max_ma20_distance_pct": 4.0,
            "max_ma5_distance_pct": 2.0,
            "market_score_floor": 48.0,
            "preferred_setup_type": "pullback",
        },
    },
    "volume_breakout": {
        "scan_family": "volume_breakout",
        "priority": 40,
        "params": {
            "min_score_threshold": 72.0,
            "volume_spike_multiplier": 2.0,
            "max_ma20_distance_pct": 5.0,
            "max_ma5_distance_pct": 2.5,
            "market_score_floor": 52.0,
            "preferred_setup_type": "breakout",
        },
    },
    "dragon_head": {
        "scan_family": "volume_breakout",
        "priority": 50,
        "params": {
            "min_score_threshold": 78.0,
            "volume_spike_multiplier": 2.2,
            "max_ma20_distance_pct": 5.5,
        },
    },
    "chan_theory": {
        "scan_family": "mainboard_swing_master",
        "priority": 60,
        "params": {
            "min_score_threshold": 72.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 3.2,
        },
    },
    "wave_theory": {
        "scan_family": "mainboard_swing_master",
        "priority": 70,
        "params": {
            "min_score_threshold": 71.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 3.8,
        },
    },
    "emotion_cycle": {
        "scan_family": "mainboard_swing_master",
        "priority": 80,
        "params": {
            "min_score_threshold": 66.0,
            "volume_spike_multiplier": 1.3,
            "max_ma20_distance_pct": 4.5,
        },
    },
    "box_oscillation": {
        "scan_family": "volume_breakout",
        "priority": 90,
        "params": {
            "min_score_threshold": 67.0,
            "volume_spike_multiplier": 1.5,
            "max_ma20_distance_pct": 4.5,
        },
    },
    "bottom_volume": {
        "scan_family": "volume_breakout",
        "priority": 100,
        "params": {
            "min_score_threshold": 65.0,
            "volume_spike_multiplier": 1.4,
            "max_ma20_distance_pct": 4.8,
        },
    },
    "one_yang_three_yin": {
        "scan_family": "shrink_pullback",
        "priority": 110,
        "params": {
            "min_score_threshold": 69.0,
            "volume_spike_multiplier": 1.4,
            "max_ma20_distance_pct": 4.0,
        },
    },
}


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    description: str
    skill_id: str
    category: str
    params: Dict[str, Any]


@dataclass
class StrategySignal:
    passed: bool
    score: float
    setup_type: str
    operation_advice: str
    analysis_summary: str
    reasons: List[str]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    metrics: Dict[str, Any]


class StockPickerService:
    """Quantitative scan + LLM review workflow for post-close stock picking."""

    _scan_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="picker_scan")
    _background_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="picker_bg")
    _schedule_lock = threading.Lock()
    _global_board_cache: Dict[str, List[Dict[str, Any]]] = {}
    _global_board_cache_lock = threading.Lock()

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        *,
        config=None,
        lightweight: bool = False,
    ) -> None:
        self.config = config or get_config()
        self.db = db_manager or DatabaseManager.get_instance()
        self.lightweight = bool(lightweight)
        self.fetcher_manager = None
        self.search_service = None
        self.analyzer = None
        self._board_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._sector_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self._capital_flow_cache: Dict[str, Optional[float]] = {}
        if self.lightweight:
            return

        self.fetcher_manager = DataFetcherManager()
        try:
            self.search_service = SearchService(
                bocha_keys=self.config.bocha_api_keys,
                tavily_keys=self.config.tavily_api_keys,
                anspire_keys=self.config.anspire_api_keys,
                brave_keys=self.config.brave_api_keys,
                serpapi_keys=self.config.serpapi_keys,
                minimax_keys=self.config.minimax_api_keys,
                searxng_base_urls=self.config.searxng_base_urls,
                searxng_public_instances_enabled=self.config.searxng_public_instances_enabled,
                news_max_age_days=self.config.news_max_age_days,
                news_strategy_profile=getattr(self.config, "news_strategy_profile", "short"),
            )
        except Exception as exc:
            logger.warning("Stock picker search service unavailable: %s", exc)
        self.analyzer = GeminiAnalyzer(config=self.config)

    @staticmethod
    def list_strategies() -> List[Dict[str, Any]]:
        strategies = [
            StrategyDefinition(
                strategy_id="mainboard_swing_master",
                name="主力波段双模",
                description="收盘后筛选主板非 ST 标的，兼顾突破启动和趋势回踩，叠加市场情绪与消息面审核。",
                skill_id="swing_after_close_picker",
                category="swing",
                params={
                    "min_score_threshold": 75.0,
                    "volume_spike_multiplier": 1.8,
                    "max_ma20_distance_pct": 3.0,
                    "max_ma5_distance_pct": 2.0,
                    "market_score_floor": 50.0,
                    "preferred_setup_type": "",
                    "pullback_min_score_threshold": 70.0,
                    "pullback_volume_spike_multiplier": 1.4,
                    "pullback_max_ma20_distance_pct": 3.5,
                    "pullback_max_ma5_distance_pct": 1.8,
                    "pullback_market_score_floor": 48.0,
                    "breakout_min_score_threshold": 74.0,
                    "breakout_volume_spike_multiplier": 1.8,
                    "breakout_max_ma20_distance_pct": 3.0,
                    "breakout_max_ma5_distance_pct": 2.2,
                    "breakout_market_score_floor": 55.0,
                    "trend_follow_min_score_threshold": 72.0,
                    "trend_follow_volume_spike_multiplier": 1.2,
                    "trend_follow_max_ma20_distance_pct": 12.0,
                    "trend_follow_max_ma5_distance_pct": 3.5,
                    "trend_follow_market_score_floor": 48.0,
                },
            ),
            StrategyDefinition(
                strategy_id="ma_golden_cross",
                name="均线金叉",
                description="聚焦 5 日线上穿 20 日线、60 日线向上的启动段。",
                skill_id="ma_golden_cross",
                category="trend",
                params={
                    "min_score_threshold": 70.0,
                    "volume_spike_multiplier": 1.6,
                    "max_ma20_distance_pct": 3.5,
                    "max_ma5_distance_pct": 2.2,
                    "market_score_floor": 50.0,
                    "preferred_setup_type": "breakout",
                },
            ),
            StrategyDefinition(
                strategy_id="shrink_pullback",
                name="缩量回踩",
                description="聚焦 MA5/10/20/30 多头排列中的缩量回踩与再启动。",
                skill_id="shrink_pullback",
                category="trend",
                params={
                    "min_score_threshold": 68.0,
                    "volume_spike_multiplier": 1.5,
                    "max_ma20_distance_pct": 4.0,
                    "max_ma5_distance_pct": 2.0,
                    "market_score_floor": 48.0,
                    "preferred_setup_type": "pullback",
                },
            ),
            StrategyDefinition(
                strategy_id="volume_breakout",
                name="放量突破",
                description="聚焦近端平台突破、倍量启动与板块联动的强势票。",
                skill_id="volume_breakout",
                category="breakout",
                params={
                    "min_score_threshold": 72.0,
                    "volume_spike_multiplier": 2.0,
                    "max_ma20_distance_pct": 5.0,
                    "max_ma5_distance_pct": 2.5,
                    "market_score_floor": 52.0,
                    "preferred_setup_type": "breakout",
                },
            ),
        ]
        return [
            {
                "strategy_id": item.strategy_id,
                "name": item.name,
                "description": item.description,
                "skill_id": item.skill_id,
                "category": item.category,
                "params": dict(item.params),
            }
            for item in strategies
        ]

    def run_scan(
        self,
        *,
        strategy_id: Optional[str] = None,
        scan_date: Optional[date] = None,
        max_candidates: Optional[int] = None,
        strategy_params: Optional[Dict[str, Any]] = None,
        send_notification: bool = True,
        force_refresh: bool = False,
        recompute_market_snapshot: bool = False,
    ) -> Dict[str, Any]:
        if not getattr(self.config, "stock_picker_enabled", True):
            raise ValueError("stock picker is disabled")

        strategy = self._resolve_strategy(strategy_id)
        optimized = self._get_latest_optimization(strategy["strategy_id"])
        strategy_params_override = self._normalize_strategy_params(strategy_params)
        effective_scan_date = scan_date or self._resolve_scan_trade_date()
        max_selected = min(
            max_candidates or getattr(self.config, "stock_picker_max_candidates", _MAX_SELECTED_CANDIDATES),
            _MAX_SELECTED_CANDIDATES,
        )
        llm_review_limit = min(
            getattr(self.config, "stock_picker_llm_review_limit", _MAX_REPORT_CANDIDATES),
            _MAX_REPORT_CANDIDATES,
            max_selected,
        )

        existing_run_id = self._find_latest_active_run_for_date(
            strategy_id=strategy["strategy_id"],
            scan_date=effective_scan_date,
        )
        if existing_run_id is not None and not force_refresh:
            payload = self.get_run_detail(existing_run_id)
            if payload is not None:
                return payload

        query_id = uuid.uuid4().hex
        run_id = self._save_scan_run(
            query_id=query_id,
            strategy=strategy,
            scan_date=effective_scan_date,
            total_scanned=0,
            matched_count=0,
            selected=[],
            market_snapshot={},
            us_snapshot={},
            optimization=optimized,
            status="queued",
        )
        self._enqueue_scan_execution(
            run_id=run_id,
            query_id=query_id,
            strategy=strategy,
            scan_date=effective_scan_date,
            max_selected=max_selected,
            llm_review_limit=llm_review_limit,
            send_notification=send_notification,
            force_refresh=force_refresh,
            recompute_market_snapshot=recompute_market_snapshot,
            optimized=optimized,
            strategy_params_override=strategy_params_override,
        )

        result = self.get_run_detail(run_id)
        if result is None:
            raise RuntimeError("scan run saved but detail lookup failed")
        return result

    def _execute_scan(
        self,
        *,
        strategy: Dict[str, Any],
        scan_date: date,
        max_selected: int,
        llm_review_limit: int,
        force_refresh: bool,
        recompute_market_snapshot: bool,
        optimized: Optional[Dict[str, Any]],
        strategy_params_override: Dict[str, Any],
    ) -> Dict[str, Any]:
        strategy_params = dict(strategy["params"])
        if optimized and isinstance(optimized.get("params"), dict):
            strategy_params.update(optimized["params"])
        if strategy_params_override:
            strategy_params.update(strategy_params_override)
        ranking_only_mode = strategy["strategy_id"] in CORE_SCHEDULED_STRATEGIES

        self._ensure_recent_market_data(scan_date, lookback_trading_days=90, force_refresh=force_refresh)
        universe = self._load_mainboard_universe()
        after_close = scan_date == date.today() and datetime.now().time() >= dt_time(15, 0)
        snapshot_use_cache = not (recompute_market_snapshot or after_close)
        market_snapshot = self._build_market_snapshot(use_cache=snapshot_use_cache)
        us_snapshot = self._build_us_market_snapshot()
        sector_snapshot = self._build_sector_snapshot(use_cache=snapshot_use_cache)
        history_frames = self._load_history_frames([item["code"] for item in universe], scan_date)

        candidates: List[Dict[str, Any]] = []
        total_scanned = 0
        for item in universe:
            code = item["code"]
            df = history_frames.get(code)
            if df is None or df.empty:
                continue
            total_scanned += 1
            signal = self._evaluate_strategy(
                strategy["strategy_id"],
                code=code,
                name=item["name"],
                bars=df,
                market_snapshot=market_snapshot,
                params=strategy_params,
            )
            if not signal.passed and not ranking_only_mode:
                continue
            metrics = dict(signal.metrics or {})
            metrics["strategy_rule_passed"] = bool(signal.passed)
            analysis_summary = signal.analysis_summary
            if ranking_only_mode and not signal.passed:
                analysis_summary = (
                    f"{item['name']} 未完全触发硬条件，但在 {strategy['name']} 全市场评分排序中靠前，"
                    "适合作为次日重点观察候选。"
                )
            candidates.append(
                {
                    "code": code,
                    "name": item["name"],
                    "scan_date": scan_date.isoformat(),
                    "strategy_id": strategy["strategy_id"],
                    "setup_type": signal.setup_type,
                    "score": round(signal.score, 2),
                    "operation_advice": signal.operation_advice if signal.passed else "观察",
                    "analysis_summary": analysis_summary,
                    "reasons": signal.reasons,
                    "stop_loss": signal.stop_loss,
                    "take_profit": signal.take_profit,
                    "metrics": metrics,
                    "news_context": None,
                    "market_context": {
                        "cn": market_snapshot,
                        "us": us_snapshot,
                    },
                    "action_plan_markdown": "",
                    "llm_model": None,
                }
            )

        if ranking_only_mode and len(candidates) > _RANKING_PREFILTER_CANDIDATES:
            candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
            candidates = candidates[:_RANKING_PREFILTER_CANDIDATES]

        self._attach_candidate_board_contexts(candidates)

        sector_snapshot = self._augment_sector_snapshot_with_candidates(sector_snapshot, candidates)
        self._apply_theme_strength(
            strategy_id=strategy["strategy_id"],
            candidates=candidates,
            sector_snapshot=sector_snapshot,
            market_snapshot=market_snapshot,
        )
        final_candidates: List[Dict[str, Any]] = []
        market_score_value = float(market_snapshot.get("score", 50.0))
        for candidate in candidates:
            setup_type = str(candidate.get("setup_type") or "")
            effective_min_score = self._resolve_effective_min_score(
                strategy_id=strategy["strategy_id"],
                setup_type=setup_type,
                params=strategy_params,
            )
            blended_score = round(float(candidate.get("score") or 0.0) * 0.86 + market_score_value * 0.14, 2)
            metrics = candidate.setdefault("metrics", {})
            metrics["effective_min_score_threshold"] = effective_min_score
            metrics["theme_final_score"] = blended_score
            metrics["ranking_only_mode"] = ranking_only_mode
            candidate["score"] = blended_score
            if ranking_only_mode or blended_score >= effective_min_score:
                final_candidates.append(candidate)

        final_candidates.sort(key=lambda item: item["score"], reverse=True)
        matched_count = len(final_candidates)
        selected = final_candidates[:max_selected]

        for rank, candidate in enumerate(selected, start=1):
            candidate["rank"] = rank
        for idx, candidate in enumerate(selected):
            if idx < llm_review_limit:
                candidate["action_plan_markdown"] = "增强链路正在生成最终版操作手册，完成后会自动更新并推送到飞书。"
            else:
                candidate["action_plan_markdown"] = "该票保留为量化候选，系统只对前 5 只生成详细操作手册。"
            candidate["llm_model"] = None

        return {
            "total_scanned": total_scanned,
            "matched_count": matched_count,
            "selected": selected,
            "market_snapshot": market_snapshot,
            "us_snapshot": us_snapshot,
            "optimization": optimized,
        }

    def _enqueue_scan_execution(
        self,
        *,
        run_id: int,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        max_selected: int,
        llm_review_limit: int,
        send_notification: bool,
        force_refresh: bool,
        recompute_market_snapshot: bool,
        optimized: Optional[Dict[str, Any]],
        strategy_params_override: Dict[str, Any],
    ) -> None:
        self._scan_executor.submit(
            type(self)._run_scan_worker,
            run_id=run_id,
            query_id=query_id,
            strategy=dict(strategy),
            scan_date=scan_date,
            max_selected=max_selected,
            llm_review_limit=llm_review_limit,
            send_notification=send_notification,
            force_refresh=force_refresh,
            recompute_market_snapshot=recompute_market_snapshot,
            optimized=dict(optimized) if isinstance(optimized, dict) else None,
            strategy_params_override=dict(strategy_params_override),
        )

    @classmethod
    def _run_scan_worker(
        cls,
        *,
        run_id: int,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        max_selected: int,
        llm_review_limit: int,
        send_notification: bool,
        force_refresh: bool,
        recompute_market_snapshot: bool,
        optimized: Optional[Dict[str, Any]],
        strategy_params_override: Dict[str, Any],
    ) -> None:
        service = cls(config=get_config())
        service._update_run_status(run_id=run_id, status="running", completed=False)
        try:
            payload = service._execute_scan(
                strategy=strategy,
                scan_date=scan_date,
                max_selected=max_selected,
                llm_review_limit=llm_review_limit,
                force_refresh=force_refresh,
                recompute_market_snapshot=recompute_market_snapshot,
                optimized=optimized,
                strategy_params_override=strategy_params_override,
            )
            service._replace_scan_run_payload(
                run_id=run_id,
                total_scanned=int(payload.get("total_scanned") or 0),
                matched_count=int(payload.get("matched_count") or 0),
                selected=payload.get("selected") or [],
                market_snapshot=payload.get("market_snapshot") or {},
                us_snapshot=payload.get("us_snapshot") or {},
                optimization=payload.get("optimization"),
            )
            if not payload.get("selected"):
                service._update_run_status(run_id=run_id, status="completed", completed=True)
                if send_notification:
                    detail = service.get_run_detail(run_id)
                    if detail is not None:
                        detail["notification_stage"] = "scan_completed"
                        service._send_scan_notification(detail)
                return
            service._enqueue_run_enrichment(
                run_id=run_id,
                query_id=query_id,
                strategy=strategy,
                scan_date=scan_date,
                llm_review_limit=llm_review_limit,
                market_snapshot=payload.get("market_snapshot") or {},
                us_snapshot=payload.get("us_snapshot") or {},
                send_notification=send_notification,
            )
        except Exception as exc:
            logger.exception("Stock picker scan worker failed for run %s: %s", run_id, exc)
            service._mark_run_failed(run_id=run_id, reason=str(exc))

    @staticmethod
    def _normalize_strategy_params(raw_params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        if not isinstance(raw_params, dict):
            return normalized
        for key, value in raw_params.items():
            if value in (None, ""):
                continue
            if isinstance(value, (int, float)):
                normalized[str(key)] = float(value)
                continue
            try:
                normalized[str(key)] = float(str(value).strip())
            except (TypeError, ValueError):
                normalized[str(key)] = value
        return normalized

    def run_scheduled_scan(self) -> Dict[str, Any]:
        strategy_id = "mainboard_swing_master"
        scan_date = self._resolve_scan_trade_date()

        with self._schedule_lock:
            existing_run = self._find_latest_run_for_date(strategy_id=strategy_id, scan_date=scan_date)
            if existing_run is not None:
                self._repair_stuck_run(run_id=existing_run)
                payload = self.get_run_detail(existing_run)
                if payload is not None:
                    return payload

            return self.run_scan(
                strategy_id=strategy_id,
                scan_date=scan_date,
                max_candidates=_MAX_SELECTED_CANDIDATES,
                send_notification=True,
            )

    def list_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        self._repair_stale_runs()
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockSelectionRun)
                .order_by(desc(StockSelectionRun.scan_date), desc(StockSelectionRun.created_at))
                .limit(limit)
            ).scalars().all()
            return [self._run_to_dict(row, with_candidates=False) for row in rows]

    def get_run_detail(self, run_id: int) -> Optional[Dict[str, Any]]:
        self._repair_stale_runs()
        with self.db.get_session() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return None
            candidates = session.execute(
                select(StockSelectionCandidate)
                .where(StockSelectionCandidate.run_id == run.id)
                .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
            ).scalars().all()
            candidate_ids = [item.id for item in candidates]
            backtests_by_candidate: Dict[int, List[StockSelectionBacktest]] = {}
            if candidate_ids:
                rows = session.execute(
                    select(StockSelectionBacktest)
                    .where(StockSelectionBacktest.candidate_id.in_(candidate_ids))
                    .order_by(StockSelectionBacktest.horizon_days.asc())
                ).scalars().all()
                for row in rows:
                    backtests_by_candidate.setdefault(row.candidate_id, []).append(row)

            payload = self._run_to_dict(run, with_candidates=False)
            payload["candidates"] = [
                self._candidate_to_dict(item, backtests_by_candidate.get(item.id, []))
                for item in candidates
            ]
            top_rows = candidates[: min(len(candidates), _MAX_REPORT_CANDIDATES)]
            if (
                not self.lightweight
                and
                run.status == "completed"
                and top_rows
                and any(self._is_placeholder_action_plan(row.action_plan_markdown) for row in top_rows)
            ):
                try:
                    strategy = self._resolve_strategy(run.strategy_id)
                    self._enqueue_run_enrichment(
                        run_id=int(run.id),
                        query_id=str(run.query_id or f"repair_{run.id}"),
                        strategy=strategy,
                        scan_date=run.scan_date,
                        llm_review_limit=min(len(top_rows), _MAX_REPORT_CANDIDATES),
                        market_snapshot=payload.get("market_snapshot") or {},
                        us_snapshot=payload.get("us_market_snapshot") or {},
                        send_notification=False,
                    )
                except Exception as exc:
                    logger.warning("Failed to auto-repair placeholder action plan for run %s: %s", run.id, exc)
            return payload

    def _enqueue_run_enrichment(
        self,
        *,
        run_id: int,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        llm_review_limit: int,
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        send_notification: bool,
    ) -> None:
        if not self._acquire_enrichment_lock(run_id):
            logger.info("Stock picker enrichment already active for run %s, skipping duplicate enqueue", run_id)
            return

        try:
            self._background_executor.submit(
                type(self)._run_enrichment_worker,
                run_id,
                query_id,
                dict(strategy),
                scan_date,
                llm_review_limit,
                dict(market_snapshot),
                dict(us_snapshot),
                send_notification,
            )
        except Exception:
            self._release_enrichment_lock(run_id)
            raise

    @classmethod
    def _run_enrichment_worker(
        cls,
        run_id: int,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        llm_review_limit: int,
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        send_notification: bool,
    ) -> None:
        service = cls(config=get_config())
        service._update_run_status(run_id=run_id, status="enriching", completed=False)
        try:
            with service.db.get_session() as session:
                candidate_rows = session.execute(
                    select(StockSelectionCandidate)
                    .where(StockSelectionCandidate.run_id == run_id)
                    .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
                ).scalars().all()

            candidate_records: List[Dict[str, Any]] = []
            for idx, row in enumerate(candidate_rows):
                candidate_records.append(
                    {
                        "id": int(row.id),
                        "rank": idx + 1,
                        "code": row.code,
                        "name": row.name,
                        "strategy_id": row.strategy_id,
                        "scan_date": row.scan_date.isoformat() if row.scan_date else scan_date.isoformat(),
                        "setup_type": row.setup_type,
                        "score": float(row.score or 0.0),
                        "operation_advice": row.operation_advice,
                        "analysis_summary": row.analysis_summary,
                        "reasons": service._safe_json_loads(row.reason_json) or [],
                        "stop_loss": row.stop_loss,
                        "take_profit": row.take_profit,
                        "metrics": service._safe_json_loads(row.indicator_snapshot_json) or {},
                        "market_context": service._safe_json_loads(row.market_context_json) or {},
                    }
                )

            openai_circuit_open = False
            failed_codes: List[str] = []

            def should_trip_openai_circuit(exc: Exception) -> bool:
                text = str(exc).lower()
                markers = (
                    "service temporarily unavailable",
                    "upstream authentication failed",
                    "key expired",
                    "upstream error",
                    "error code: 502",
                    "error code: 503",
                    "timed out",
                )
                return any(marker in text for marker in markers)

            def enrich_candidate(candidate_payload: Dict[str, Any]) -> Dict[str, Any]:
                nonlocal openai_circuit_open
                review_payload: Dict[str, Any] = {"news_context": None, "news_score": 50.0, "from_cache": False}
                try:
                    review_payload = service._collect_candidate_intel(
                        code=str(candidate_payload.get("code") or ""),
                        name=str(candidate_payload.get("name") or candidate_payload.get("code") or ""),
                        scan_date=scan_date,
                        query_id=query_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "Stock picker intel enrichment failed for run %s %s: %s",
                        run_id,
                        candidate_payload.get("code"),
                        exc,
                    )

                try:
                    action_plan, llm_model = service._build_action_plan(
                        candidate=dict(candidate_payload),
                        strategy=strategy,
                        market_snapshot=market_snapshot,
                        us_snapshot=us_snapshot,
                        review_payload=review_payload,
                        prefer_fallback_only=openai_circuit_open,
                    )
                except Exception as exc:
                    if should_trip_openai_circuit(exc):
                        openai_circuit_open = True
                    logger.warning(
                        "Stock picker action plan retrying without news for run %s %s: %s",
                        run_id,
                        candidate_payload.get("code"),
                        exc,
                    )
                    action_plan, llm_model = service._build_action_plan(
                        candidate=dict(candidate_payload),
                        strategy=strategy,
                        market_snapshot=market_snapshot,
                        us_snapshot=us_snapshot,
                        review_payload={"news_context": None, "news_score": 50.0, "from_cache": False},
                        prefer_fallback_only=openai_circuit_open,
                    )

                return {
                    "id": int(candidate_payload["id"]),
                    "news_context_json": json.dumps(review_payload.get("news_context"), ensure_ascii=False)
                    if review_payload.get("news_context") is not None
                    else None,
                    "action_plan_markdown": action_plan,
                    "llm_model": llm_model,
                    "enrichment_error": None,
                }

            enriched_updates: Dict[int, Dict[str, Any]] = {}
            report_candidates = candidate_records[: max(0, llm_review_limit)]
            if report_candidates:
                primary_model = str(getattr(service.config, "litellm_model", "") or "")
                # The GPT-5.4 gateway is unstable under concurrent streamed /responses calls.
                # Keep picker handbook generation serial on OpenAI-compatible primaries and
                # rely on per-candidate DeepSeek fallback instead of failing the whole run.
                if primary_model.startswith("openai/"):
                    worker_count = 1
                else:
                    worker_count = min(max(1, len(report_candidates)), 3)
                with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="picker_enrich") as executor:
                    futures = {
                        executor.submit(enrich_candidate, candidate_payload): candidate_payload
                        for candidate_payload in report_candidates
                    }
                    for future in as_completed(futures):
                        candidate_payload = futures[future]
                        try:
                            update = future.result()
                        except Exception as exc:
                            code = str(candidate_payload.get("code") or "")
                            logger.warning(
                                "Stock picker action plan enrichment failed for run %s %s: %s",
                                run_id,
                                code,
                                exc,
                            )
                            failed_codes.append(code)
                            enriched_updates[int(candidate_payload["id"])] = {
                                "id": int(candidate_payload["id"]),
                                "news_context_json": None,
                                "action_plan_markdown": f"增强版操作手册暂未生成成功，原因：{exc}",
                                "llm_model": None,
                                "enrichment_error": str(exc),
                            }
                            continue
                        enriched_updates[int(update["id"])] = update

            with service.db.session_scope() as session:
                rows = session.execute(
                    select(StockSelectionCandidate)
                    .where(StockSelectionCandidate.run_id == run_id)
                    .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
                ).scalars().all()
                for idx, row in enumerate(rows):
                    if idx >= llm_review_limit:
                        row.action_plan_markdown = row.action_plan_markdown or "该票保留为量化候选，系统只对前 5 只生成详细操作手册。"
                        continue
                    update = enriched_updates.get(int(row.id))
                    if update is None:
                        failed_codes.append(str(row.code))
                        row.action_plan_markdown = row.action_plan_markdown or "增强版操作手册暂未生成成功，请稍后重试。"
                        row.llm_model = row.llm_model or None
                        continue
                    row.news_context_json = update.get("news_context_json")
                    row.action_plan_markdown = str(update.get("action_plan_markdown") or "").strip()
                    row.llm_model = update.get("llm_model")

            service._update_run_status(run_id=run_id, status="completed", completed=True)
            payload = service.get_run_detail(run_id)
            if payload is not None:
                payload["enrichment_errors"] = failed_codes
                if send_notification:
                    payload["notification_stage"] = "enhanced"
                    service._send_scan_notification(payload)

            backtest_stats = service.backfill_backtests(strategy_id=strategy["strategy_id"])
            optimization = service.optimize_strategy(
                strategy_id=strategy["strategy_id"],
                selected_horizon_days=5,
            )
            service._update_run_optimization(run_id=run_id, optimization=optimization)

            payload = service.get_run_detail(run_id)
            if payload is not None:
                payload["backtest_stats"] = backtest_stats
                payload["optimization"] = optimization
                payload["enrichment_errors"] = failed_codes
        except Exception as exc:
            logger.exception("Stock picker background enrichment failed for run %s: %s", run_id, exc)
            service._update_run_status(run_id=run_id, status="completed", completed=True)
        finally:
            service._release_enrichment_lock(run_id)

    @staticmethod
    def _is_placeholder_action_plan(value: Optional[str]) -> bool:
        text = (value or "").strip()
        if not text:
            return True
        if "后台正在生成次日操作手册" in text:
            return True
        failure_markers = (
            "增强版操作手册暂未生成成功",
            "LLM generation failed",
            "Cannot run the event loop while another loop is running",
            "All LLM models failed",
        )
        if any(marker in text for marker in failure_markers):
            return True
        malformed_markers = (
            "收盘 -",
            "MA5/10/20/30/60=-/-/-/-/-",
            "低吸挂单价：0.01",
            "突破跟单价：0.01",
            "止损价：0.01",
        )
        return any(marker in text for marker in malformed_markers)

    def _repair_stuck_run(self, *, run_id: int) -> None:
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None or run.status not in ("queued", "running", "enriching"):
                return

            candidate_rows = session.execute(
                select(StockSelectionCandidate)
                .where(StockSelectionCandidate.run_id == run_id)
                .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
            ).scalars().all()

            top_rows = candidate_rows[:_MAX_REPORT_CANDIDATES]
            if top_rows and any(self._is_placeholder_action_plan(row.action_plan_markdown) for row in top_rows):
                run.status = "failed"
                run.completed_at = datetime.now()
                run.summary_markdown = "扫描进程已中断：后台服务重启或旧任务超时，请重新发起选股。"
            elif not candidate_rows and run.created_at <= datetime.now() - timedelta(minutes=10):
                summary = str(run.summary_markdown or "")
                if "今日没有符合条件的标的" in summary or "候选股" in summary:
                    run.status = "completed"
                    run.completed_at = datetime.now()
                    return
                run.status = "failed"
                run.completed_at = datetime.now()
                run.summary_markdown = "扫描进程已中断：任务超过 10 分钟仍未写入候选，请重新发起选股。"

    def _repair_stale_runs(self, *, older_than_minutes: int = 10) -> None:
        cutoff = datetime.now() - timedelta(minutes=max(1, older_than_minutes))
        with self.db.get_session() as session:
            run_ids = session.execute(
                select(StockSelectionRun.id)
                .where(
                    and_(
                        StockSelectionRun.status.in_(("queued", "running", "enriching")),
                        StockSelectionRun.created_at <= cutoff,
                    )
                )
                .order_by(StockSelectionRun.created_at.asc())
                .limit(5)
            ).scalars().all()
        for run_id in run_ids:
            try:
                self._repair_stuck_run(run_id=int(run_id))
            except Exception as exc:
                logger.warning("Failed to repair stale stock picker run %s: %s", run_id, exc)

    def _find_latest_run_for_date(self, *, strategy_id: str, scan_date: date) -> Optional[int]:
        with self.db.get_session() as session:
            row = session.execute(
                select(StockSelectionRun.id)
                .where(
                    and_(
                        StockSelectionRun.strategy_id == strategy_id,
                        StockSelectionRun.scan_date == scan_date,
                        StockSelectionRun.status.in_(("queued", "running", "enriching", "completed")),
                    )
                )
                .order_by(desc(StockSelectionRun.created_at))
                .limit(1)
            ).scalars().first()
        return int(row) if row is not None else None

    def _find_latest_active_run_for_date(self, *, strategy_id: str, scan_date: date) -> Optional[int]:
        with self.db.get_session() as session:
            row = session.execute(
                select(StockSelectionRun.id)
                .where(
                    and_(
                        StockSelectionRun.strategy_id == strategy_id,
                        StockSelectionRun.scan_date == scan_date,
                        StockSelectionRun.status.in_(("queued", "running", "enriching")),
                    )
                )
                .order_by(desc(StockSelectionRun.created_at))
                .limit(1)
            ).scalars().first()
        return int(row) if row is not None else None

    def _update_run_status(self, *, run_id: int, status: str, completed: bool) -> None:
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return
            run.status = status
            if completed:
                run.completed_at = datetime.now()
            else:
                run.completed_at = None

    def _mark_run_failed(self, *, run_id: int, reason: str) -> None:
        message = (reason or "未知错误").strip()
        summary = f"扫描失败：{message}"
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return
            run.status = "failed"
            run.completed_at = datetime.now()
            run.summary_markdown = summary[:4000]

    def _replace_scan_run_payload(
        self,
        *,
        run_id: int,
        total_scanned: int,
        matched_count: int,
        selected: Sequence[Dict[str, Any]],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        optimization: Optional[Dict[str, Any]],
    ) -> None:
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return

            existing_rows = session.execute(
                select(StockSelectionCandidate).where(StockSelectionCandidate.run_id == run_id)
            ).scalars().all()
            for row in existing_rows:
                session.delete(row)
            session.flush()

            run.status = "queued"
            run.total_scanned = int(total_scanned or 0)
            run.matched_count = int(matched_count or 0)
            run.selected_count = len(selected)
            run.market_snapshot_json = json.dumps(market_snapshot or {}, ensure_ascii=False)
            run.us_market_snapshot_json = json.dumps(us_snapshot or {}, ensure_ascii=False)
            run.optimization_snapshot_json = json.dumps(optimization or {}, ensure_ascii=False)
            run.summary_markdown = self._build_run_summary_markdown(
                strategy={
                    "strategy_id": run.strategy_id,
                    "name": run.strategy_name,
                },
                scan_date=run.scan_date,
                selected=selected,
                market_snapshot=market_snapshot,
                us_snapshot=us_snapshot,
            )
            run.completed_at = None

            for candidate in selected:
                session.add(
                    StockSelectionCandidate(
                        run_id=run.id,
                        code=str(candidate.get("code") or "").strip(),
                        name=str(candidate.get("name") or "").strip(),
                        strategy_id=str(candidate.get("strategy_id") or run.strategy_id),
                        scan_date=run.scan_date,
                        setup_type=str(candidate.get("setup_type") or "mixed"),
                        score=float(candidate.get("score") or 0.0),
                        rank=int(candidate.get("rank") or 0),
                        selected=True,
                        operation_advice=str(candidate.get("operation_advice") or ""),
                        analysis_summary=candidate.get("analysis_summary"),
                        action_plan_markdown=candidate.get("action_plan_markdown"),
                        reason_json=json.dumps(candidate.get("reasons") or [], ensure_ascii=False),
                        indicator_snapshot_json=json.dumps(candidate.get("metrics") or {}, ensure_ascii=False),
                        market_context_json=json.dumps(candidate.get("market_context") or {}, ensure_ascii=False),
                        news_context_json=json.dumps(candidate.get("news_context"), ensure_ascii=False)
                        if candidate.get("news_context") is not None
                        else None,
                        llm_model=candidate.get("llm_model"),
                        stop_loss=self._to_float(candidate.get("stop_loss")) if candidate.get("stop_loss") not in (None, "") else None,
                        take_profit=self._to_float(candidate.get("take_profit")) if candidate.get("take_profit") not in (None, "") else None,
                        created_at=datetime.now(),
                    )
                )

    @staticmethod
    def _enrichment_lock_path(run_id: int) -> Path:
        _ENRICHMENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        return _ENRICHMENT_LOG_DIR / f"run_{int(run_id)}.lock"

    @classmethod
    def _acquire_enrichment_lock(cls, run_id: int, *, stale_after_minutes: int = 20) -> bool:
        lock_path = cls._enrichment_lock_path(run_id)
        if lock_path.exists():
            try:
                age = datetime.now() - datetime.fromtimestamp(lock_path.stat().st_mtime)
                if age < timedelta(minutes=max(1, stale_after_minutes)):
                    return False
                lock_path.unlink(missing_ok=True)
            except Exception:
                return False

        payload = {
            "run_id": int(run_id),
            "pid": os.getpid(),
            "created_at": datetime.now().isoformat(),
        }
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return True

    @classmethod
    def _release_enrichment_lock(cls, run_id: int) -> None:
        try:
            cls._enrichment_lock_path(run_id).unlink(missing_ok=True)
        except Exception:
            pass

    def backfill_backtests(
        self,
        *,
        strategy_id: Optional[str] = None,
        max_candidates: int = 200,
    ) -> Dict[str, Any]:
        processed = 0
        completed = 0
        pending = 0

        with self.db.get_session() as session:
            conditions = []
            if strategy_id:
                conditions.append(StockSelectionCandidate.strategy_id == strategy_id)

            query = select(StockSelectionCandidate)
            if conditions:
                query = query.where(and_(*conditions))
            query = query.order_by(desc(StockSelectionCandidate.scan_date), StockSelectionCandidate.rank.asc()).limit(max_candidates)
            candidates = session.execute(query).scalars().all()
            candidate_ids = [int(candidate.id) for candidate in candidates]

            existing_rows: Dict[tuple[int, int], StockSelectionBacktest] = {}
            if candidate_ids:
                rows = session.execute(
                    select(StockSelectionBacktest).where(StockSelectionBacktest.candidate_id.in_(candidate_ids))
                ).scalars().all()
                existing_rows = {
                    (int(row.candidate_id), int(row.horizon_days)): row
                    for row in rows
                }

            to_insert: List[StockSelectionBacktest] = []
            for candidate in candidates:
                bars = self._load_forward_bars(candidate.code, candidate.scan_date, max(BACKTEST_HORIZONS) + 1)

                for horizon in BACKTEST_HORIZONS:
                    existing = existing_rows.get((int(candidate.id), int(horizon)))
                    if existing is not None and str(existing.status or "").lower() == "completed":
                        continue
                    processed += 1
                    record = self._evaluate_candidate_horizon(candidate, bars, horizon)
                    if record["status"] == "completed":
                        completed += 1
                    else:
                        pending += 1
                    if existing is not None:
                        existing.status = record["status"]
                        existing.entry_date = record.get("entry_date")
                        existing.exit_date = record.get("exit_date")
                        existing.entry_price = record.get("entry_price")
                        existing.exit_price = record.get("exit_price")
                        existing.end_close = record.get("end_close")
                        existing.max_high = record.get("max_high")
                        existing.min_low = record.get("min_low")
                        existing.return_pct = record.get("return_pct")
                        existing.max_drawdown_pct = record.get("max_drawdown_pct")
                        existing.outcome = record.get("outcome")
                        existing.evaluated_at = datetime.now()
                        continue

                    to_insert.append(
                        StockSelectionBacktest(
                            candidate_id=candidate.id,
                            strategy_id=candidate.strategy_id,
                            code=candidate.code,
                            scan_date=candidate.scan_date,
                            horizon_days=horizon,
                            status=record["status"],
                            entry_date=record.get("entry_date"),
                            exit_date=record.get("exit_date"),
                            entry_price=record.get("entry_price"),
                            exit_price=record.get("exit_price"),
                            end_close=record.get("end_close"),
                            max_high=record.get("max_high"),
                            min_low=record.get("min_low"),
                            return_pct=record.get("return_pct"),
                            max_drawdown_pct=record.get("max_drawdown_pct"),
                            outcome=record.get("outcome"),
                            created_at=datetime.now(),
                            evaluated_at=datetime.now(),
                        )
                    )

            if to_insert:
                session.add_all(to_insert)
            if to_insert or processed:
                session.commit()

        return {
            "processed": processed,
            "completed": completed,
            "pending": pending,
        }

    def optimize_strategy(
        self,
        *,
        strategy_id: str,
        lookback_days: int = 90,
        selected_horizon_days: Optional[int] = 5,
    ) -> Dict[str, Any]:
        strategy = self._resolve_strategy(strategy_id)
        cutoff = date.today() - timedelta(days=lookback_days)
        candidate_rows: List[Dict[str, Any]] = []
        horizons_to_try = [int(selected_horizon_days)] if selected_horizon_days else [3, 5, 10]
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockSelectionCandidate, StockSelectionBacktest)
                .join(
                    StockSelectionBacktest,
                    StockSelectionBacktest.candidate_id == StockSelectionCandidate.id,
                )
                .where(
                    and_(
                        StockSelectionCandidate.strategy_id == strategy_id,
                        StockSelectionCandidate.scan_date >= cutoff,
                        StockSelectionCandidate.selected.is_(True),
                        StockSelectionCandidate.rank <= 5,
                        StockSelectionBacktest.horizon_days.in_(horizons_to_try),
                        StockSelectionBacktest.status == "completed",
                    )
                )
            ).all()

            for candidate, backtest in rows:
                metrics = self._safe_json_loads(candidate.indicator_snapshot_json)
                if not isinstance(metrics, dict):
                    metrics = {}
                market_context = self._safe_json_loads(candidate.market_context_json)
                market_score = 50.0
                if isinstance(market_context, dict):
                    cn_ctx = market_context.get("cn")
                    if isinstance(cn_ctx, dict):
                        market_score = float(cn_ctx.get("score") or market_score)
                candidate_rows.append(
                    {
                        "score": float(candidate.score or 0.0),
                        "distance_to_ma20_pct": float(metrics.get("distance_to_ma20_pct") or 999.0),
                        "distance_to_ma5_pct": float(metrics.get("distance_to_ma5_pct") or 999.0),
                        "volume_spike_factor": float(metrics.get("volume_spike_factor") or 0.0),
                        "market_score": float(metrics.get("market_score") or market_score),
                        "setup_type": str(candidate.setup_type or "mixed"),
                        "horizon_days": int(backtest.horizon_days or 0),
                        "max_drawdown_pct": float(backtest.max_drawdown_pct or 0.0),
                        "return_pct": float(backtest.return_pct or 0.0),
                    }
                )

            if not candidate_rows:
                payload = {
                    "strategy_id": strategy_id,
                    "strategy_name": strategy["name"],
                    "lookback_days": lookback_days,
                    "selected_horizon_days": int(selected_horizon_days or 5),
                    "status": "insufficient_data",
                    "params": dict(strategy["params"]),
                    "metrics": {"sample_count": 0},
                }
                session.add(
                    StockSelectionOptimization(
                        strategy_id=strategy_id,
                        strategy_name=strategy["name"],
                        lookback_days=lookback_days,
                        selected_horizon_days=int(selected_horizon_days or 5),
                        metrics_json=json.dumps(payload["metrics"], ensure_ascii=False),
                        params_json=json.dumps(payload["params"], ensure_ascii=False),
                        status="insufficient_data",
                        created_at=datetime.now(),
                    )
                )
                session.commit()
                return payload

            best_score = -10**9
            best_payload = None
            setup_options = ("", "pullback", "breakout")
            if strategy_id == "dragon_head":
                setup_options = ("breakout", "")
            elif strategy_id in {"bull_trend", "shrink_pullback"}:
                setup_options = ("pullback", "")
            for horizon in horizons_to_try:
                horizon_rows = [row for row in candidate_rows if row["horizon_days"] == horizon]
                if len(horizon_rows) < 6:
                    continue
                for preferred_setup_type in setup_options:
                    for min_score_threshold in (66.0, 70.0, 72.0, 75.0, 78.0):
                        for max_ma20_distance_pct in (2.5, 3.0, 3.5, 4.0, 4.5):
                            for max_ma5_distance_pct in (1.5, 2.0, 2.5, 3.0):
                                for volume_spike_multiplier in (1.4, 1.6, 1.8, 2.0):
                                    for market_score_floor in (45.0, 50.0, 55.0):
                                        filtered = []
                                        market_penalties = []
                                        for row in horizon_rows:
                                            if row["score"] < min_score_threshold:
                                                continue
                                            if row["distance_to_ma20_pct"] > max_ma20_distance_pct:
                                                continue
                                            if row["volume_spike_factor"] < volume_spike_multiplier:
                                                continue
                                            if preferred_setup_type and row["setup_type"] != preferred_setup_type:
                                                continue
                                            if row["setup_type"] == "pullback" and row["distance_to_ma5_pct"] > max_ma5_distance_pct:
                                                continue
                                            filtered.append(row)
                                            row_market_score = float(row["market_score"] or 0.0)
                                            market_penalties.append(
                                                max(0.0, (market_score_floor - row_market_score) * 1.1 + 4.0)
                                                if row_market_score < market_score_floor
                                                else 0.0
                                            )

                                        min_samples = 8 if not preferred_setup_type else 6
                                        if len(filtered) < min_samples:
                                            continue

                                        avg_return = sum(row["return_pct"] for row in filtered) / len(filtered)
                                        median_return = sorted(row["return_pct"] for row in filtered)[len(filtered) // 2]
                                        win_rate = sum(1 for row in filtered if row["return_pct"] > 0) / len(filtered)
                                        loss_rate = sum(1 for row in filtered if row["return_pct"] < 0) / len(filtered)
                                        avg_drawdown = sum(row["max_drawdown_pct"] for row in filtered) / len(filtered)
                                        worst_drawdown = min(row["max_drawdown_pct"] for row in filtered)
                                        high_drawdown_rate = sum(
                                            1 for row in filtered if row["max_drawdown_pct"] <= -5.0
                                        ) / len(filtered)
                                        tail_loss_rate = sum(
                                            1 for row in filtered if row["return_pct"] <= -3.0
                                        ) / len(filtered)
                                        avg_market_penalty = sum(market_penalties) / len(market_penalties)
                                        positive_returns = sum(max(row["return_pct"], 0.0) for row in filtered)
                                        negative_returns = abs(sum(min(row["return_pct"], 0.0) for row in filtered))
                                        profit_factor = positive_returns / max(negative_returns, 0.01)
                                        objective = (
                                            avg_return * 1.15
                                            + median_return * 0.55
                                            + win_rate * 8.5
                                            + min(profit_factor, 4.0) * 1.6
                                            - abs(avg_drawdown) * 1.45
                                            - abs(worst_drawdown) * 0.42
                                            - high_drawdown_rate * 9.0
                                            - tail_loss_rate * 8.0
                                            - loss_rate * 2.5
                                            - avg_market_penalty * 0.35
                                            + min(len(filtered), 30) * 0.04
                                        )
                                        if objective > best_score:
                                            best_score = objective
                                            setup_breakdown = {
                                                "pullback": sum(1 for row in filtered if row["setup_type"] == "pullback"),
                                                "breakout": sum(1 for row in filtered if row["setup_type"] == "breakout"),
                                                "mixed": sum(1 for row in filtered if row["setup_type"] not in {"pullback", "breakout"}),
                                            }
                                            best_payload = {
                                                "strategy_id": strategy_id,
                                                "strategy_name": strategy["name"],
                                                "lookback_days": lookback_days,
                                                "selected_horizon_days": horizon,
                                                "status": "completed",
                                                "params": {
                                                    "min_score_threshold": min_score_threshold,
                                                    "max_ma20_distance_pct": max_ma20_distance_pct,
                                                    "max_ma5_distance_pct": max_ma5_distance_pct,
                                                    "volume_spike_multiplier": volume_spike_multiplier,
                                                    "market_score_floor": market_score_floor,
                                                    "preferred_setup_type": preferred_setup_type,
                                                },
                                                "metrics": {
                                                    "sample_count": len(filtered),
                                                    "avg_return_pct": round(avg_return, 2),
                                                    "median_return_pct": round(median_return, 2),
                                                    "win_rate_pct": round(win_rate * 100, 2),
                                                    "loss_rate_pct": round(loss_rate * 100, 2),
                                                    "avg_max_drawdown_pct": round(avg_drawdown, 2),
                                                    "worst_drawdown_pct": round(worst_drawdown, 2),
                                                    "high_drawdown_rate_pct": round(high_drawdown_rate * 100, 2),
                                                    "tail_loss_rate_pct": round(tail_loss_rate * 100, 2),
                                                    "profit_factor": round(profit_factor, 3),
                                                    "avg_market_penalty": round(avg_market_penalty, 2),
                                                    "objective": round(objective, 4),
                                                    "setup_type_distribution": setup_breakdown,
                                                },
                                            }
                                            if strategy_id == "mainboard_swing_master":
                                                dual_params = dict(strategy["params"])
                                                dual_params.update(
                                                    {
                                                        "min_score_threshold": min_score_threshold,
                                                        "max_ma20_distance_pct": max_ma20_distance_pct,
                                                        "max_ma5_distance_pct": max_ma5_distance_pct,
                                                        "volume_spike_multiplier": volume_spike_multiplier,
                                                        "market_score_floor": market_score_floor,
                                                        "preferred_setup_type": preferred_setup_type,
                                                    }
                                                )
                                                if preferred_setup_type == "pullback":
                                                    dual_params.update(
                                                        {
                                                            "pullback_min_score_threshold": min_score_threshold,
                                                            "pullback_max_ma20_distance_pct": max_ma20_distance_pct,
                                                            "pullback_max_ma5_distance_pct": max_ma5_distance_pct,
                                                            "pullback_volume_spike_multiplier": volume_spike_multiplier,
                                                            "pullback_market_score_floor": market_score_floor,
                                                        }
                                                    )
                                                elif preferred_setup_type == "breakout":
                                                    dual_params.update(
                                                        {
                                                            "breakout_min_score_threshold": min_score_threshold,
                                                            "breakout_max_ma20_distance_pct": max_ma20_distance_pct,
                                                            "breakout_max_ma5_distance_pct": max_ma5_distance_pct,
                                                            "breakout_volume_spike_multiplier": volume_spike_multiplier,
                                                            "breakout_market_score_floor": market_score_floor,
                                                        }
                                                    )
                                                best_payload["params"] = dual_params

            if best_payload is None:
                best_payload = {
                    "strategy_id": strategy_id,
                    "strategy_name": strategy["name"],
                    "lookback_days": lookback_days,
                    "selected_horizon_days": int(selected_horizon_days or 5),
                    "status": "insufficient_data",
                    "params": dict(strategy["params"]),
                    "metrics": {"sample_count": len(candidate_rows)},
                }

            if best_payload.get("status") == "completed":
                try:
                    llm_review = self._build_optimization_review(
                        strategy=strategy,
                        lookback_days=lookback_days,
                        candidate_rows=candidate_rows,
                        best_payload=best_payload,
                    )
                    if llm_review:
                        best_payload["llm_review"] = llm_review
                        summary_text = str(llm_review.get("diagnosis_summary") or "").strip()
                        if summary_text:
                            best_payload["metrics"]["llm_summary"] = summary_text[:500]
                except Exception as exc:
                    logger.warning("Stock picker optimization LLM review failed for %s: %s", strategy_id, exc)

            if isinstance(best_payload.get("llm_review"), dict):
                best_payload["metrics"]["llm_review"] = best_payload["llm_review"]

            session.add(
                StockSelectionOptimization(
                    strategy_id=strategy_id,
                    strategy_name=strategy["name"],
                    lookback_days=lookback_days,
                    selected_horizon_days=selected_horizon_days,
                    metrics_json=json.dumps(best_payload["metrics"], ensure_ascii=False),
                    params_json=json.dumps(best_payload["params"], ensure_ascii=False),
                    status=best_payload["status"],
                    created_at=datetime.now(),
                )
            )
            session.commit()

        return best_payload

    def _resolve_strategy(self, strategy_id: Optional[str]) -> Dict[str, Any]:
        target = strategy_id or getattr(self.config, "stock_picker_default_strategy", "mainboard_swing_master")
        if target == "swing_after_close_picker":
            target = "mainboard_swing_master"
        if target == "mainboard_swing_master":
            preset = _PICKER_STRATEGY_PRESETS[target]
            return {
                "strategy_id": target,
                "name": "主力波段双模",
                "description": "旧版兼容策略：收盘后综合突破启动、趋势回踩、量能、消息面与市场情绪。",
                "skill_id": "swing_after_close_picker",
                "category": "swing",
                "params": dict(preset.get("params") or {}),
            }
        for item in self._strategy_catalog():
            if item["strategy_id"] == target:
                return item
        raise ValueError(f"unknown strategy_id: {target}")

    @staticmethod
    def _resolve_effective_min_score(
        *,
        strategy_id: str,
        setup_type: str,
        params: Dict[str, Any],
    ) -> float:
        default_threshold = float(params.get("min_score_threshold", 70.0) or 70.0)
        if strategy_id in {"mainboard_swing_master", "swing_trend_follow"}:
            if setup_type == "pullback":
                return float(params.get("pullback_min_score_threshold", default_threshold) or default_threshold)
            if setup_type == "breakout":
                return float(params.get("breakout_min_score_threshold", default_threshold) or default_threshold)
            if setup_type == "trend_follow":
                return float(params.get("trend_follow_min_score_threshold", default_threshold) or default_threshold)
        if strategy_id == "main_force_breakout":
            return float(params.get("main_force_min_score_threshold", default_threshold) or default_threshold)
        if strategy_id == "shanliu_theme_flow":
            return float(params.get("shanliu_min_score_threshold", default_threshold) or default_threshold)
        return default_threshold

    def _resolve_scan_trade_date(self) -> date:
        from src.core.trading_calendar import get_market_now

        latest_db_date = self._get_latest_daily_date()
        if latest_db_date is not None:
            now = get_market_now("cn")
            if latest_db_date >= now.date() and now.time() < dt_time(15, 0):
                latest_complete = self._get_latest_complete_daily_date(
                    max_date=now.date() - timedelta(days=1),
                    minimum_rows=2500,
                )
                if latest_complete is not None:
                    return latest_complete
            return latest_db_date
        return date.today()

    def _load_mainboard_universe(self) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        tickflow_fetcher = self.fetcher_manager._get_tickflow_fetcher()
        if tickflow_fetcher is not None:
            try:
                instruments = []
                instruments.extend(tickflow_fetcher._get_client().exchanges.get_instruments("SH"))
                instruments.extend(tickflow_fetcher._get_client().exchanges.get_instruments("SZ"))
                for item in instruments:
                    if str(item.get("type") or "").strip().lower() != "stock":
                        continue
                    code = str(item.get("code") or "").strip()
                    name = str(item.get("name") or code).strip()
                    if not code or not self._is_main_board_code(code) or self._is_st_name(name):
                        continue
                    rows.append({"code": code, "name": name})
            except Exception as exc:
                logger.warning("Stock picker failed to load universe from TickFlow exchanges: %s", exc)

        if not rows:
            stock_list = self._load_universe_from_local_db()
            if stock_list is None or stock_list.empty:
                try:
                    stock_list = BaostockFetcher().get_stock_list()
                except Exception as exc:
                    logger.warning("Stock picker failed to load universe from Baostock: %s", exc)
            if stock_list is None or stock_list.empty:
                raise RuntimeError(
                    "unable to load A-share universe from TickFlow exchanges, local database, or Baostock."
                )
            for _, row in stock_list.iterrows():
                code = str(row.get("code") or "").strip()
                name = str(row.get("name") or code).strip()
                if not code or not self._is_main_board_code(code) or self._is_st_name(name):
                    continue
                rows.append({"code": code, "name": name})
        return rows

    @staticmethod
    def _is_main_board_code(code: str) -> bool:
        return code.startswith(("600", "601", "603", "605", "000", "001", "002"))

    @staticmethod
    def _is_st_name(name: str) -> bool:
        normalized = name.replace(" ", "").upper()
        return normalized.startswith("ST") or normalized.startswith("*ST") or "ST" in normalized[:4]

    def _ensure_recent_market_data(
        self,
        scan_date: date,
        *,
        lookback_trading_days: int,
        force_refresh: bool = False,
    ) -> None:
        has_scan_snapshot = self._count_daily_rows(scan_date) >= 2500
        if (
            not force_refresh
            and has_scan_snapshot
            and self._has_sufficient_local_history_window(
                scan_date,
                minimum_codes=2000,
                minimum_rows_per_code=min(lookback_trading_days, 60),
            )
        ):
            logger.info("Stock picker reuses local daily snapshots for %s", scan_date)
            return

        universe = self._load_mainboard_universe()
        if self._refresh_recent_market_data_with_tickflow(
            scan_date=scan_date,
            universe=universe,
            lookback_trading_days=lookback_trading_days,
        ):
            return

        raise RuntimeError(f"TickFlow full-market refresh failed for {scan_date.isoformat()}")

    def _refresh_recent_market_data_with_tickflow(
        self,
        *,
        scan_date: date,
        universe: Sequence[Dict[str, str]],
        lookback_trading_days: int,
    ) -> bool:
        tickflow_fetcher = self.fetcher_manager._get_tickflow_fetcher()
        if tickflow_fetcher is None or not hasattr(tickflow_fetcher, "get_daily_batch"):
            return False

        code_list = [str(item.get("code") or "").strip() for item in universe if str(item.get("code") or "").strip()]
        if not code_list:
            return False

        try:
            batch_frames = tickflow_fetcher.get_daily_batch(
                code_list,
                count=lookback_trading_days,
                end_date=scan_date.strftime("%Y-%m-%d"),
            )
        except Exception as exc:
            logger.warning("Stock picker TickFlow batch refresh failed: %s", exc)
            return False

        if not batch_frames:
            return False

        normalized_frames: List[pd.DataFrame] = []
        for code, frame in batch_frames.items():
            if frame is None or frame.empty:
                continue
            normalized = frame.copy()
            normalized["code"] = code
            normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
            normalized["data_source"] = "TickFlowFetcher"
            normalized_frames.append(
                normalized[["code", "date", "open", "high", "low", "close", "volume", "amount", "pct_chg", "data_source"]]
            )

        if not normalized_frames:
            return False

        merged = pd.concat(normalized_frames, ignore_index=True)
        self._save_market_daily_snapshot(merged)

        coverage = self._count_daily_rows(scan_date)
        logger.info(
            "Stock picker TickFlow batch refresh completed: scan_date=%s, rows=%s, coverage=%s",
            scan_date,
            len(merged),
            coverage,
        )
        return coverage >= 2000 and self._has_sufficient_local_history_window(
            scan_date,
            minimum_codes=1000,
            minimum_rows_per_code=min(lookback_trading_days, 40),
        )

    def _has_sufficient_local_history_window(
        self,
        scan_date: date,
        *,
        minimum_codes: int,
        minimum_rows_per_code: int,
    ) -> bool:
        window_start = scan_date - timedelta(days=max(minimum_rows_per_code * 2, 120))
        with self.db.get_session() as session:
            codes = session.execute(
                select(func.count())
                .select_from(
                    select(StockDaily.code)
                    .where(and_(StockDaily.date >= window_start, StockDaily.date <= scan_date))
                    .group_by(StockDaily.code)
                    .having(func.count(StockDaily.id) >= minimum_rows_per_code)
                    .subquery()
                )
            ).scalar_one_or_none()
        return int(codes or 0) >= minimum_codes

    def _get_latest_daily_date(self) -> Optional[date]:
        with self.db.get_session() as session:
            value = session.execute(select(func.max(StockDaily.date))).scalar_one_or_none()
            return value

    def _get_latest_complete_daily_date(
        self,
        *,
        max_date: Optional[date] = None,
        minimum_rows: int = 2500,
    ) -> Optional[date]:
        with self.db.get_session() as session:
            query = (
                select(StockDaily.date)
                .group_by(StockDaily.date)
                .having(func.count(StockDaily.id) >= minimum_rows)
                .order_by(StockDaily.date.desc())
            )
            if max_date is not None:
                query = query.where(StockDaily.date <= max_date)
            value = session.execute(query.limit(1)).scalar_one_or_none()
        return value

    def _load_universe_from_local_db(self) -> pd.DataFrame:
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily.code, func.max(StockDaily.name).label("name"))
                .group_by(StockDaily.code)
            ).all()
        if not rows:
            return pd.DataFrame(columns=["code", "name"])
        return pd.DataFrame(
            [
                {
                    "code": str(code or "").strip(),
                    "name": str(name or code or "").strip(),
                }
                for code, name in rows
            ]
        )

    def _has_recent_local_history(self, scan_date: date, minimum_codes: int = 50) -> bool:
        window_start = scan_date - timedelta(days=120)
        with self.db.get_session() as session:
            codes = session.execute(
                select(func.count(func.distinct(StockDaily.code))).where(
                    and_(StockDaily.date >= window_start, StockDaily.date <= scan_date)
                )
            ).scalar_one_or_none()
        return int(codes or 0) >= minimum_codes

    def _count_daily_rows(self, target_date: date) -> int:
        with self.db.get_session() as session:
            count = session.execute(
                select(func.count(StockDaily.id)).where(StockDaily.date == target_date)
            ).scalar() or 0
            return int(count)

    @staticmethod
    def _normalize_market_daily(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        normalized = df.copy()
        normalized["code"] = normalized["ts_code"].astype(str).str.split(".").str[0]
        normalized["date"] = pd.to_datetime(normalized["trade_date"], format="%Y%m%d").dt.date
        normalized["volume"] = pd.to_numeric(normalized["vol"], errors="coerce") * 100
        normalized["amount"] = pd.to_numeric(normalized["amount"], errors="coerce") * 1000
        for column in ("open", "high", "low", "close", "pct_chg"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        normalized["data_source"] = "TushareFetcher"
        return normalized[["code", "date", "open", "high", "low", "close", "volume", "amount", "pct_chg", "data_source"]]

    def _save_market_daily_snapshot(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            if not row.get("code") or row.get("date") is None:
                continue
            records.append(
                {
                    "code": str(row["code"]),
                    "date": row["date"],
                    "open": self._to_float(row.get("open")),
                    "high": self._to_float(row.get("high")),
                    "low": self._to_float(row.get("low")),
                    "close": self._to_float(row.get("close")),
                    "volume": self._to_float(row.get("volume")),
                    "amount": self._to_float(row.get("amount")),
                    "pct_chg": self._to_float(row.get("pct_chg")),
                    "ma5": None,
                    "ma10": None,
                    "ma20": None,
                    "volume_ratio": None,
                    "data_source": str(row.get("data_source") or "TushareFetcher"),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            )
        if not records:
            return

        def _write(session) -> int:
            table = StockDaily.__table__
            # SQLite has a low parameter ceiling, so large market-wide snapshots
            # must be upserted in small chunks.
            chunk_size = 50
            for start in range(0, len(records), chunk_size):
                batch = records[start:start + chunk_size]
                stmt = sqlite_insert(table).values(batch)
                update_cols = {
                    column: getattr(stmt.excluded, column)
                    for column in (
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                        "pct_chg",
                        "data_source",
                        "updated_at",
                    )
                }
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["code", "date"],
                        set_=update_cols,
                    )
                )
            return len(records)

        self.db._run_write_transaction("save_stock_picker_market_snapshot", _write)

    def _load_history_frames(
        self,
        codes: Sequence[str],
        scan_date: date,
        lookback_days: int = 90,
    ) -> Dict[str, pd.DataFrame]:
        if not codes:
            return {}
        start_date = scan_date - timedelta(days=lookback_days * 2)
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        code_list = list(dict.fromkeys(codes))
        batch_size = 800
        with self.db.get_session() as session:
            for index in range(0, len(code_list), batch_size):
                batch = code_list[index:index + batch_size]
                rows = session.execute(
                    select(StockDaily)
                    .where(
                        and_(
                            StockDaily.code.in_(batch),
                            StockDaily.date >= start_date,
                            StockDaily.date <= scan_date,
                        )
                    )
                    .order_by(StockDaily.code.asc(), StockDaily.date.asc())
                ).scalars().all()
                for row in rows:
                    grouped.setdefault(row.code, []).append(
                        {
                            "date": row.date,
                            "open": self._to_float(row.open),
                            "high": self._to_float(row.high),
                            "low": self._to_float(row.low),
                            "close": self._to_float(row.close),
                            "volume": self._to_float(row.volume),
                            "amount": self._to_float(row.amount),
                            "pct_chg": self._to_float(row.pct_chg),
                        }
                    )
        frames: Dict[str, pd.DataFrame] = {}
        for code, items in grouped.items():
            df = pd.DataFrame(items)
            if df.empty:
                continue
            df = df.sort_values("date").reset_index(drop=True)
            frames[code] = self._attach_indicators(df)
        return frames

    @staticmethod
    def _attach_indicators(df: pd.DataFrame) -> pd.DataFrame:
        bars = df.copy()
        numeric_columns = ["open", "high", "low", "close", "volume", "amount", "pct_chg"]
        for column in numeric_columns:
            bars[column] = pd.to_numeric(bars[column], errors="coerce")
        for window in (5, 10, 20, 30, 60):
            bars[f"ma{window}"] = bars["close"].rolling(window).mean()
        bars["vol_ma5"] = bars["volume"].rolling(5).mean()
        bars["vol_ma20"] = bars["volume"].rolling(20).mean()
        bars["ma20_slope"] = bars["ma20"] - bars["ma20"].shift(1)
        bars["ma60_slope"] = bars["ma60"] - bars["ma60"].shift(1)
        bars["max_high_20_prev"] = bars["high"].shift(1).rolling(20).max()
        low_n = bars["low"].rolling(9).min()
        high_n = bars["high"].rolling(9).max()
        rsv = (bars["close"] - low_n) / (high_n - low_n).replace(0, math.nan) * 100
        bars["kdj_k"] = rsv.ewm(com=2, adjust=False).mean()
        bars["kdj_d"] = bars["kdj_k"].ewm(com=2, adjust=False).mean()
        bars["kdj_j"] = 3 * bars["kdj_k"] - 2 * bars["kdj_d"]
        ema12 = bars["close"].ewm(span=12, adjust=False).mean()
        ema26 = bars["close"].ewm(span=26, adjust=False).mean()
        bars["macd_dif"] = ema12 - ema26
        bars["macd_dea"] = bars["macd_dif"].ewm(span=9, adjust=False).mean()
        bars["macd_hist"] = (bars["macd_dif"] - bars["macd_dea"]) * 2
        return bars

    def _evaluate_strategy(
        self,
        strategy_id: str,
        *,
        code: str,
        name: str,
        bars: pd.DataFrame,
        market_snapshot: Dict[str, Any],
        params: Dict[str, Any],
    ) -> StrategySignal:
        if len(bars) < 65:
            return StrategySignal(False, 0.0, "insufficient", "观望", "样本不足", [], None, None, {})
        today = bars.iloc[-1]
        prev = bars.iloc[-2]
        recent8 = bars.tail(8)

        if any(pd.isna(today.get(col)) for col in ("ma5", "ma10", "ma20", "ma30", "ma60")):
            return StrategySignal(False, 0.0, "insufficient", "观望", "均线样本不足", [], None, None, {})

        close_price = self._to_float(today["close"])
        open_price = self._to_float(today["open"])
        high_price = self._to_float(today["high"])
        ma5 = self._to_float(today["ma5"])
        ma10 = self._to_float(today["ma10"])
        ma20 = self._to_float(today["ma20"])
        ma30 = self._to_float(today["ma30"])
        ma60 = self._to_float(today["ma60"])
        low_price = self._to_float(today["low"])
        volume = self._to_float(today["volume"])
        pct_chg = self._to_float(today["pct_chg"])
        vol_ma20 = max(self._to_float(today["vol_ma20"]), 1.0)
        vol_ma5 = max(self._to_float(today["vol_ma5"]), 1.0)
        recent20 = bars.tail(20)
        recent40 = bars.tail(40)
        recent60 = bars.tail(60)
        distance_to_ma20_pct = abs(close_price - ma20) / max(ma20, 0.01) * 100
        distance_to_ma5_pct = abs(close_price - ma5) / max(ma5, 0.01) * 100
        volume_spike_factor = float((recent8["volume"] / recent8["vol_ma20"].replace(0, pd.NA)).max(skipna=True) or 0.0)
        market_score = float(market_snapshot.get("score", 50.0) or 50.0)
        ma5_cross_ma20 = bool(prev["ma5"] <= prev["ma20"] and today["ma5"] > today["ma20"])
        ma5_cross_recent = bool((bars["ma5"].shift(1) <= bars["ma20"].shift(1)).tail(2).any() and (bars["ma5"] > bars["ma20"]).tail(2).any())
        ma20_up = bool(today["ma20_slope"] > 0)
        ma60_up = bool(today["ma60_slope"] > 0)
        trend_stack = bool(ma5 > ma10 > ma20 > ma30)
        trend_stack_up = bool(
            trend_stack
            and today["ma5"] > prev["ma5"]
            and today["ma10"] > prev["ma10"]
            and today["ma20"] > prev["ma20"]
            and today["ma30"] > prev["ma30"]
        )
        touch_ma5 = bool(low_price <= ma5 * 1.01 and close_price >= ma5 * 0.99 and close_price >= ma10 * 0.985)
        shrink_volume = bool(volume <= vol_ma5 * 0.95)
        breakout_near_high = bool(close_price >= self._to_float(today["max_high_20_prev"]) * 0.985 if not pd.isna(today["max_high_20_prev"]) else False)
        macd_bull = bool(today["macd_dif"] > today["macd_dea"] and today["macd_hist"] > -0.02)
        kdj_bull = bool(today["kdj_j"] >= today["kdj_k"])
        strong_body = bool(pct_chg >= 2.0)
        body_pct = abs(close_price - open_price) / max(close_price, 0.01) * 100
        upper_shadow_pct = max(high_price - max(close_price, open_price), 0.0) / max(close_price, 0.01) * 100
        lower_shadow_pct = max(min(close_price, open_price) - low_price, 0.0) / max(close_price, 0.01) * 100
        range_20_pct = (self._to_float(recent20["high"].max()) - self._to_float(recent20["low"].min())) / max(close_price, 0.01) * 100
        range_40_pct = (self._to_float(recent40["high"].max()) - self._to_float(recent40["low"].min())) / max(close_price, 0.01) * 100
        amount = self._to_float(today.get("amount"))
        amount_ma20 = max(self._to_float(recent20["amount"].mean()), 1.0) if "amount" in recent20.columns else 1.0
        amount_ratio = amount / max(amount_ma20, 0.01) if amount > 0 else 0.0
        high_20_prev = self._to_float(recent20["high"].shift(1).max())
        low_20_prev = self._to_float(recent20["low"].shift(1).min())
        high_60 = self._to_float(recent60["high"].max())
        low_60 = self._to_float(recent60["low"].min())
        recent_decline_pct = (high_60 - close_price) / max(high_60, 0.01) * 100 if high_60 > 0 else 0.0
        breakout_above_20 = bool(close_price >= max(high_20_prev * 1.002, ma20))
        support_hold = bool(low_price >= low_20_prev * 0.985 if low_20_prev > 0 else True)
        tr = pd.concat(
            [
                bars["high"] - bars["low"],
                (bars["high"] - bars["close"].shift(1)).abs(),
                (bars["low"] - bars["close"].shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr14 = float(tr.tail(14).mean() or 0.0)
        atr_pct = atr14 / max(close_price, 0.01) * 100
        volatility_contract = bool(atr_pct <= max(2.8, float(tr.tail(30).mean() or atr14) / max(close_price, 0.01) * 100 * 0.85))
        recent_low_idx = recent20["low"].astype(float).idxmin()
        prior_low_hist = self._to_float(bars.loc[recent_low_idx, "macd_hist"]) if recent_low_idx in bars.index else 0.0
        bottom_divergence = bool(low_price <= low_20_prev * 1.02 and today["macd_hist"] > prior_low_hist and support_hold)
        top_divergence = bool(close_price >= high_20_prev * 0.995 and today["macd_hist"] < prev["macd_hist"] and upper_shadow_pct >= 1.0)
        box_support = bool(close_price <= recent40["low"].min() * 1.04 if len(recent40) >= 20 else False)
        box_resistance = bool(close_price >= recent40["high"].max() * 0.98 if len(recent40) >= 20 else False)
        bearish_trend = bool(close_price < ma20 < ma60 and recent_decline_pct >= 15)
        open_above_prev_close = bool(open_price >= self._to_float(prev["close"]))
        close_near_high = bool((high_price - close_price) / max(close_price, 0.01) * 100 <= 0.8)
        institution_style = bool(amount_ratio >= 1.1 and close_near_high and upper_shadow_pct <= 1.0 and body_pct >= 1.2)
        weak_money_follow = bool(upper_shadow_pct >= 1.8 and body_pct <= 1.0 and close_price < high_price * 0.99)

        reasons: List[str] = []
        score = 0.0
        passed = False
        setup_type = "mixed"
        preferred_setup_type = str(params.get("preferred_setup_type") or "").strip().lower()
        market_score_floor = float(params.get("market_score_floor", 0.0) or 0.0)
        max_ma5_distance_pct = float(params.get("max_ma5_distance_pct", 2.0) or 2.0)
        pullback_min_score_threshold = float(params.get("pullback_min_score_threshold", params.get("min_score_threshold", 68.0)) or 68.0)
        pullback_volume_spike_multiplier = float(params.get("pullback_volume_spike_multiplier", params.get("volume_spike_multiplier", 1.5)) or 1.5)
        pullback_max_ma20_distance_pct = float(params.get("pullback_max_ma20_distance_pct", params.get("max_ma20_distance_pct", 4.0)) or 4.0)
        pullback_max_ma5_distance_pct = float(params.get("pullback_max_ma5_distance_pct", max_ma5_distance_pct) or max_ma5_distance_pct)
        pullback_market_score_floor = float(params.get("pullback_market_score_floor", market_score_floor) or market_score_floor)
        breakout_min_score_threshold = float(params.get("breakout_min_score_threshold", params.get("min_score_threshold", 72.0)) or 72.0)
        breakout_volume_spike_multiplier = float(params.get("breakout_volume_spike_multiplier", params.get("volume_spike_multiplier", 1.8)) or 1.8)
        breakout_max_ma20_distance_pct = float(params.get("breakout_max_ma20_distance_pct", params.get("max_ma20_distance_pct", 3.0)) or 3.0)
        breakout_max_ma5_distance_pct = float(params.get("breakout_max_ma5_distance_pct", max_ma5_distance_pct) or max_ma5_distance_pct)
        breakout_market_score_floor = float(params.get("breakout_market_score_floor", market_score_floor) or market_score_floor)
        trend_follow_min_score_threshold = float(params.get("trend_follow_min_score_threshold", params.get("min_score_threshold", 72.0)) or 72.0)
        trend_follow_volume_spike_multiplier = float(params.get("trend_follow_volume_spike_multiplier", 1.2) or 1.2)
        trend_follow_max_ma20_distance_pct = float(params.get("trend_follow_max_ma20_distance_pct", 12.0) or 12.0)
        trend_follow_max_ma5_distance_pct = float(params.get("trend_follow_max_ma5_distance_pct", 3.5) or 3.5)
        trend_follow_market_score_floor = float(params.get("trend_follow_market_score_floor", market_score_floor) or market_score_floor)
        main_force_market_score_floor = float(params.get("main_force_market_score_floor", market_score_floor) or market_score_floor)
        shanliu_market_score_floor = float(params.get("shanliu_market_score_floor", market_score_floor) or market_score_floor)
        ma_gap_5_10_pct = (ma5 - ma10) / max(close_price, 0.01) * 100
        ma_gap_10_20_pct = (ma10 - ma20) / max(close_price, 0.01) * 100
        ma_gap_20_30_pct = (ma20 - ma30) / max(close_price, 0.01) * 100
        positive_ma_gaps = [gap for gap in (ma_gap_5_10_pct, ma_gap_10_20_pct, ma_gap_20_30_pct) if gap > 0]
        ma_gap_balance_pct = (
            max(positive_ma_gaps) - min(positive_ma_gaps)
            if len(positive_ma_gaps) == 3
            else 999.0
        )
        ma_gap_balance_limit_pct = float(params.get("ma_gap_balance_limit_pct", 2.8) or 2.8)
        ma_gap_min_pct = float(params.get("ma_gap_min_pct", 0.0) or 0.0)
        orderly_ma_spacing = bool(
            ma_gap_5_10_pct > ma_gap_min_pct
            and ma_gap_10_20_pct > ma_gap_min_pct
            and ma_gap_20_30_pct > ma_gap_min_pct
            and ma_gap_balance_pct <= ma_gap_balance_limit_pct
        )
        ma_gap_mean_pct = sum(positive_ma_gaps) / len(positive_ma_gaps) if len(positive_ma_gaps) == 3 else 0.0
        ma_gap_uniformity_score = max(0.0, 100.0 - ma_gap_balance_pct * 25.0) if ma_gap_balance_pct < 999 else 0.0

        limitup_lookback = int(float(params.get("main_force_limitup_lookback_days", 60) or 60))
        recent_limit_window = bars.tail(max(8, limitup_lookback)).copy()
        limit_like = recent_limit_window["pct_chg"].astype(float).fillna(0.0) >= 9.0
        prior_limit_streak = bool((limit_like.shift(1, fill_value=False) & limit_like).any())
        prior_limit_count = int(limit_like.sum())
        cross_series = (bars["ma5"].shift(1) <= bars["ma20"].shift(1)) & (bars["ma5"] > bars["ma20"])
        recent_crosses = bars.loc[cross_series.fillna(False)].tail(5)
        cross_volume_multiplier = float(params.get("main_force_cross_volume_multiplier", 1.25) or 1.25)
        cross_with_volume = False
        cross_days_ago: Optional[int] = None
        if not recent_crosses.empty:
            cross_idx = recent_crosses.index[-1]
            cross_row = bars.loc[cross_idx]
            cross_volume = self._to_float(cross_row.get("volume"))
            cross_vol_ma20 = max(self._to_float(cross_row.get("vol_ma20")), 1.0)
            cross_with_volume = cross_volume / cross_vol_ma20 >= cross_volume_multiplier
            cross_days_ago = int(len(bars) - 1 - list(bars.index).index(cross_idx)) if cross_idx in bars.index else None
        main_force_pullback_max_ma20_distance_pct = float(params.get("main_force_pullback_max_ma20_distance_pct", 3.0) or 3.0)
        main_force_shrink_volume_ratio = float(params.get("main_force_shrink_volume_ratio", 1.05) or 1.05)
        pullback_to_ma20 = bool(
            close_price >= ma20 * 0.985
            and distance_to_ma20_pct <= main_force_pullback_max_ma20_distance_pct
            and low_price <= ma20 * 1.025
        )
        post_cross_shrink = bool(volume <= vol_ma5 * main_force_shrink_volume_ratio and volume <= vol_ma20 * 1.15)

        if strategy_id in {"mainboard_swing_master", "ma_golden_cross"}:
            breakout_mode = (
                ma5_cross_recent
                and ma20_up
                and ma60_up
                and distance_to_ma20_pct <= breakout_max_ma20_distance_pct
                and volume_spike_factor >= breakout_volume_spike_multiplier
                and close_price >= ma20
            )
            if breakout_mode:
                setup_type = "breakout"
                score += 35
                reasons.append("5日线近期上穿20日线，20日与60日线同步拐头向上")
            if ma5_cross_ma20:
                score += 8
                reasons.append("当日形成明确金叉")
            if strong_body:
                score += 8
                reasons.append("收盘中阳确认启动")
            if breakout_near_high:
                score += 10
                reasons.append("接近近端突破位收盘")
            if macd_bull:
                score += 8
                reasons.append("MACD 多头")
            if kdj_bull:
                score += 6
            passed = breakout_mode

        if strategy_id in {"mainboard_swing_master", "shrink_pullback"}:
            pullback_mode = (
                trend_stack_up
                and touch_ma5
                and ma60_up
                and distance_to_ma20_pct <= pullback_max_ma20_distance_pct
                and volume_spike_factor >= pullback_volume_spike_multiplier
                and distance_to_ma5_pct <= pullback_max_ma5_distance_pct
            )
            if pullback_mode:
                setup_type = "pullback" if strategy_id != "mainboard_swing_master" or not passed else "mixed"
                score += 34
                reasons.append("5/10/20/30日线多头向上，K线回踩5日线后仍守住趋势")
            if distance_to_ma5_pct <= 2.0:
                score += 9
            if shrink_volume:
                score += 8
                reasons.append("回踩阶段量能收缩，抛压可控")
            if macd_bull:
                score += 8
            if kdj_bull:
                score += 5
            passed = passed or pullback_mode

        if strategy_id in {"mainboard_swing_master", "swing_trend_follow"}:
            trend_follow_mode = (
                trend_stack_up
                and orderly_ma_spacing
                and ma60_up
                and close_price >= ma5 * 0.985
                and distance_to_ma5_pct <= trend_follow_max_ma5_distance_pct
                and distance_to_ma20_pct <= trend_follow_max_ma20_distance_pct
                and volume_spike_factor >= trend_follow_volume_spike_multiplier
                and macd_bull
                and not weak_money_follow
            )
            if trend_follow_mode and (not passed or strategy_id == "swing_trend_follow"):
                setup_type = "trend_follow"
                score += 42
                reasons.append("5/10/20/30日线多头上行且均线间隔有序，符合波段趋势跟随结构")
            elif trend_follow_mode:
                score += 8
                reasons.append("均线间隔有序，趋势跟随结构加分")
            if trend_follow_mode and ma_gap_uniformity_score >= 70:
                score += 7
                reasons.append("均线间隔接近平行扩散，趋势稳定性较好")
            if trend_follow_mode and close_near_high:
                score += 6
                reasons.append("收盘接近日内高位，趋势承接较强")
            if trend_follow_mode and amount_ratio >= 1.05:
                score += 5
            passed = passed or trend_follow_mode

        if strategy_id == "main_force_breakout":
            main_force_mode = (
                prior_limit_streak
                and prior_limit_count >= 2
                and cross_with_volume
                and ma5 > ma20
                and ma20_up
                and ma60_up
                and pullback_to_ma20
                and post_cross_shrink
                and macd_bull
                and not weak_money_follow
            )
            if main_force_mode:
                setup_type = "main_force_breakout"
                score += 46
                reasons.append("前期存在连板记忆，回调后 5 日线放量上穿 20 日线并缩量回踩 20 日线")
                passed = True
            if prior_limit_streak:
                score += 10
                reasons.append("近阶段存在连板/强势股记忆")
            if cross_with_volume:
                score += 9
                reasons.append("5 日线上穿 20 日线时量能确认")
            if pullback_to_ma20:
                score += 10
                reasons.append("股价回踩 20 日线附近未破结构")
            if post_cross_shrink:
                score += 8
                reasons.append("回踩阶段缩量，筹码抛压较轻")
            if amount_ratio >= 1.0 and close_price >= ma20:
                score += 5

        if strategy_id == "shanliu_theme_flow":
            shanliu_volume_spike_multiplier = float(params.get("shanliu_volume_spike_multiplier", 1.25) or 1.25)
            shanliu_amount_ratio_min = float(params.get("shanliu_amount_ratio_min", 1.05) or 1.05)
            theme_ignition = bool(
                volume_spike_factor >= shanliu_volume_spike_multiplier
                and amount_ratio >= shanliu_amount_ratio_min
                and pct_chg >= 1.2
                and close_price >= ma10
                and not weak_money_follow
            )
            leader_role = bool(
                theme_ignition
                and pct_chg >= 4.0
                and close_near_high
                and (breakout_near_high or breakout_above_20)
            )
            core_role = bool(
                theme_ignition
                and (trend_stack_up or ma20_up)
                and institution_style
                and amount_ratio >= max(1.15, shanliu_amount_ratio_min)
            )
            relay_role = bool(
                theme_ignition
                and 1.2 <= pct_chg <= 5.5
                and (touch_ma5 or close_price >= ma5)
                and volume_spike_factor >= shanliu_volume_spike_multiplier
            )
            low_absorb_role = bool(
                market_score < 50
                and (trend_stack_up or ma20_up)
                and close_price >= ma20 * 0.985
                and distance_to_ma20_pct <= 4.0
                and shrink_volume
                and not weak_money_follow
            )
            if leader_role:
                setup_type = "shanliu_leader"
                score += 44
                reasons.append("题材启动日个股强于板块，具备高度龙头候选特征")
            elif core_role:
                setup_type = "shanliu_core"
                score += 40
                reasons.append("量价偏机构主导，适合作为强题材中军观察")
            elif relay_role:
                setup_type = "shanliu_relay"
                score += 36
                reasons.append("题材初燃后量能接力，短线情绪跟随条件成立")
            elif low_absorb_role:
                setup_type = "shanliu_low_absorb"
                score += 34
                reasons.append("市场情绪偏弱时回踩强趋势，符合低吸拿筹码模式")
            passed = leader_role or core_role or relay_role or low_absorb_role
            if hot_money_market := (market_score >= 60):
                score += 7
                reasons.append("A 股情绪偏强，适合高抛套利型跟随")
            elif market_score < 50:
                score += 3
                reasons.append("A 股情绪偏弱，模式按低吸和核心票承接处理")

        if strategy_id == "volume_breakout":
            breakout_mode = breakout_near_high and volume_spike_factor >= float(params.get("volume_spike_multiplier", 2.0)) and ma20_up and ma60_up
            if breakout_mode:
                setup_type = "breakout"
                score += 42
                reasons.append("放量突破近20日高点")
                passed = True
            if strong_body:
                score += 12
            if macd_bull:
                score += 10

        if strategy_id == "dragon_head":
            breakout_mode = (
                breakout_above_20
                and ma20_up
                and ma60_up
                and volume_spike_factor >= float(params.get("volume_spike_multiplier", 2.0))
                and pct_chg >= 2.5
                and close_price >= ma5
            )
            if breakout_mode:
                setup_type = "breakout"
                score += 44
                reasons.append("强势突破近端平台，具备龙头候选的动量基础")
                passed = True
            if open_above_prev_close:
                score += 6
            if strong_body:
                score += 8
            if breakout_near_high:
                score += 8
            if macd_bull:
                score += 8

        if strategy_id == "chan_theory":
            chan_second_buy = bool(
                trend_stack_up
                and touch_ma5
                and close_price > ma20
                and support_hold
                and macd_bull
                and not top_divergence
            )
            chan_third_buy = bool(
                ma20_up
                and ma60_up
                and breakout_above_20
                and close_price > ma10
                and volume_spike_factor >= max(1.2, float(params.get("volume_spike_multiplier", 1.4)) * 0.8)
            )
            if bottom_divergence:
                setup_type = "pullback"
                score += 28
                reasons.append("低位 MACD 背驰改善，接近缠论二买区域")
            if chan_second_buy:
                setup_type = "pullback"
                score += 34
                reasons.append("均线多头中回踩不破，符合缠论二买结构")
            if chan_third_buy:
                setup_type = "breakout"
                score += 38
                reasons.append("离开中枢上沿并放量确认，接近缠论三买")
            passed = bottom_divergence or chan_second_buy or chan_third_buy
            if top_divergence:
                passed = False
                reasons.append("高位背驰迹象明显，暂不作为缠论买点")

        if strategy_id == "wave_theory":
            impulse_extension = bool(
                ma20_up
                and ma60_up
                and breakout_above_20
                and volume_spike_factor >= 1.3
                and pct_chg >= 1.5
            )
            retrace_ratio = (high_60 - close_price) / max(high_60 - low_60, 0.01)
            wave_two_pullback = bool(
                trend_stack_up
                and 0.25 <= retrace_ratio <= 0.65
                and touch_ma5
                and macd_bull
            )
            if wave_two_pullback:
                setup_type = "pullback"
                score += 36
                reasons.append("回撤位置接近波浪理论二浪/四浪区间")
            if impulse_extension:
                setup_type = "breakout"
                score += 40
                reasons.append("放量推升，具备三浪延伸结构特征")
            passed = wave_two_pullback or impulse_extension

        if strategy_id == "emotion_cycle":
            sentiment_bottom = bool(
                market_score <= 52
                and volatility_contract
                and shrink_volume
                and close_price >= ma20 * 0.97
                and macd_bull
            )
            warm_start = bool(
                45 <= market_score <= 68
                and volatility_contract
                and strong_body
                and close_price >= ma5
            )
            if sentiment_bottom:
                setup_type = "pullback"
                score += 34
                reasons.append("情绪低位叠加波动收敛，具备逆情绪启动条件")
            if warm_start:
                setup_type = "breakout"
                score += 28
                reasons.append("情绪回暖初期放量转强")
            passed = sentiment_bottom or warm_start

        if strategy_id == "box_oscillation":
            box_width_ok = 5.0 <= range_40_pct <= 18.0
            box_buy = bool(box_width_ok and box_support and support_hold and macd_bull)
            box_break = bool(box_width_ok and box_resistance and volume_spike_factor >= 1.4 and ma20_up)
            if box_buy:
                setup_type = "pullback"
                score += 34
                reasons.append("处于箱体底部附近，支撑位有效")
            if box_break:
                setup_type = "breakout"
                score += 30
                reasons.append("箱体上沿放量突破")
            passed = box_buy or box_break

        if strategy_id == "bottom_volume":
            bottom_reversal = bool(
                bearish_trend
                and volume_spike_factor >= max(2.4, float(params.get("volume_spike_multiplier", 1.4)) * 1.6)
                and close_price > open_price
                and lower_shadow_pct >= 0.8
                and support_hold
            )
            if bottom_reversal:
                setup_type = "pullback"
                score += 40
                reasons.append("下跌后底部放量并收阳，具备反转信号")
                passed = True

        if strategy_id == "one_yang_three_yin":
            last5 = bars.tail(5).reset_index(drop=True)
            if len(last5) == 5:
                day1 = last5.iloc[0]
                day5 = last5.iloc[4]
                middle = last5.iloc[1:4]
                day1_bull = self._to_float(day1["close"]) > self._to_float(day1["open"]) and (
                    (self._to_float(day1["close"]) - self._to_float(day1["open"])) / max(self._to_float(day1["close"]), 0.01) * 100 >= 2.0
                )
                middle_inside = bool(
                    (middle["low"] >= min(self._to_float(day1["open"]), self._to_float(day1["close"])) * 0.99).all()
                    and (middle["high"] <= max(self._to_float(day1["open"]), self._to_float(day1["close"])) * 1.01).all()
                )
                middle_shrink = bool((middle["volume"] <= middle["volume"].shift(1).fillna(middle["volume"].iloc[0]) * 1.05).all())
                day5_break = bool(
                    self._to_float(day5["close"]) > self._to_float(day5["open"])
                    and self._to_float(day5["close"]) >= self._to_float(day1["close"])
                )
                pattern_ok = day1_bull and middle_inside and middle_shrink and day5_break and trend_stack
                if pattern_ok:
                    setup_type = "pullback"
                    score += 42
                    reasons.append("一阳夹三阴形态完成，趋势整理后再启动")
                    passed = True

        if ma60_up:
            score += 8
        if market_score >= 55:
            score += 6
        if institution_style:
            score += 8
            reasons.append("量价配合偏强，存在机构/主导资金推动迹象")
        if weak_money_follow:
            score -= 6
            reasons.append("上影偏长且收盘不够强，短线跟风资金承接一般")

        active_market_score_floor = market_score_floor
        if setup_type == "pullback":
            active_market_score_floor = pullback_market_score_floor
        elif setup_type == "breakout":
            active_market_score_floor = breakout_market_score_floor
        elif setup_type == "trend_follow":
            active_market_score_floor = trend_follow_market_score_floor
        elif setup_type == "main_force_breakout":
            active_market_score_floor = main_force_market_score_floor
        elif setup_type.startswith("shanliu"):
            active_market_score_floor = shanliu_market_score_floor
        elif setup_type == "mixed":
            active_market_score_floor = max(pullback_market_score_floor, breakout_market_score_floor, market_score_floor)

        if active_market_score_floor > 0 and market_score < active_market_score_floor:
            market_penalty = min(24.0, (active_market_score_floor - market_score) * 1.1 + 4.0)
            score -= market_penalty
            reasons.append(
                f"A股情绪分 {round(market_score, 2)} 低于惩罚起点 {round(active_market_score_floor, 2)}，扣分 {round(market_penalty, 2)}"
            )

        min_score_threshold = float(params.get("min_score_threshold", 70.0))
        passed = passed and score >= min_score_threshold
        if preferred_setup_type in {"pullback", "breakout"} and setup_type != preferred_setup_type:
            passed = False
        if strategy_id in {"mainboard_swing_master", "swing_trend_follow"}:
            if setup_type == "pullback" and score < pullback_min_score_threshold:
                passed = False
                reasons.append(
                    f"回踩结构评分 {round(score, 2)} 低于阈值 {round(pullback_min_score_threshold, 2)}"
                )
            if setup_type == "breakout" and score < breakout_min_score_threshold:
                passed = False
                reasons.append(
                    f"突破结构评分 {round(score, 2)} 低于阈值 {round(breakout_min_score_threshold, 2)}"
                )
            if setup_type == "trend_follow" and score < trend_follow_min_score_threshold:
                passed = False
                reasons.append(
                    f"趋势跟随结构评分 {round(score, 2)} 低于阈值 {round(trend_follow_min_score_threshold, 2)}"
                )
        if strategy_id == "main_force_breakout":
            main_force_min_score_threshold = float(params.get("main_force_min_score_threshold", min_score_threshold) or min_score_threshold)
            if score < main_force_min_score_threshold:
                passed = False
                reasons.append(f"主力突破结构评分 {round(score, 2)} 低于阈值 {round(main_force_min_score_threshold, 2)}")

        if not passed:
            return StrategySignal(
                passed=False,
                score=score,
                setup_type=setup_type,
                operation_advice="观望",
                analysis_summary=f"{name} 未达到 {strategy_id} 触发阈值",
                reasons=reasons,
                stop_loss=None,
                take_profit=None,
                metrics={
                    "distance_to_ma20_pct": round(distance_to_ma20_pct, 2),
                    "distance_to_ma5_pct": round(distance_to_ma5_pct, 2),
                    "volume_spike_factor": round(volume_spike_factor, 2),
                    "market_score": round(market_score, 2),
                    "pct_chg": round(pct_chg, 2),
                    "amount_ratio": round(amount_ratio, 2),
                    "atr14": round(atr14, 2),
                    "atr_pct": round(atr_pct, 2),
                    "range_40_pct": round(range_40_pct, 2),
                    "upper_shadow_pct": round(upper_shadow_pct, 2),
                    "body_pct": round(body_pct, 2),
                    "institution_style": institution_style,
                    "weak_money_follow": weak_money_follow,
                    "trend_stack_up": trend_stack_up,
                    "ma20_up": ma20_up,
                    "ma60_up": ma60_up,
                    "top_divergence": top_divergence,
                    "bottom_divergence": bottom_divergence,
                    "ma_gap_5_10_pct": round(ma_gap_5_10_pct, 2),
                    "ma_gap_10_20_pct": round(ma_gap_10_20_pct, 2),
                    "ma_gap_20_30_pct": round(ma_gap_20_30_pct, 2),
                    "ma_gap_balance_pct": round(ma_gap_balance_pct, 2) if ma_gap_balance_pct < 999 else None,
                    "ma_gap_uniformity_score": round(ma_gap_uniformity_score, 2),
                    "orderly_ma_spacing": orderly_ma_spacing,
                    "prior_limit_streak": prior_limit_streak,
                    "prior_limit_count": prior_limit_count,
                    "cross_with_volume": cross_with_volume,
                    "cross_days_ago": cross_days_ago,
                    "pullback_to_ma20": pullback_to_ma20,
                    "post_cross_shrink": post_cross_shrink,
                },
            )

        stop_loss = round(min(ma10 if setup_type == "pullback" else ma20, close_price * 0.95), 2)
        take_profit = round(close_price * (1.12 if setup_type == "pullback" else 1.10), 2)

        return StrategySignal(
            passed=True,
            score=score,
            setup_type=setup_type,
            operation_advice="买入",
            analysis_summary=f"{name} 符合 {setup_type} 型波段候选，适合次日观察承接后参与。",
            reasons=reasons[:6],
            stop_loss=stop_loss,
            take_profit=take_profit,
            metrics={
                "distance_to_ma20_pct": round(distance_to_ma20_pct, 2),
                "distance_to_ma5_pct": round(distance_to_ma5_pct, 2),
                "volume_spike_factor": round(volume_spike_factor, 2),
                "market_score": round(market_score, 2),
                "pct_chg": round(pct_chg, 2),
                "amount_ratio": round(amount_ratio, 2),
                "atr14": round(atr14, 2),
                "atr_pct": round(atr_pct, 2),
                "range_40_pct": round(range_40_pct, 2),
                "upper_shadow_pct": round(upper_shadow_pct, 2),
                "body_pct": round(body_pct, 2),
                "institution_style": institution_style,
                "weak_money_follow": weak_money_follow,
                "trend_stack_up": trend_stack_up,
                "ma20_up": ma20_up,
                "ma60_up": ma60_up,
                "macd_bull": macd_bull,
                "kdj_bull": kdj_bull,
                "top_divergence": top_divergence,
                "bottom_divergence": bottom_divergence,
                "ma_gap_5_10_pct": round(ma_gap_5_10_pct, 2),
                "ma_gap_10_20_pct": round(ma_gap_10_20_pct, 2),
                "ma_gap_20_30_pct": round(ma_gap_20_30_pct, 2),
                "ma_gap_balance_pct": round(ma_gap_balance_pct, 2) if ma_gap_balance_pct < 999 else None,
                "ma_gap_uniformity_score": round(ma_gap_uniformity_score, 2),
                "orderly_ma_spacing": orderly_ma_spacing,
                "prior_limit_streak": prior_limit_streak,
                "prior_limit_count": prior_limit_count,
                "cross_with_volume": cross_with_volume,
                "cross_days_ago": cross_days_ago,
                "pullback_to_ma20": pullback_to_ma20,
                "post_cross_shrink": post_cross_shrink,
            },
        )

    def _build_market_snapshot(self, *, use_cache: bool = True) -> Dict[str, Any]:
        indices = self.fetcher_manager.get_main_indices(region="cn", use_cache=use_cache) or []
        stats = self.fetcher_manager.get_market_stats(use_cache=use_cache) or {}
        score = 50.0
        rising_indices = 0
        for item in indices:
            pct = self._to_float(item.get("change_pct"))
            if pct > 0:
                rising_indices += 1
                score += 5
            elif pct < 0:
                score -= 4
        up_count = float(stats.get("up_count") or 0)
        down_count = float(stats.get("down_count") or 0)
        limit_up_count = float(stats.get("limit_up_count") or 0)
        limit_down_count = float(stats.get("limit_down_count") or 0)
        if up_count + down_count > 0:
            breadth = up_count / max(up_count + down_count, 1.0)
            score += (breadth - 0.5) * 24
        score += min(limit_up_count, 80.0) * 0.08
        score -= min(limit_down_count, 40.0) * 0.25
        regime = "震荡"
        if score >= 63:
            regime = "偏强"
        elif score <= 45:
            regime = "偏弱"
        return {
            "score": round(max(0.0, min(score, 100.0)), 2),
            "regime": regime,
            "indices": indices,
            "stats": stats,
            "summary": f"A股情绪 {regime}，指数走强数 {rising_indices}/{len(indices) or 1}。",
        }

    def _build_us_market_snapshot(self) -> Dict[str, Any]:
        indices = self.fetcher_manager.get_main_indices(region="us") or []
        score = 50.0
        falling = 0
        for item in indices:
            pct = self._to_float(item.get("change_pct"))
            if pct > 0:
                score += 6
            elif pct < 0:
                falling += 1
                score -= 7
        mood = "中性"
        if score >= 58:
            mood = "偏暖"
        elif score <= 42:
            mood = "偏冷"
        return {
            "score": round(max(0.0, min(score, 100.0)), 2),
            "mood": mood,
            "indices": indices,
            "summary": f"隔夜美股情绪 {mood}，弱势指数数 {falling}/{len(indices) or 1}。",
        }

    @staticmethod
    def _normalize_board_name(name: Any) -> str:
        text = str(name or "").strip()
        if not text:
            return ""
        text = re.sub(r"\s+", "", text)
        text = (
            text.replace("、", "")
            .replace("/", "")
            .replace("Ⅱ", "")
            .replace("Ⅲ", "")
            .replace("I", "")
        )
        replacements = {
            "服装服饰": "服饰",
            "纺织服装": "纺织",
            "食品饮料": "食品",
            "食品加工": "食品",
            "预加工食品": "食品",
            "基础化工": "化工",
            "农化制品": "农化",
            "农药兽药": "农药",
            "航运港口": "航运",
            "交通运输": "运输",
            "城商行": "银行",
        }
        for source, target in replacements.items():
            text = text.replace(source, target)
        for suffix in ("概念", "板块", "行业", "制造业", "加工业", "采选业", "业"):
            if text.endswith(suffix) and len(text) > len(suffix) + 1:
                text = text[: -len(suffix)]
        return text.lower()

    @classmethod
    def _board_name_variants(cls, name: Any) -> List[str]:
        normalized = cls._normalize_board_name(name)
        if not normalized:
            return []
        variants = {normalized}
        compact = re.sub(r"(制造|加工|采选|服务|仓储|运输|流通|饮料|服饰|服装)$", "", normalized)
        if compact and len(compact) >= 2:
            variants.add(compact)
        if len(normalized) >= 4:
            variants.add(normalized[:4])
        if len(normalized) >= 2:
            for idx in range(len(normalized) - 1):
                variants.add(normalized[idx: idx + 2])
        return [item for item in variants if item]

    @classmethod
    def _match_sector_record(
        cls,
        board_names: Sequence[str],
        mapping: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not board_names or not mapping:
            return None

        best_record: Optional[Dict[str, Any]] = None
        best_score = 0.0
        for board_name in board_names:
            board_variants = cls._board_name_variants(board_name)
            if not board_variants:
                continue
            for key, record in mapping.items():
                sector_variants = cls._board_name_variants(key)
                if not sector_variants:
                    continue
                score = 0.0
                for left in board_variants:
                    for right in sector_variants:
                        if left == right:
                            score = max(score, 1.0)
                        elif left in right or right in left:
                            score = max(score, 0.9)
                        else:
                            ratio = SequenceMatcher(None, left, right).ratio()
                            if ratio >= 0.76:
                                score = max(score, ratio)
                if score > best_score:
                    best_score = score
                    best_record = record
        return best_record if best_score >= 0.76 else None

    def _build_sector_snapshot(self, *, use_cache: bool = True, top_n: int = 12) -> Dict[str, Any]:
        cache_key = f"{date.today().isoformat()}:{int(use_cache)}:{int(top_n)}"
        cached = self._sector_snapshot_cache.get(cache_key)
        if cached:
            return cached

        top: List[Dict[str, Any]] = []
        bottom: List[Dict[str, Any]] = []
        capital_top: List[Dict[str, Any]] = []
        capital_bottom: List[Dict[str, Any]] = []
        try:
            top, bottom = self.fetcher_manager.get_sector_rankings(n=top_n) or ([], [])
        except Exception as exc:
            logger.warning("Stock picker sector rankings unavailable: %s", exc)
        try:
            adapter = getattr(self.fetcher_manager, "_fundamental_adapter", None)
            if adapter is not None:
                capital_payload = adapter.get_capital_flow("000001") or {}
                sector_rankings = capital_payload.get("sector_rankings") or {}
                capital_top = list(sector_rankings.get("top") or [])
                capital_bottom = list(sector_rankings.get("bottom") or [])
        except Exception as exc:
            logger.warning("Stock picker sector capital rankings unavailable: %s", exc)

        top_items: List[Dict[str, Any]] = []
        bottom_items: List[Dict[str, Any]] = []
        top_map: Dict[str, Dict[str, Any]] = {}
        bottom_map: Dict[str, Dict[str, Any]] = {}
        capital_top_items: List[Dict[str, Any]] = []
        capital_bottom_items: List[Dict[str, Any]] = []
        capital_top_map: Dict[str, Dict[str, Any]] = {}
        capital_bottom_map: Dict[str, Dict[str, Any]] = {}

        for idx, item in enumerate(top or [], start=1):
            name = str(item.get("name") or "").strip()
            record = {
                "name": name,
                "change_pct": round(self._to_float(item.get("change_pct")), 2),
                "rank": idx,
            }
            top_items.append(record)
            norm = self._normalize_board_name(name)
            if norm:
                top_map[norm] = record

        for idx, item in enumerate(bottom or [], start=1):
            name = str(item.get("name") or "").strip()
            record = {
                "name": name,
                "change_pct": round(self._to_float(item.get("change_pct")), 2),
                "rank": idx,
            }
            bottom_items.append(record)
            norm = self._normalize_board_name(name)
            if norm:
                bottom_map[norm] = record

        for idx, item in enumerate(capital_top or [], start=1):
            name = str(item.get("name") or "").strip()
            record = {
                "name": name,
                "net_inflow": round(self._to_float(item.get("net_inflow")), 2),
                "rank": idx,
            }
            capital_top_items.append(record)
            norm = self._normalize_board_name(name)
            if norm:
                capital_top_map[norm] = record

        for idx, item in enumerate(capital_bottom or [], start=1):
            name = str(item.get("name") or "").strip()
            record = {
                "name": name,
                "net_inflow": round(self._to_float(item.get("net_inflow")), 2),
                "rank": idx,
            }
            capital_bottom_items.append(record)
            norm = self._normalize_board_name(name)
            if norm:
                capital_bottom_map[norm] = record

        snapshot = {
            "top": top_items,
            "bottom": bottom_items,
            "top_map": top_map,
            "bottom_map": bottom_map,
            "capital_top": capital_top_items,
            "capital_bottom": capital_bottom_items,
            "capital_top_map": capital_top_map,
            "capital_bottom_map": capital_bottom_map,
            "updated_at": datetime.now().isoformat(),
        }
        self._sector_snapshot_cache[cache_key] = snapshot
        return snapshot

    def _get_candidate_board_context(self, code: str) -> List[Dict[str, Any]]:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            return []
        cached = self._board_cache.get(normalized_code)
        if cached is not None:
            return cached
        with self._global_board_cache_lock:
            cached = self._global_board_cache.get(normalized_code)
        if cached is not None:
            self._board_cache[normalized_code] = cached
            return cached
        try:
            boards = self.fetcher_manager.get_belong_boards(normalized_code) or []
        except Exception as exc:
            logger.debug("Stock picker belong boards unavailable for %s: %s", normalized_code, exc)
            boards = []
        self._board_cache[normalized_code] = boards
        with self._global_board_cache_lock:
            self._global_board_cache[normalized_code] = boards
        return boards

    def _attach_candidate_board_contexts(self, candidates: List[Dict[str, Any]]) -> None:
        if not candidates:
            return

        def load_boards(candidate: Dict[str, Any]) -> tuple[int, List[str]]:
            idx = int(candidate.get("_board_attach_index") or 0)
            boards = self._get_candidate_board_context(str(candidate.get("code") or ""))
            board_names = [
                str(item.get("name") or item.get("板块名称") or item.get("industry") or "").strip()
                for item in boards
                if str(item.get("name") or item.get("板块名称") or item.get("industry") or "").strip()
            ]
            return idx, board_names

        for idx, candidate in enumerate(candidates):
            candidate["_board_attach_index"] = idx

        results: Dict[int, List[str]] = {}
        worker_count = min(5, max(1, len(candidates)))
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="picker_board") as executor:
            futures = [executor.submit(load_boards, candidate) for candidate in candidates]
            for future in as_completed(futures):
                try:
                    idx, board_names = future.result()
                except Exception as exc:
                    logger.debug("Stock picker board context worker failed: %s", exc)
                    continue
                results[idx] = board_names

        for idx, candidate in enumerate(candidates):
            candidate.pop("_board_attach_index", None)
            board_names = results.get(idx, [])
            candidate["_theme_board_names"] = board_names
            candidate["_theme_primary_boards"] = self._select_theme_board_names(board_names)

    @staticmethod
    def _select_theme_board_names(board_names: Sequence[str], *, max_items: int = 3) -> List[str]:
        selected: List[str] = []
        for raw_name in board_names:
            name = str(raw_name or "").strip()
            if not name or name in selected:
                continue
            selected.append(name)
            if len(selected) >= max_items:
                break
        return selected

    def _get_stock_main_net_inflow(self, code: str) -> Optional[float]:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            return None
        if normalized_code in self._capital_flow_cache:
            return self._capital_flow_cache[normalized_code]
        adapter = getattr(self.fetcher_manager, "_fundamental_adapter", None)
        if adapter is None:
            self._capital_flow_cache[normalized_code] = None
            return None
        try:
            payload = adapter.get_capital_flow(normalized_code) or {}
            stock_flow = payload.get("stock_flow") or {}
            value = stock_flow.get("main_net_inflow")
            inflow = float(value) if value not in (None, "") else None
        except Exception as exc:
            logger.debug("Stock picker main capital flow unavailable for %s: %s", normalized_code, exc)
            inflow = None
        self._capital_flow_cache[normalized_code] = inflow
        return inflow

    def _build_candidate_board_factor_snapshot(
        self,
        candidates: Sequence[Dict[str, Any]],
        *,
        top_n: int = 12,
    ) -> Dict[str, Any]:
        aggregates: Dict[str, Dict[str, Any]] = {}
        for candidate in candidates:
            metrics = candidate.setdefault("metrics", {})
            board_names = list(candidate.get("_theme_primary_boards") or [])
            if not board_names:
                board_names = self._select_theme_board_names(candidate.get("_theme_board_names") or [])
            if not board_names:
                continue

            pct_chg = self._to_float(metrics.get("pct_chg"))
            amount_ratio = self._to_float(metrics.get("amount_ratio"))
            volume_spike_factor = self._to_float(metrics.get("volume_spike_factor"))
            institution_style = bool(metrics.get("institution_style"))
            weak_money_follow = bool(metrics.get("weak_money_follow"))
            base_score = self._to_float(candidate.get("score"))
            # Per-stock capital-flow calls are slow and can block the whole scan.
            # Use cached/precomputed values only; sector capital rankings remain the
            # primary money-flow signal for stock picking.
            raw_main_net_inflow = metrics.get("stock_main_net_inflow")
            try:
                main_net_inflow = float(raw_main_net_inflow) if raw_main_net_inflow not in (None, "") else None
            except (TypeError, ValueError):
                main_net_inflow = None
            if main_net_inflow is not None:
                metrics["stock_main_net_inflow"] = round(main_net_inflow, 2)

            capital_score = 0.0
            if main_net_inflow is not None:
                capital_score += max(min(main_net_inflow / 10_000_000.0, 18.0), -18.0)
            capital_score += max(amount_ratio - 1.0, 0.0) * 6.0
            capital_score += 3.0 if institution_style else 0.0
            capital_score -= 3.0 if weak_money_follow else 0.0

            momentum_score = (
                max(base_score - 60.0, 0.0) * 0.7
                + max(pct_chg, 0.0) * 2.4
                + max(volume_spike_factor - 1.0, 0.0) * 4.0
                + max(amount_ratio - 1.0, 0.0) * 4.5
                + (4.0 if institution_style else 0.0)
                - (4.0 if weak_money_follow else 0.0)
            )

            for index, board_name in enumerate(board_names):
                normalized = self._normalize_board_name(board_name)
                if not normalized:
                    continue
                weight = 1.0 if index == 0 else (0.72 if index == 1 else 0.56)
                item = aggregates.setdefault(
                    normalized,
                    {
                        "name": board_name,
                        "candidate_count": 0,
                        "momentum_score": 0.0,
                        "capital_score": 0.0,
                        "avg_pct_chg_sum": 0.0,
                        "institution_count": 0,
                    },
                )
                item["candidate_count"] += 1
                item["momentum_score"] += momentum_score * weight
                item["capital_score"] += capital_score * weight
                item["avg_pct_chg_sum"] += pct_chg * weight
                item["institution_count"] += 1 if institution_style else 0

        if not aggregates:
            return {
                "derived_top": [],
                "derived_top_map": {},
                "derived_capital_top": [],
                "derived_capital_top_map": {},
            }

        records: List[Dict[str, Any]] = []
        for normalized, item in aggregates.items():
            candidate_count = int(item["candidate_count"] or 0)
            if candidate_count <= 0:
                continue
            avg_pct_chg = item["avg_pct_chg_sum"] / candidate_count
            institution_ratio = item["institution_count"] / candidate_count
            momentum_score = float(item["momentum_score"] or 0.0) + min(candidate_count, 4) * 1.6
            capital_score = float(item["capital_score"] or 0.0) + institution_ratio * 3.0
            records.append(
                {
                    "name": str(item["name"] or normalized),
                    "normalized_name": normalized,
                    "candidate_count": candidate_count,
                    "avg_pct_chg": round(avg_pct_chg, 2),
                    "institution_ratio": round(institution_ratio, 3),
                    "momentum_score": round(momentum_score, 2),
                    "capital_score": round(capital_score, 2),
                }
            )

        top_records = [
            item
            for item in sorted(records, key=lambda item: (item["momentum_score"], item["capital_score"]), reverse=True)
            if item["avg_pct_chg"] >= 0.8
            and (
                item["candidate_count"] >= 2
                or item["momentum_score"] >= 14.0
                or item["institution_ratio"] >= 0.6
            )
        ][:top_n]
        capital_records = [
            item
            for item in sorted(records, key=lambda item: (item["capital_score"], item["momentum_score"]), reverse=True)
            if item["capital_score"] > 1.0
            and (
                item["candidate_count"] >= 2
                or item["capital_score"] >= 6.0
                or item["institution_ratio"] >= 0.5
            )
        ][:top_n]

        derived_top: List[Dict[str, Any]] = []
        derived_top_map: Dict[str, Dict[str, Any]] = {}
        for index, item in enumerate(top_records, start=1):
            record = {
                "name": item["name"],
                "change_pct": item["avg_pct_chg"],
                "rank": index,
                "candidate_count": item["candidate_count"],
                "institution_ratio": item["institution_ratio"],
                "source": "peer_momentum",
            }
            derived_top.append(record)
            derived_top_map[item["normalized_name"]] = record

        derived_capital_top: List[Dict[str, Any]] = []
        derived_capital_top_map: Dict[str, Dict[str, Any]] = {}
        for index, item in enumerate(capital_records, start=1):
            record = {
                "name": item["name"],
                "net_inflow": item["capital_score"],
                "rank": index,
                "candidate_count": item["candidate_count"],
                "institution_ratio": item["institution_ratio"],
                "source": "peer_capital_proxy",
            }
            derived_capital_top.append(record)
            derived_capital_top_map[item["normalized_name"]] = record

        return {
            "derived_top": derived_top,
            "derived_top_map": derived_top_map,
            "derived_capital_top": derived_capital_top,
            "derived_capital_top_map": derived_capital_top_map,
        }

    def _augment_sector_snapshot_with_candidates(
        self,
        sector_snapshot: Dict[str, Any],
        candidates: Sequence[Dict[str, Any]],
        *,
        top_n: int = 12,
    ) -> Dict[str, Any]:
        snapshot = dict(sector_snapshot or {})
        derived = self._build_candidate_board_factor_snapshot(candidates, top_n=top_n)
        snapshot["derived_top"] = list(derived.get("derived_top") or [])
        snapshot["derived_capital_top"] = list(derived.get("derived_capital_top") or [])
        snapshot["derived_top_map"] = dict(derived.get("derived_top_map") or {})
        snapshot["derived_capital_top_map"] = dict(derived.get("derived_capital_top_map") or {})

        top_map = dict(snapshot.get("top_map") or {})
        capital_top_map = dict(snapshot.get("capital_top_map") or {})
        for key, value in (snapshot["derived_top_map"] or {}).items():
            top_map.setdefault(key, value)
        for key, value in (snapshot["derived_capital_top_map"] or {}).items():
            capital_top_map.setdefault(key, value)
        snapshot["top_map"] = top_map
        snapshot["capital_top_map"] = capital_top_map

        if not snapshot.get("capital_top"):
            snapshot["capital_top"] = list(snapshot["derived_capital_top"])
        return snapshot

    def _apply_theme_strength(
        self,
        *,
        strategy_id: str,
        candidates: List[Dict[str, Any]],
        sector_snapshot: Optional[Dict[str, Any]],
        market_snapshot: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not candidates or not sector_snapshot:
            return

        top_map = sector_snapshot.get("top_map") or {}
        bottom_map = sector_snapshot.get("bottom_map") or {}
        capital_top_map = sector_snapshot.get("capital_top_map") or {}
        capital_bottom_map = sector_snapshot.get("capital_bottom_map") or {}
        derived_top_map = sector_snapshot.get("derived_top_map") or {}
        derived_capital_top_map = sector_snapshot.get("derived_capital_top_map") or {}
        require_hot_theme = strategy_id == "dragon_head"
        market_stats = (market_snapshot or {}).get("stats") or {}
        limit_up_count = self._to_float(market_stats.get("limit_up_count"))
        limit_down_count = self._to_float(market_stats.get("limit_down_count"))
        up_count = self._to_float(market_stats.get("up_count"))
        down_count = self._to_float(market_stats.get("down_count"))
        breadth = up_count / max(up_count + down_count, 1.0) if (up_count + down_count) > 0 else 0.5
        risk_off_market = breadth < 0.46 or limit_down_count >= max(18.0, limit_up_count * 0.7)
        hot_money_market = breadth > 0.56 and limit_up_count >= max(22.0, limit_down_count * 1.6)

        filtered: List[Dict[str, Any]] = []
        for candidate in candidates:
            stock_name = str(candidate.get("name") or "").strip()
            board_names = list(candidate.get("_theme_board_names") or [])
            primary_boards = list(candidate.get("_theme_primary_boards") or self._select_theme_board_names(board_names))
            if not board_names and stock_name:
                board_names = [stock_name]
                primary_boards = [stock_name]
            top_hit = self._match_sector_record(primary_boards or board_names, top_map)
            bottom_hit = self._match_sector_record(board_names, bottom_map)
            capital_top_hit = self._match_sector_record(primary_boards or board_names, capital_top_map)
            capital_bottom_hit = self._match_sector_record(board_names, capital_bottom_map)
            peer_top_hit = self._match_sector_record(primary_boards or board_names, derived_top_map)
            peer_capital_hit = self._match_sector_record(primary_boards or board_names, derived_capital_top_map)
            metrics = candidate.setdefault("metrics", {})
            pct_chg = self._to_float(metrics.get("pct_chg"))
            amount_ratio = self._to_float(metrics.get("amount_ratio"))
            atr_pct = self._to_float(metrics.get("atr_pct"))
            institution_style = bool(metrics.get("institution_style"))
            weak_money_follow = bool(metrics.get("weak_money_follow"))
            setup_type = str(candidate.get("setup_type") or "")
            theme_delta = 0.0
            theme_reasons: List[str] = []

            if top_hit is not None and str(top_hit.get("source") or "") != "peer_momentum":
                top_rank = int(top_hit.get("rank") or 99)
                if top_rank <= 3:
                    theme_delta += 10
                elif top_rank <= 5:
                    theme_delta += 7
                else:
                    theme_delta += 4
                rel_strength = pct_chg - self._to_float(top_hit.get("change_pct"))
                if rel_strength >= 2.0:
                    theme_delta += 6
                    theme_reasons.append(f"题材 {top_hit['name']} 处于领涨前列，个股强于板块 {rel_strength:.2f}%")
                elif rel_strength >= 0.5:
                    theme_delta += 3
                    theme_reasons.append(f"题材 {top_hit['name']} 处于领涨前列，个股强于板块 {rel_strength:.2f}%")
                else:
                    theme_reasons.append(f"所属题材 {top_hit['name']} 位于强势区")

            if bottom_hit is not None:
                bottom_rank = int(bottom_hit.get("rank") or 99)
                if bottom_rank <= 3:
                    theme_delta -= 10
                elif bottom_rank <= 5:
                    theme_delta -= 6
                else:
                    theme_delta -= 3
                theme_reasons.append(f"所属题材 {bottom_hit['name']} 处于弱势区")

            if peer_top_hit is not None and str(peer_top_hit.get("source") or "") == "peer_momentum":
                peer_rank = int(peer_top_hit.get("rank") or 99)
                peer_count = int(peer_top_hit.get("candidate_count") or 0)
                peer_avg_pct = self._to_float(peer_top_hit.get("change_pct"))
                if peer_rank <= 5 and (peer_count >= 2 or peer_avg_pct >= 1.2):
                    if peer_rank <= 3:
                        theme_delta += 8
                    else:
                        theme_delta += 5
                    if peer_count >= 2:
                        theme_delta += 3
                    theme_reasons.append(
                        f"同题材候选共振较强，{peer_top_hit['name']} 在候选池热度排名靠前"
                    )

            if capital_top_hit is not None:
                cap_rank = int(capital_top_hit.get("rank") or 99)
                if cap_rank <= 3:
                    theme_delta += 12
                elif cap_rank <= 5:
                    theme_delta += 8
                else:
                    theme_delta += 5
                theme_reasons.append(f"板块 {capital_top_hit['name']} 主力资金净流入居前")

            if capital_bottom_hit is not None:
                cap_rank = int(capital_bottom_hit.get("rank") or 99)
                if cap_rank <= 3:
                    theme_delta -= 8
                elif cap_rank <= 5:
                    theme_delta -= 5
                else:
                    theme_delta -= 3
                theme_reasons.append(f"板块 {capital_bottom_hit['name']} 资金承接偏弱")

            if capital_top_hit is None and peer_capital_hit is not None:
                peer_cap_rank = int(peer_capital_hit.get("rank") or 99)
                peer_cap_count = int(peer_capital_hit.get("candidate_count") or 0)
                peer_cap_strength = self._to_float(peer_capital_hit.get("net_inflow"))
                if peer_cap_rank <= 5 and peer_cap_strength > 1.0 and (
                    peer_cap_count >= 2
                    or peer_cap_strength >= 6.0
                    or self._to_float(peer_capital_hit.get("institution_ratio")) >= 0.5
                ):
                    if peer_cap_rank <= 3:
                        theme_delta += 10
                    else:
                        theme_delta += 7
                    if peer_cap_count >= 2:
                        theme_delta += 2
                    capital_top_hit = peer_capital_hit
                    theme_reasons.append(
                        f"板块 {peer_capital_hit['name']} 在候选池主力资金代理强度居前"
                    )

            if top_hit is not None and capital_top_hit is not None:
                if self._normalize_board_name(top_hit.get("name")) == self._normalize_board_name(capital_top_hit.get("name")):
                    theme_delta += 6
                    theme_reasons.append("板块涨幅强度与资金流入形成共振")
            elif top_hit is not None or capital_top_hit is not None:
                theme_delta += 3
            else:
                theme_delta -= 12
                theme_reasons.append("所属板块未进入强势或资金流入名单，板块抬升效应不足")

            if institution_style:
                theme_delta += 5
                theme_reasons.append("量价结构偏机构主导，不是纯情绪脉冲")
            elif weak_money_follow:
                theme_delta -= 5
                theme_reasons.append("短线跟风资金主导迹象偏重，承接稳定性一般")

            if amount_ratio >= 1.25 and pct_chg >= 2.0:
                theme_delta += 4
                theme_reasons.append("当日资金活跃度较高，短线跟随价值提升")
            elif amount_ratio <= 0.85 and pct_chg < 1.0:
                theme_delta -= 3

            if hot_money_market and top_hit is not None and int(top_hit.get("rank") or 99) <= 8:
                theme_delta += 4
                theme_reasons.append("短线情绪偏热，强题材更容易走出跟随机会")
            if hot_money_market and capital_top_hit is not None and int(capital_top_hit.get("rank") or 99) <= 8:
                theme_delta += 3
                theme_reasons.append("短线情绪与板块资金流入同向，跟随性更强")
            if risk_off_market and setup_type == "breakout":
                theme_delta -= 6
                theme_reasons.append("短线情绪偏弱，突破型交易额外降权")

            if atr_pct >= 4.8 and weak_money_follow:
                theme_delta -= 4
                theme_reasons.append("高波动叠加跟风特征，尾部回撤风险偏大")

            if strategy_id == "mainboard_swing_master" and top_hit is None and capital_top_hit is None and not board_names:
                theme_reasons.append("未拿到明确板块归属，板块强度按中性处理")

            if strategy_id == "dragon_head":
                hot_theme_pass = (
                    top_hit is not None
                    and capital_top_hit is not None
                    and int(top_hit.get("rank") or 99) <= 5
                    and int(capital_top_hit.get("rank") or 99) <= 5
                    and pct_chg >= 2.5
                )
                if hot_theme_pass:
                    theme_delta += 8
                    theme_reasons.append("符合龙头策略的板块强度与资金共振要求")
                elif require_hot_theme:
                    continue

            if strategy_id == "shanliu_theme_flow":
                has_hot_theme = top_hit is not None or peer_top_hit is not None
                has_capital_theme = capital_top_hit is not None or peer_capital_hit is not None
                if has_hot_theme and has_capital_theme:
                    theme_delta += 16
                    theme_reasons.append("山流要求的题材强度与资金流入同时命中，作为主导加分项")
                elif has_hot_theme or has_capital_theme:
                    theme_delta += 7
                    theme_reasons.append("山流题材或资金因子单侧命中，保留但降低确定性")
                else:
                    theme_delta -= 18
                    theme_reasons.append("山流策略未命中强题材/强资金主线，显著降权")
                if hot_money_market and has_hot_theme:
                    theme_delta += 6
                    theme_reasons.append("短线情绪与题材热度共振，适合跟随")
                if risk_off_market and not has_capital_theme:
                    theme_delta -= 6
                    theme_reasons.append("弱情绪下未见资金主线承接，降低山流优先级")

            candidate["score"] = round(float(candidate.get("score") or 0.0) + theme_delta, 2)
            metrics["theme_boards"] = board_names[:6]
            metrics["theme_primary_boards"] = primary_boards[:3]
            metrics["theme_score_delta"] = round(theme_delta, 2)
            metrics["theme_hot_sector"] = top_hit
            metrics["theme_cold_sector"] = bottom_hit
            metrics["theme_hot_capital_sector"] = capital_top_hit
            metrics["theme_cold_capital_sector"] = capital_bottom_hit
            metrics["theme_peer_hot_sector"] = peer_top_hit
            metrics["theme_peer_capital_sector"] = peer_capital_hit
            metrics["theme_strength_label"] = "hot" if theme_delta > 0 else ("cold" if theme_delta < 0 else "neutral")
            metrics["hot_money_market"] = hot_money_market
            metrics["risk_off_market"] = risk_off_market
            reasons = candidate.setdefault("reasons", [])
            for reason in theme_reasons[:2]:
                if reason not in reasons:
                    reasons.append(reason)
            filtered.append(candidate)

        candidates[:] = filtered

    def _collect_candidate_intel(
        self,
        *,
        code: str,
        name: str,
        scan_date: Optional[date] = None,
        query_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        effective_scan_date = scan_date or date.today()
        cached_results = self._load_cached_candidate_intel(
            code=code,
            name=name,
            scan_date=effective_scan_date,
        )
        if cached_results:
            news_context = (
                self.search_service.format_intel_report(cached_results, name)
                if self.search_service is not None
                else self._format_cached_intel_report(cached_results, name)
            )
            return {
                "intel_results": cached_results,
                "news_context": news_context,
                "news_score": self._score_intel_payload(cached_results),
                "from_cache": True,
            }

        if self.search_service is None or not getattr(self.search_service, "is_available", False):
            return {"news_context": None, "news_score": 50.0, "from_cache": False}

        executor: Optional[ThreadPoolExecutor] = None
        try:
            executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="picker_intel")
            future = executor.submit(self.search_service.search_comprehensive_intel, code, name, 3)
            intel_results = future.result(timeout=25)
            executor.shutdown(wait=False, cancel_futures=True)
            self._save_candidate_intel_cache(
                code=code,
                name=name,
                intel_results=intel_results,
                query_id=query_id,
            )
            news_context = self.search_service.format_intel_report(intel_results, name)
            news_score = self._score_intel_payload(intel_results)
            return {
                "intel_results": intel_results,
                "news_context": news_context,
                "news_score": news_score,
                "from_cache": False,
            }
        except TimeoutError:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            logger.warning("Stock picker intel collection timed out for %s", code)
            return {"news_context": None, "news_score": 50.0, "from_cache": False}
        except Exception as exc:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            logger.warning("Stock picker intel collection failed for %s: %s", code, exc)
            return {"news_context": None, "news_score": 50.0, "from_cache": False}

    def _load_cached_candidate_intel(
        self,
        *,
        code: str,
        name: str,
        scan_date: date,
    ) -> Dict[str, SearchResponse]:
        window_start = datetime.combine(scan_date, dt_time.min)
        window_end = window_start + timedelta(days=1)
        grouped: Dict[str, List[NewsIntel]] = {}
        with self.db.get_session() as session:
            rows = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= window_start,
                        NewsIntel.fetched_at < window_end,
                    )
                )
                .order_by(NewsIntel.dimension.asc(), desc(NewsIntel.published_date), desc(NewsIntel.fetched_at))
            ).scalars().all()
        for row in rows:
            grouped.setdefault(str(row.dimension or "latest_news"), [])
            if len(grouped[str(row.dimension or "latest_news")]) >= 5:
                continue
            grouped[str(row.dimension or "latest_news")].append(row)

        responses: Dict[str, SearchResponse] = {}
        for dimension, items in grouped.items():
            if not items:
                continue
            responses[dimension] = SearchResponse(
                query=items[0].query or f"{name} {code}",
                provider=items[0].provider or "cache",
                success=True,
                search_time=0.0,
                results=[
                    SearchResult(
                        title=item.title or "",
                        snippet=item.snippet or "",
                        url=item.url or "",
                        source=item.source or (item.provider or "cache"),
                        published_date=item.published_date.isoformat() if item.published_date else None,
                    )
                    for item in items
                ],
            )
        return responses

    def _save_candidate_intel_cache(
        self,
        *,
        code: str,
        name: str,
        intel_results: Dict[str, SearchResponse],
        query_id: Optional[str],
    ) -> None:
        for dimension, response in intel_results.items():
            if not response.success or not response.results:
                continue
            try:
                self.db.save_news_intel(
                    code=code,
                    name=name,
                    dimension=dimension,
                    query=response.query,
                    response=response,
                    query_context={
                        "query_id": query_id or "",
                        "query_source": "system",
                        "requester_platform": "picker",
                        "requester_query": response.query,
                    },
                )
            except Exception as exc:
                logger.warning("Stock picker failed to cache news intel for %s/%s: %s", code, dimension, exc)

    @staticmethod
    def _format_cached_intel_report(intel_results: Dict[str, SearchResponse], stock_name: str) -> str:
        lines = [f"## {stock_name} 消息面摘要", ""]
        for dimension, response in intel_results.items():
            if not response.results:
                continue
            lines.append(f"### {dimension}")
            for item in response.results[:3]:
                lines.append(f"- {item.title}")
            lines.append("")
        return "\n".join(lines).strip()


def _picker_format_price(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    if numeric <= 0:
        return "-"
    return f"{numeric:.2f}"


def _picker_format_pct(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{numeric:.2f}%"


def _picker_build_strategy_catalog() -> List[Dict[str, Any]]:
    skills = load_skills_from_directory(_BUILTIN_STRATEGY_DIR)
    custom_strategies = [
        {
            "strategy_id": "swing_trend_follow",
            "name": "波段趋势",
            "description": "只在沪深主板非 ST 股票池内选股，重点识别 5/10/20/30 日线多头向上、均线间隔有序、趋势稳定的波段跟随标的。",
            "skill_id": "swing_trend_follow",
            "category": "swing",
        },
        {
            "strategy_id": "main_force_breakout",
            "name": "主力突破",
            "description": "只在沪深主板非 ST 股票池内选股，寻找曾经连板后回调、5 日线放量上穿 20 日线、再缩量回踩 20 日线的主力二波候选。",
            "skill_id": "main_force_breakout",
            "category": "breakout",
        },
        {
            "strategy_id": "shanliu_theme_flow",
            "name": "山流",
            "description": "只在沪深主板非 ST 股票池内选股，优先识别强题材合力、机构主导与游资接力共振的短线情绪周期启动点。",
            "skill_id": "shanliu_theme_flow",
            "category": "theme",
        },
    ]
    catalog: List[Dict[str, Any]] = []
    seen = set()
    for item in custom_strategies:
        preset = _PICKER_STRATEGY_PRESETS[item["strategy_id"]]
        catalog.append(
            {
                **item,
                "params": dict(preset["params"]),
                "priority": int(preset["priority"]),
            }
        )
        seen.add(item["strategy_id"])

    for skill in skills:
        strategy_id = "mainboard_swing_master" if skill.name == "swing_after_close_picker" else skill.name
        if strategy_id == "mainboard_swing_master":
            continue
        if strategy_id in seen:
            continue
        preset = _PICKER_STRATEGY_PRESETS.get(strategy_id)
        if not preset:
            continue
        catalog.append(
            {
                "strategy_id": strategy_id,
                "name": skill.display_name or strategy_id,
                "description": skill.description or strategy_id,
                "skill_id": skill.name,
                "category": skill.category or "trend",
                "params": dict(preset.get("params") or {}),
                "priority": int(preset.get("priority") or 999),
            }
        )
        seen.add(strategy_id)

    catalog.sort(key=lambda item: (int(item.get("priority") or 999), str(item["strategy_id"])))
    return [
        {
            "strategy_id": item["strategy_id"],
            "name": item["name"],
            "description": item["description"],
            "skill_id": item["skill_id"],
            "category": item["category"],
            "params": item["params"],
        }
        for item in catalog
    ]


def _picker_strategy_catalog(self: StockPickerService) -> List[Dict[str, Any]]:
    return _picker_build_strategy_catalog()


def _picker_list_strategies() -> List[Dict[str, Any]]:
    return _picker_build_strategy_catalog()


def _picker_to_float(self: StockPickerService, value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _picker_score_intel_payload(self: StockPickerService, intel_results: Dict[str, SearchResponse]) -> float:
    positive_keywords = ("订单", "中标", "预增", "增长", "回购", "增持", "景气", "上调", "催化", "突破")
    negative_keywords = ("减持", "立案", "处罚", "亏损", "诉讼", "风险", "暴雷", "问询", "质押", "违约")
    score = 50.0
    for response in intel_results.values():
        if not response.success:
            continue
        for item in response.results[:3]:
            text = f"{item.title} {item.snippet}"
            score += sum(2.5 for keyword in positive_keywords if keyword in text)
            score -= sum(4.0 for keyword in negative_keywords if keyword in text)
    return max(0.0, min(score, 100.0))


def _picker_ensure_candidate_metrics(self: StockPickerService, candidate: Dict[str, Any]) -> Dict[str, Any]:
    metrics = dict(candidate.get("metrics") or {})
    required_keys = {"close", "ma5", "ma10", "ma20", "ma30", "ma60", "support_price", "resistance_price", "atr14"}
    if required_keys.issubset({key for key, value in metrics.items() if value not in (None, "")}):
        return metrics

    code = str(candidate.get("code") or "").strip()
    scan_date_raw = candidate.get("scan_date")
    if not code or not scan_date_raw:
        return metrics
    try:
        scan_date = scan_date_raw if isinstance(scan_date_raw, date) else datetime.fromisoformat(str(scan_date_raw)).date()
    except ValueError:
        return metrics

    bars = self._load_history_frames([code], scan_date, lookback_days=90).get(code)
    if bars is None or bars.empty:
        return metrics

    last = bars.iloc[-1]
    recent10 = bars.tail(10)
    recent20 = bars.tail(20)
    prev_close_series = bars["close"].shift(1)
    tr = pd.concat(
        [
            bars["high"] - bars["low"],
            (bars["high"] - prev_close_series).abs(),
            (bars["low"] - prev_close_series).abs(),
        ],
        axis=1,
    ).max(axis=1)
    metrics.update(
        {
            "close": round(self._to_float(last.get("close")), 2),
            "open": round(self._to_float(last.get("open")), 2),
            "high": round(self._to_float(last.get("high")), 2),
            "low": round(self._to_float(last.get("low")), 2),
            "prev_close": round(self._to_float(bars.iloc[-2]["close"]) if len(bars) > 1 else last.get("close"), 2),
            "ma5": round(self._to_float(last.get("ma5")), 2),
            "ma10": round(self._to_float(last.get("ma10")), 2),
            "ma20": round(self._to_float(last.get("ma20")), 2),
            "ma30": round(self._to_float(last.get("ma30")), 2),
            "ma60": round(self._to_float(last.get("ma60")), 2),
            "recent10_high": round(self._to_float(recent10["high"].max()), 2),
            "recent10_low": round(self._to_float(recent10["low"].min()), 2),
            "recent20_high": round(self._to_float(recent20["high"].max()), 2),
            "recent20_low": round(self._to_float(recent20["low"].min()), 2),
            "return_5d_pct": round(((self._to_float(last.get("close")) / max(self._to_float(bars.iloc[-6]["close"]) if len(bars) > 5 else self._to_float(last.get("close")), 0.01)) - 1) * 100, 2) if len(bars) > 5 else 0.0,
            "return_10d_pct": round(((self._to_float(last.get("close")) / max(self._to_float(bars.iloc[-11]["close"]) if len(bars) > 10 else self._to_float(last.get("close")), 0.01)) - 1) * 100, 2) if len(bars) > 10 else 0.0,
            "avg_amp_10_pct": round((((recent10["high"] - recent10["low"]) / recent10["close"].replace(0, math.nan)) * 100).mean(), 2),
            "atr14": round(self._to_float(tr.tail(14).mean()), 2),
            "support_price": round(max(min(self._to_float(last.get("ma5")), self._to_float(last.get("ma10")), self._to_float(last.get("ma20"))), self._to_float(recent10["low"].min())), 2),
            "resistance_price": round(max(self._to_float(recent10["high"].max()), self._to_float(recent20["high"].max())), 2),
            "breakout_trigger_price": round(max(self._to_float(recent10["high"].max()) * 1.002, self._to_float(last.get("close")) * 1.005), 2),
        }
    )
    candidate["metrics"] = metrics
    return metrics


def _picker_derive_trade_levels(self: StockPickerService, candidate: Dict[str, Any]) -> Dict[str, float]:
    metrics = _picker_ensure_candidate_metrics(self, candidate)
    close_price = max(self._to_float(metrics.get("close")), 0.01)
    ma5 = self._to_float(metrics.get("ma5"))
    ma10 = self._to_float(metrics.get("ma10"))
    ma20 = self._to_float(metrics.get("ma20"))
    atr14 = max(self._to_float(metrics.get("atr14")), close_price * 0.012, 0.05)
    support_price = max(self._to_float(metrics.get("support_price")), close_price * 0.96, 0.01)
    resistance_price = max(self._to_float(metrics.get("resistance_price")), close_price * 1.015)
    breakout_trigger = max(self._to_float(metrics.get("breakout_trigger_price")), resistance_price * 1.001, close_price)
    setup_type = str(candidate.get("setup_type") or "pullback").strip().lower()

    if setup_type == "breakout":
        buy_low = max(breakout_trigger * 0.998, close_price * 1.002)
        buy_high = buy_low + max(atr14 * 0.35, close_price * 0.004)
    else:
        anchor = ma5 if ma5 > 0 else close_price
        buy_low = max(min(anchor * 0.998, close_price * 0.997), support_price)
        buy_high = min(max(anchor * 1.005, close_price * 1.003), resistance_price * 0.998)

    stop_loss = self._to_float(candidate.get("stop_loss")) or max(min(ma10 or close_price, ma20 or close_price), support_price * 0.995)
    take_profit = self._to_float(candidate.get("take_profit")) or max(resistance_price * 1.03, close_price * 1.08)
    reduce_price = min(max(close_price + atr14 * 1.5, resistance_price * 0.995), take_profit * 0.985)
    abandon_price = min(stop_loss, support_price * 0.995)

    buy_low = max(buy_low, 0.01)
    buy_high = max(buy_high, buy_low)
    breakout_buy = max(breakout_trigger, buy_high, 0.01)
    reduce_price = max(reduce_price, breakout_buy)
    take_profit = max(take_profit, reduce_price)
    stop_loss = max(stop_loss, 0.01)
    abandon_price = max(min(abandon_price, stop_loss), 0.01)

    return {
        "buy_low": round(buy_low, 2),
        "buy_high": round(buy_high, 2),
        "breakout_buy": round(breakout_buy, 2),
        "reduce_price": round(reduce_price, 2),
        "take_profit": round(take_profit, 2),
        "stop_loss": round(stop_loss, 2),
        "abandon_price": round(abandon_price, 2),
    }


def _picker_build_stock_profile(self: StockPickerService, candidate: Dict[str, Any]) -> str:
    metrics = _picker_ensure_candidate_metrics(self, candidate)
    setup_label = "趋势回踩" if str(candidate.get("setup_type") or "").lower() == "pullback" else "突破启动"
    return (
        f"{setup_label}结构，收盘 {self._format_price(metrics.get('close'))}，"
        f"MA5/10/20/30/60="
        f"{self._format_price(metrics.get('ma5'))}/"
        f"{self._format_price(metrics.get('ma10'))}/"
        f"{self._format_price(metrics.get('ma20'))}/"
        f"{self._format_price(metrics.get('ma30'))}/"
        f"{self._format_price(metrics.get('ma60'))}；"
        f"近 8 日最大倍量 {metrics.get('volume_spike_factor', '-')} 倍，"
        f"5 日涨幅 {_picker_format_pct(metrics.get('return_5d_pct'))}，"
        f"10 日涨幅 {_picker_format_pct(metrics.get('return_10d_pct'))}，"
        f"10 日均振幅 {_picker_format_pct(metrics.get('avg_amp_10_pct'))}，"
        f"短支撑 {self._format_price(metrics.get('support_price'))}，"
        f"短压 {self._format_price(metrics.get('resistance_price'))}，"
        f"ATR14 {self._format_price(metrics.get('atr14'))}。"
    )


def _picker_compact_review_text(
    self: StockPickerService,
    *,
    code: str,
    name: str,
    review_text: Optional[str],
    max_chars: int = 420,
) -> str:
    raw = (review_text or "").strip()
    if not raw:
        return "未检索到高相关度新闻，按中性处理。"
    keywords = {str(code or "").strip(), str(name or "").strip()}
    kept: List[str] = []
    for line in raw.splitlines():
        clean = line.strip(" -\t")
        if not clean:
            continue
        if any(keyword and keyword in clean for keyword in keywords):
            kept.append(clean)
        elif clean.startswith(("公司公告", "最新消息", "机构分析", "风险排查")):
            kept.append(clean)
        if sum(len(item) for item in kept) >= max_chars:
            break
    compact = "\n".join(kept).strip() or raw[:max_chars].strip()
    return compact[:max_chars]


def _picker_is_action_plan_usable(text: Optional[str]) -> bool:
    content = (text or "").strip()
    if len(content) < 180:
        return False
    required_sections = ("一、个股画像", "二、入场前提", "三、三种开盘预案", "四、挂单计划", "五、持仓与卖出", "六、风险点")
    if not all(section in content for section in required_sections):
        return False
    required_prices = ("低吸挂单价", "接力挂单价", "突破追价", "首次减仓价", "止盈价", "止损价", "放弃价")
    return all(label in content for label in required_prices)


def _picker_repair_action_plan(
    self: StockPickerService,
    *,
    candidate: Dict[str, Any],
    strategy: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    text: Optional[str],
) -> str:
    content = (text or "").strip()
    if not content:
        return ""

    trade_levels = self._derive_trade_levels(candidate)
    stock_profile = self._build_stock_profile(candidate)
    reasons = "；".join(str(item) for item in (candidate.get("reasons") or [])[:4]) or "量价结构满足策略条件"

    trade_block = (
        "四、挂单计划\n"
        f"- 低吸挂单价：{self._format_price(trade_levels['buy_low'])}\n"
        f"- 接力挂单价：{self._format_price(trade_levels['buy_high'])}\n"
        f"- 突破追价：{self._format_price(trade_levels['breakout_buy'])}\n"
        f"- 首次减仓价：{self._format_price(trade_levels['reduce_price'])}\n"
        f"- 止盈价：{self._format_price(trade_levels['take_profit'])}\n"
        f"- 止损价：{self._format_price(trade_levels['stop_loss'])}\n"
        f"- 放弃价：{self._format_price(trade_levels['abandon_price'])}\n"
    )

    fallback_sections = {
        "一、个股画像": (
            "一、个股画像\n"
            f"- {stock_profile}\n"
            f"- 量化结论：{candidate.get('analysis_summary')}\n"
            f"- 触发原因：{reasons}\n"
        ),
        "二、入场前提": (
            "二、入场前提\n"
            f"- 先确认股价稳住 {self._format_price(trade_levels['buy_low'])} 上方，跌破 {self._format_price(trade_levels['abandon_price'])} 当天放弃。\n"
            f"- 板块和指数若同步转弱，只保留轻仓试错。\n"
        ),
        "三、三种开盘预案": (
            "三、三种开盘预案\n"
            f"- 平开或小低开：观察 {self._format_price(trade_levels['buy_low'])}-{self._format_price(trade_levels['buy_high'])} 承接，缩量回踩可低吸。\n"
            f"- 小幅高开：不追高，等回踩不破 {self._format_price(trade_levels['buy_high'])} 再考虑接力。\n"
            f"- 强势突破：只有放量站上 {self._format_price(trade_levels['breakout_buy'])} 才允许追价。\n"
        ),
        "五、持仓与卖出": (
            "五、持仓与卖出\n"
            f"- 先看 {self._format_price(trade_levels['reduce_price'])} 一带是否能实现首次减仓，接近 {self._format_price(trade_levels['take_profit'])} 再分批兑现。\n"
            f"- 跌破 {self._format_price(trade_levels['stop_loss'])} 直接止损，不拖延。\n"
        ),
        "六、风险点": (
            "六、风险点\n"
            f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}。\n"
            f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}。\n"
            "- 若新闻突发利空或板块龙头转弱，优先降仓位。\n"
        ),
    }

    if "四、挂单计划" in content:
        content = re.sub(
            r"四、挂单计划[\s\S]*?(?=\n五、持仓与卖出|\Z)",
            trade_block.strip(),
            content,
            flags=re.MULTILINE,
        )
    else:
        if "五、持仓与卖出" in content:
            content = content.replace("五、持仓与卖出", f"{trade_block}\n五、持仓与卖出", 1)
        else:
            content = f"{content}\n\n{trade_block}"

    for section_name, section_text in fallback_sections.items():
        if section_name not in content:
            content = f"{content}\n\n{section_text.strip()}"

    return content.strip()


def _picker_build_template_action_plan(
    self: StockPickerService,
    *,
    candidate: Dict[str, Any],
    strategy: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    review_text: Optional[str],
) -> str:
    trade_levels = self._derive_trade_levels(candidate)
    stock_profile = self._build_stock_profile(candidate)
    reasons = "；".join(str(item) for item in (candidate.get("reasons") or [])[:6]) or "量价结构满足策略条件"
    return (
        f"### {candidate['name']}（{candidate['code']}）次日操作手册\n\n"
        f"**核心策略：** {strategy['name']} | **形态：** {candidate.get('setup_type')} | **评分：** {candidate.get('score')}\n\n"
        f"**一、个股画像**\n"
        f"- {stock_profile}\n"
        f"- 量化结论：{candidate.get('analysis_summary')}\n"
        f"- 触发原因：{reasons}\n"
        f"- 市场环境：A股 {market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}；美股 {us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}\n"
        f"- 消息面：{review_text or '当前没有额外高置信新闻，先按中性处理。'}\n\n"
        f"**二、入场前提**\n"
        f"- 开盘后先看是否稳住 {self._format_price(trade_levels['buy_low'])} 上方，弱于该价位不急于挂单。\n"
        f"- 若竞价或开盘 15 分钟内直接跌破 {self._format_price(trade_levels['abandon_price'])}，当天放弃。\n\n"
        f"**三、三种开盘预案**\n"
        f"- 平开或小低开：优先观察 {self._format_price(trade_levels['buy_low'])}-{self._format_price(trade_levels['buy_high'])} 的承接，缩量回踩可低吸。\n"
        f"- 小幅高开：高开后不破 {self._format_price(trade_levels['buy_high'])}，可等二次站稳再挂单。\n"
        f"- 强势突破：只有放量站上 {self._format_price(trade_levels['breakout_buy'])} 才允许追价，而且只做轻仓。\n\n"
        f"**四、挂单计划**\n"
        f"- 低吸挂单价：{self._format_price(trade_levels['buy_low'])}\n"
        f"- 接力挂单价：{self._format_price(trade_levels['buy_high'])}\n"
        f"- 突破追价：{self._format_price(trade_levels['breakout_buy'])}\n"
        f"- 首次减仓价：{self._format_price(trade_levels['reduce_price'])}\n"
        f"- 止盈价：{self._format_price(trade_levels['take_profit'])}\n"
        f"- 止损价：{self._format_price(trade_levels['stop_loss'])}\n"
        f"- 放弃价：{self._format_price(trade_levels['abandon_price'])}\n\n"
        f"**五、持仓与卖出**\n"
        f"- 计划持有 5-10 个交易日，2-3 天内站不上 {self._format_price(trade_levels['reduce_price'])} 就降低预期。\n"
        f"- 触及 {self._format_price(trade_levels['reduce_price'])} 可先减仓锁定利润，接近 {self._format_price(trade_levels['take_profit'])} 再分批兑现。\n"
        f"- 放量跌破 {self._format_price(trade_levels['stop_loss'])} 直接执行止损，不等反抽。\n\n"
        f"**六、风险点**\n"
        f"- 若板块龙头转弱、指数情绪急速回落、或新闻出现明确利空，优先降仓位。\n"
    )


def _picker_call_openai_compatible_chat_completion(
    self: StockPickerService,
    *,
    model: str,
    prompt: str,
    system_prompt: str,
    max_tokens: int,
    temperature: float,
    timeout_seconds: float = 75.0,
) -> str:
    from openai import OpenAI

    transport = self.analyzer._resolve_openai_transport_params(model, self.config)
    api_key = transport.get("api_key")
    api_base = transport.get("api_base")
    if not api_key or not api_base:
        raise ValueError(f"{model} missing api_key/api_base for OpenAI-compatible chat call")

    client = OpenAI(api_key=api_key, base_url=api_base, timeout=timeout_seconds)
    response = client.chat.completions.create(
        model=model.split("/", 1)[1] if "/" in model else model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    message = None
    if getattr(response, "choices", None):
        choice = response.choices[0]
        message = getattr(choice, "message", None)
    content = getattr(message, "content", None) if message is not None else None
    if not content:
        raise ValueError(f"{model} returned empty chat completion")
    return str(content).strip()


def _picker_build_action_plan(
    self: StockPickerService,
    *,
    candidate: Dict[str, Any],
    strategy: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    review_payload: Optional[Dict[str, Any]],
    prefer_fallback_only: bool = False,
) -> tuple[str, Optional[str]]:
    review_text = review_payload.get("news_context") if isinstance(review_payload, dict) else None
    picker_system_prompt = (
        "你是A股收盘后交易执行助手。"
        "必须直接输出完整中文操作手册，不要写“好的”“下面开始分析”“作为AI”等前置废话。"
        "必须给出具体挂单价格，并严格使用指定标题。"
    )
    compact_review_text = self._picker_compact_review_text(
        code=str(candidate.get("code") or ""),
        name=str(candidate.get("name") or ""),
        review_text=review_text,
    )
    trade_levels = self._derive_trade_levels(candidate)
    stock_profile = self._build_stock_profile(candidate)
    if not getattr(self.analyzer, "is_available", lambda: False)():
        raise RuntimeError("LLM analyzer is unavailable")

    prompt = (
        "你是 A 股收盘后波段交易助手。请根据下面这只股票的量化数据、均线形态、波动特征、支撑压力位和新闻摘要，"
        "生成一份面向第二天的精简操作手册。必须说清楚挂什么价格买、挂什么价格卖，并且要体现这只股票自己的历史特征。"
        "\n\n输出要求：\n"
        "1. 只能用中文。\n"
        "2. 必须输出以下标题：一、个股画像；二、入场前提；三、三种开盘预案；四、挂单计划；五、持仓与卖出；六、风险点。\n"
        "3. 挂单计划里必须出现具体价格：低吸挂单价、接力挂单价、突破追价、首次减仓价、止盈价、止损价、放弃价。\n"
        "4. 不能写成泛化模板，必须解释该股的均线结构、量能、波动率、支撑压力为何对应这些价格。\n"
        "5. 不能编造未给出的基本面或新闻。\n\n"
        "6. 每个标题下最多写 3 个要点，整份手册尽量控制在 700 字左右，避免空话和重复。\n\n"
        f"策略：{strategy['name']}\n"
        f"股票：{candidate['name']} ({candidate['code']})\n"
        f"形态：{candidate.get('setup_type')}\n"
        f"评分：{candidate.get('score')}\n"
        f"量化结论：{candidate.get('analysis_summary')}\n"
        f"触发原因：{'；'.join(str(item) for item in (candidate.get('reasons') or [])[:6])}\n"
        f"个股画像：{stock_profile}\n"
        f"A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}\n"
        f"美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}\n"
        f"价格参考：低吸 {trade_levels['buy_low']}，接力 {trade_levels['buy_high']}，突破 {trade_levels['breakout_buy']}，"
        f"减仓 {trade_levels['reduce_price']}，止盈 {trade_levels['take_profit']}，止损 {trade_levels['stop_loss']}，放弃 {trade_levels['abandon_price']}\n"
        f"消息摘要：{compact_review_text}"
    )
    retry_prompt = (
        "按下面固定骨架重写，禁止省略标题，禁止解释过程，禁止写套话。\n"
        "输出骨架：\n"
        "一、个股画像\n"
        "二、入场前提\n"
        "三、三种开盘预案\n"
        "四、挂单计划\n"
        "五、持仓与卖出\n"
        "六、风险点\n\n"
        "挂单计划必须逐行写出：\n"
        f"低吸挂单价：{trade_levels['buy_low']}\n"
        f"接力挂单价：{trade_levels['buy_high']}\n"
        f"突破追价：{trade_levels['breakout_buy']}\n"
        f"首次减仓价：{trade_levels['reduce_price']}\n"
        f"止盈价：{trade_levels['take_profit']}\n"
        f"止损价：{trade_levels['stop_loss']}\n"
        f"放弃价：{trade_levels['abandon_price']}\n\n"
        "必须结合该股均线、近10日波动、支撑压力和消息摘要说明这些价格为什么这样定。\n\n"
        f"{prompt}"
    )

    def _deepseek_picker_fallback_model() -> Optional[str]:
        fallback_models = list(getattr(self.config, "litellm_fallback_models", []) or [])
        for model_name in fallback_models:
            if str(model_name or "").startswith("deepseek/"):
                return str(model_name)
        configured_models = [
            str((entry.get("model_name") or "")).strip()
            for entry in (getattr(self.config, "llm_model_list", []) or [])
        ]
        for model_name in configured_models:
            if model_name.startswith("deepseek/"):
                return model_name
        return "deepseek/deepseek-chat"

    def generate_with_model(
        model_name: str,
        prompt_text: str,
        *,
        max_tokens: int,
        temperature: float,
    ) -> str:
        if model_name.startswith("openai/"):
            generated, _usage = self.analyzer._call_openai_responses_stream_fallback(
                model=model_name,
                prompt=prompt_text,
                system_prompt=picker_system_prompt,
                config=self.config,
                max_tokens=max_tokens,
                temperature=temperature,
                timeout_seconds=75,
            )
            return str(generated or "").strip()

        if model_name.startswith("deepseek/"):
            return self._call_openai_compatible_chat_completion(
                model=model_name,
                prompt=prompt_text,
                system_prompt=picker_system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                timeout_seconds=75,
            )

        override = copy(self.config)
        override.litellm_model = model_name
        override.litellm_fallback_models = []
        previous_override = getattr(self.analyzer, "_config_override", None)
        self.analyzer._config_override = override
        try:
            result = self.analyzer._call_litellm(
                prompt_text,
                {
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "timeout_seconds": 75,
                },
                system_prompt=picker_system_prompt,
                stream=False,
            )
            generated = result[0] if isinstance(result, tuple) else result
            return str(generated or "").strip()
        finally:
            self.analyzer._config_override = previous_override

    primary_model = str(getattr(self.config, "litellm_model", "") or "")
    fallback_model = _deepseek_picker_fallback_model()
    model_order: List[str] = []
    if not prefer_fallback_only and primary_model:
        model_order.append(primary_model)
    if fallback_model and fallback_model not in model_order:
        model_order.append(fallback_model)

    attempts = (
        (prompt, 900, 0.25),
        (retry_prompt, 950, 0.1),
    )
    last_error: Optional[Exception] = None
    last_output = ""
    for prompt_text, max_tokens, temperature in attempts:
        for model_name in model_order:
            if not model_name:
                continue
            try:
                generated = generate_with_model(
                    model_name,
                    prompt_text,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                generated = str(generated or "").strip()
                if generated.startswith("好的") or generated.startswith("下面") or generated.startswith("作为"):
                    non_empty_lines = [line.strip() for line in generated.splitlines() if line.strip()]
                    if len(non_empty_lines) > 1:
                        generated = "\n".join(non_empty_lines[1:]).strip()
                generated = self._repair_action_plan(
                    candidate=candidate,
                    strategy=strategy,
                    market_snapshot=market_snapshot,
                    us_snapshot=us_snapshot,
                    text=generated,
                )
                last_output = generated
                if _picker_is_action_plan_usable(generated):
                    return generated, model_name
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Stock picker LLM action plan generation failed for %s via %s: %s",
                    candidate.get("code"),
                    model_name,
                    exc,
                )
                continue

    if last_error is not None:
        raise RuntimeError(f"LLM generation failed: {last_error}") from last_error
    raise RuntimeError(f"LLM generation returned unusable action plan: {last_output[:120]}")


def _picker_build_run_summary_markdown(
    self: StockPickerService,
    strategy: Dict[str, Any],
    scan_date: date,
    selected: Sequence[Dict[str, Any]],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
) -> str:
    lines = [
        f"# {scan_date.isoformat()} 收盘后选股",
        "",
        f"- 策略：{strategy['name']}",
        "- 范围：沪深主板（非 ST）",
        f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
        f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
        f"- 候选上限：{_MAX_SELECTED_CANDIDATES} 只，详细手册上限：{_MAX_REPORT_CANDIDATES} 只",
        "",
        "## 候选股",
    ]
    if not selected:
        lines.append("- 今日没有符合条件的标的。")
        return "\n".join(lines)
    for item in selected:
        lines.append(
            f"- {item['rank']}. {item['name']}({item['code']}) | 分数 {item['score']} | {item['setup_type']} | {item['analysis_summary']}"
        )
    return "\n".join(lines)


def _picker_send_scan_notification(self: StockPickerService, payload: Dict[str, Any]) -> None:
    try:
        NotificationService().send(self._build_notification_markdown(payload))
    except Exception as exc:
        logger.warning("Stock picker notification failed: %s", exc)


def _picker_safe_json_loads(self: StockPickerService, value: Any) -> Any:
    if value in (None, "", b""):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def _picker_backtest_to_dict(self: StockPickerService, row: StockSelectionBacktest) -> Dict[str, Any]:
    return {
        "id": row.id,
        "candidate_id": row.candidate_id,
        "strategy_id": row.strategy_id,
        "code": row.code,
        "scan_date": row.scan_date.isoformat() if row.scan_date else None,
        "horizon_days": row.horizon_days,
        "status": row.status,
        "entry_date": row.entry_date.isoformat() if row.entry_date else None,
        "exit_date": row.exit_date.isoformat() if row.exit_date else None,
        "entry_price": row.entry_price,
        "exit_price": row.exit_price,
        "end_close": row.end_close,
        "max_high": row.max_high,
        "min_low": row.min_low,
        "return_pct": row.return_pct,
        "max_drawdown_pct": row.max_drawdown_pct,
        "outcome": row.outcome,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "evaluated_at": row.evaluated_at.isoformat() if row.evaluated_at else None,
    }


def _picker_candidate_to_dict(
    self: StockPickerService,
    row: StockSelectionCandidate,
    backtests: Optional[Sequence[StockSelectionBacktest]] = None,
) -> Dict[str, Any]:
    return {
        "id": row.id,
        "run_id": row.run_id,
        "code": row.code,
        "name": row.name,
        "strategy_id": row.strategy_id,
        "scan_date": row.scan_date.isoformat() if row.scan_date else None,
        "setup_type": row.setup_type,
        "score": float(row.score or 0.0),
        "rank": int(row.rank or 0),
        "selected": bool(row.selected),
        "operation_advice": row.operation_advice,
        "analysis_summary": row.analysis_summary,
        "action_plan_markdown": row.action_plan_markdown,
        "reasons": self._safe_json_loads(row.reason_json) or [],
        "metrics": self._safe_json_loads(row.indicator_snapshot_json) or {},
        "market_context": self._safe_json_loads(row.market_context_json) or {},
        "news_context": self._safe_json_loads(row.news_context_json),
        "llm_model": row.llm_model,
        "stop_loss": row.stop_loss,
        "take_profit": row.take_profit,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "backtests": [self._backtest_to_dict(item) for item in (backtests or [])],
    }


def _picker_run_to_dict(
    self: StockPickerService,
    row: StockSelectionRun,
    *,
    with_candidates: bool = False,
) -> Dict[str, Any]:
    payload = {
        "id": row.id,
        "run_id": row.id,
        "query_id": row.query_id,
        "strategy_id": row.strategy_id,
        "strategy_name": row.strategy_name,
        "scan_date": row.scan_date.isoformat() if row.scan_date else None,
        "status": row.status,
        "total_scanned": int(row.total_scanned or 0),
        "matched_count": int(row.matched_count or 0),
        "selected_count": int(row.selected_count or 0),
        "market_snapshot": self._safe_json_loads(row.market_snapshot_json) or {},
        "us_market_snapshot": self._safe_json_loads(row.us_market_snapshot_json) or {},
        "optimization": self._safe_json_loads(row.optimization_snapshot_json),
        "summary_markdown": row.summary_markdown,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }
    if with_candidates:
        payload["candidates"] = []
    return payload


def _picker_get_latest_optimization(
    self: StockPickerService,
    strategy_id: str,
) -> Optional[Dict[str, Any]]:
    with self.db.get_session() as session:
        rows = session.execute(
            select(StockSelectionOptimization)
            .where(StockSelectionOptimization.strategy_id == strategy_id)
            .order_by(desc(StockSelectionOptimization.created_at))
            .limit(12)
        ).scalars().all()

    for row in rows:
        params = self._safe_json_loads(row.params_json)
        metrics = self._safe_json_loads(row.metrics_json)
        if not isinstance(params, dict):
            continue
        if not isinstance(metrics, dict):
            metrics = {}
        if (row.status or "").strip().lower() != "completed":
            continue
        sample_count = int(self._to_float(metrics.get("sample_count"), 0.0))
        if sample_count < 6:
            continue
        return {
            "id": row.id,
            "strategy_id": row.strategy_id,
            "strategy_name": row.strategy_name,
            "lookback_days": int(row.lookback_days or 0),
            "selected_horizon_days": int(row.selected_horizon_days or 0),
            "status": row.status,
            "params": params,
            "metrics": metrics,
            "llm_review": metrics.get("llm_review") if isinstance(metrics.get("llm_review"), dict) else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
    return None


def _picker_save_scan_run(
    self: StockPickerService,
    *,
    query_id: str,
    strategy: Dict[str, Any],
    scan_date: date,
    total_scanned: int,
    matched_count: int,
    selected: Sequence[Dict[str, Any]],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    optimization: Optional[Dict[str, Any]],
    status: str,
) -> int:
    summary_markdown = self._build_run_summary_markdown(
        strategy=strategy,
        scan_date=scan_date,
        selected=selected,
        market_snapshot=market_snapshot,
        us_snapshot=us_snapshot,
    )

    with self.db.session_scope() as session:
        run = StockSelectionRun(
            query_id=query_id,
            strategy_id=strategy["strategy_id"],
            strategy_name=strategy["name"],
            scan_date=scan_date,
            status=status,
            total_scanned=int(total_scanned or 0),
            matched_count=int(matched_count or 0),
            selected_count=len(selected),
            market_snapshot_json=json.dumps(market_snapshot or {}, ensure_ascii=False),
            us_market_snapshot_json=json.dumps(us_snapshot or {}, ensure_ascii=False),
            optimization_snapshot_json=json.dumps(optimization or {}, ensure_ascii=False),
            summary_markdown=summary_markdown,
            created_at=datetime.now(),
            completed_at=datetime.now() if status == "completed" else None,
        )
        session.add(run)
        session.flush()

        for candidate in selected:
            session.add(
                StockSelectionCandidate(
                    run_id=run.id,
                    code=str(candidate.get("code") or "").strip(),
                    name=str(candidate.get("name") or "").strip(),
                    strategy_id=str(candidate.get("strategy_id") or strategy["strategy_id"]),
                    scan_date=scan_date,
                    setup_type=str(candidate.get("setup_type") or "mixed"),
                    score=float(candidate.get("score") or 0.0),
                    rank=int(candidate.get("rank") or 0),
                    selected=True,
                    operation_advice=str(candidate.get("operation_advice") or ""),
                    analysis_summary=candidate.get("analysis_summary"),
                    action_plan_markdown=candidate.get("action_plan_markdown"),
                    reason_json=json.dumps(candidate.get("reasons") or [], ensure_ascii=False),
                    indicator_snapshot_json=json.dumps(candidate.get("metrics") or {}, ensure_ascii=False),
                    market_context_json=json.dumps(candidate.get("market_context") or {}, ensure_ascii=False),
                    news_context_json=json.dumps(candidate.get("news_context"), ensure_ascii=False)
                    if candidate.get("news_context") is not None
                    else None,
                    llm_model=candidate.get("llm_model"),
                    stop_loss=self._to_float(candidate.get("stop_loss")) if candidate.get("stop_loss") not in (None, "") else None,
                    take_profit=self._to_float(candidate.get("take_profit")) if candidate.get("take_profit") not in (None, "") else None,
                    created_at=datetime.now(),
                )
            )
        session.flush()
        return int(run.id)


def _picker_update_run_optimization(
    self: StockPickerService,
    *,
    run_id: int,
    optimization: Optional[Dict[str, Any]],
) -> None:
    with self.db.session_scope() as session:
        run = session.execute(
            select(StockSelectionRun).where(StockSelectionRun.id == run_id)
        ).scalar_one_or_none()
        if run is None:
            return
        run.optimization_snapshot_json = json.dumps(optimization or {}, ensure_ascii=False)


def _picker_load_forward_bars(
    self: StockPickerService,
    code: str,
    scan_date: date,
    max_days: int,
) -> pd.DataFrame:
    with self.db.get_session() as session:
        rows = session.execute(
            select(StockDaily)
            .where(
                and_(
                    StockDaily.code == code,
                    StockDaily.date > scan_date,
                )
            )
            .order_by(StockDaily.date.asc())
            .limit(max(max_days, 1))
        ).scalars().all()
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    return pd.DataFrame(
        [
            {
                "date": row.date,
                "open": self._to_float(row.open),
                "high": self._to_float(row.high),
                "low": self._to_float(row.low),
                "close": self._to_float(row.close),
                "volume": self._to_float(row.volume),
                "amount": self._to_float(row.amount),
            }
            for row in rows
        ]
    )


def _picker_evaluate_candidate_horizon(
    self: StockPickerService,
    candidate: StockSelectionCandidate,
    bars: pd.DataFrame,
    horizon: int,
) -> Dict[str, Any]:
    if bars is None or bars.empty or len(bars) < horizon:
        return {"status": "pending"}

    window = bars.head(horizon).reset_index(drop=True)
    entry_bar = window.iloc[0]
    exit_bar = window.iloc[horizon - 1]
    entry_price = self._to_float(entry_bar.get("open")) or self._to_float(entry_bar.get("close"))
    exit_price = self._to_float(exit_bar.get("close"))
    if entry_price <= 0 or exit_price <= 0:
        return {"status": "pending"}

    max_high = self._to_float(window["high"].max())
    min_low = self._to_float(window["low"].min())
    return_pct = (exit_price - entry_price) / max(entry_price, 0.01) * 100.0
    max_drawdown_pct = (min_low - entry_price) / max(entry_price, 0.01) * 100.0
    if return_pct > 0.8:
        outcome = "win"
    elif return_pct < -0.8:
        outcome = "loss"
    else:
        outcome = "neutral"

    return {
        "status": "completed",
        "entry_date": entry_bar.get("date"),
        "exit_date": exit_bar.get("date"),
        "entry_price": round(entry_price, 2),
        "exit_price": round(exit_price, 2),
        "end_close": round(self._to_float(exit_bar.get("close")), 2),
        "max_high": round(max_high, 2),
        "min_low": round(min_low, 2),
        "return_pct": round(return_pct, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "outcome": outcome,
    }


def _picker_load_universe_from_local_db(self: StockPickerService) -> pd.DataFrame:
    with self.db.get_session() as session:
        rows = session.execute(
            select(StockDaily.code)
            .group_by(StockDaily.code)
            .order_by(StockDaily.code.asc())
        ).all()
    if not rows:
        return pd.DataFrame(columns=["code", "name"])
    return pd.DataFrame(
        [
            {
                "code": str(code or "").strip(),
                "name": str(code or "").strip(),
            }
            for (code,) in rows
            if str(code or "").strip()
        ]
    )


def _picker_resolve_scan_trade_date(self: StockPickerService) -> date:
    today = date.today()
    tickflow_fetcher = self.fetcher_manager._get_tickflow_fetcher()
    if tickflow_fetcher is not None and hasattr(tickflow_fetcher, "get_daily_batch"):
        try:
            frames = tickflow_fetcher.get_daily_batch(
                ["000001", "600519", "601318"],
                count=2,
                end_date=today.strftime("%Y-%m-%d"),
            )
            available_dates: List[date] = []
            for frame in frames.values():
                if frame is None or frame.empty or "date" not in frame.columns:
                    continue
                latest = pd.to_datetime(frame["date"]).dt.date.max()
                if latest is not None:
                    available_dates.append(latest)
            if available_dates:
                return max(available_dates)
        except Exception as exc:
            logger.warning("Stock picker failed to resolve TickFlow trade date: %s", exc)

    try:
        effective = get_effective_trading_date("cn")
    except Exception as exc:
        logger.warning("Stock picker failed to resolve effective CN trade date from calendar: %s", exc)
        effective = today

    latest_complete = self._get_latest_complete_daily_date(max_date=effective)
    if latest_complete is not None:
        return latest_complete

    if today.weekday() == 5:
        return today - timedelta(days=1)
    if today.weekday() == 6:
        return today - timedelta(days=2)

    return effective


def _picker_is_reusable_run_payload(self: StockPickerService, payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False

    status = str(payload.get("status") or "").strip().lower()
    total_scanned = int(payload.get("total_scanned") or 0)
    selected_count = int(payload.get("selected_count") or 0)
    candidates = list(payload.get("candidates") or [])

    if status != "completed":
        return False
    if total_scanned < 1000:
        return False
    if selected_count <= 0:
        return True
    if not candidates:
        return False

    top_candidates = candidates[: min(len(candidates), _MAX_REPORT_CANDIDATES)]
    return all(
        not self._is_placeholder_action_plan(item.get("action_plan_markdown"))
        and str(item.get("action_plan_markdown") or "").strip()
        for item in top_candidates
    )


def _picker_run_scheduled_scan(
    self: StockPickerService,
    strategy_id: Optional[str] = None,
) -> Dict[str, Any]:
    target_strategies = [strategy_id] if strategy_id else list(CORE_SCHEDULED_STRATEGIES)
    scan_date = self._resolve_scan_trade_date()

    payloads: List[Dict[str, Any]] = []
    with self._schedule_lock:
        for item_strategy_id in target_strategies:
            existing_run = self._find_latest_run_for_date(strategy_id=item_strategy_id, scan_date=scan_date)
            if existing_run is not None:
                self._repair_stuck_run(run_id=existing_run)
                payload = self.get_run_detail(existing_run)
                if self._is_reusable_run_payload(payload):
                    payloads.append(payload)
                    continue
                logger.warning(
                    "Stock picker will rerun scheduled scan because existing run is incomplete or unusable: strategy=%s run_id=%s scan_date=%s",
                    item_strategy_id,
                    existing_run,
                    scan_date,
                )

            payloads.append(
                self.run_scan(
                    strategy_id=item_strategy_id,
                    scan_date=scan_date,
                    max_candidates=_MAX_REPORT_CANDIDATES,
                    send_notification=True,
                    recompute_market_snapshot=True,
                )
            )

    if strategy_id:
        return payloads[0] if payloads else {}
    return {
        "status": "queued",
        "scan_date": scan_date.isoformat(),
        "strategy_ids": target_strategies,
        "runs": payloads,
    }


def _picker_get_latest_market_snapshot(self: StockPickerService) -> Dict[str, Any]:
    with self.db.get_session() as session:
        row = session.execute(
            select(StockSelectionRun.market_snapshot_json, StockSelectionRun.completed_at)
            .where(StockSelectionRun.status == "completed")
            .order_by(desc(StockSelectionRun.scan_date), desc(StockSelectionRun.created_at))
            .limit(1)
        ).first()
    if not row:
        return {}
    snapshot = self._safe_json_loads(row[0]) or {}
    if row[1] is not None:
        snapshot["updated_at"] = row[1].isoformat()
    return snapshot


def _picker_extract_json_block(raw_text: str) -> Optional[Dict[str, Any]]:
    parsed = _picker_extract_json_value(raw_text)
    return parsed if isinstance(parsed, dict) else None


def _picker_strip_code_fence(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if not text:
        return ""
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text, count=1)
        text = re.sub(r"\s*```$", "", text, count=1)
    return text.strip()


def _picker_extract_json_value(raw_text: str) -> Optional[Any]:
    text = _picker_strip_code_fence(raw_text)
    if not text:
        return None

    for candidate in (text, text.strip("`").strip()):
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except Exception:
            pass
        try:
            parsed_yaml = yaml.safe_load(candidate)
        except Exception:
            parsed_yaml = None
        if isinstance(parsed_yaml, (dict, list)):
            return parsed_yaml

    decoder = json.JSONDecoder()
    best_match: Optional[Any] = None
    best_score = -1
    for start, char in enumerate(text):
        if char not in "{[":
            continue
        try:
            parsed, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        size_bonus = 0
        if isinstance(parsed, dict):
            size_bonus = len(parsed.keys()) * 1000
        elif isinstance(parsed, list):
            size_bonus = len(parsed) * 500
        score = size_bonus + end
        if score > best_score:
            best_match = parsed
            best_score = score

    if best_match is not None:
        return best_match

    python_like = _picker_strip_code_fence(text)
    if python_like:
        normalized = (
            python_like.replace(": true", ": True")
            .replace(": false", ": False")
            .replace(": null", ": None")
        )
        try:
            parsed = ast.literal_eval(normalized)
        except Exception:
            parsed = None
        if isinstance(parsed, (dict, list)):
            return parsed
    return None


def _picker_normalize_review_adjustments(value: Any) -> List[Dict[str, str]]:
    parsed = value
    if isinstance(parsed, str):
        parsed = _picker_extract_json_value(parsed)
    if not isinstance(parsed, list):
        return []
    rows: List[Dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        direction = str(item.get("direction") or "").strip()
        rationale = str(item.get("rationale") or "").strip()
        if not (name or direction or rationale):
            continue
        rows.append(
            {
                "name": name,
                "direction": direction,
                "rationale": rationale,
            }
        )
    return rows


def _picker_normalize_factor_hypotheses(value: Any) -> List[Dict[str, str]]:
    parsed = value
    if isinstance(parsed, str):
        parsed = _picker_extract_json_value(parsed)
    if not isinstance(parsed, list):
        return []
    rows: List[Dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        factor = str(item.get("factor") or "").strip()
        hypothesis = str(item.get("hypothesis") or "").strip()
        expected_effect = str(item.get("expected_effect") or "").strip()
        if not (factor or hypothesis or expected_effect):
            continue
        rows.append(
            {
                "factor": factor,
                "hypothesis": hypothesis,
                "expected_effect": expected_effect,
            }
        )
    return rows


def _picker_normalize_experiments(value: Any) -> List[Dict[str, str]]:
    parsed = value
    if isinstance(parsed, str):
        parsed = _picker_extract_json_value(parsed)
    if not isinstance(parsed, list):
        return []
    rows: List[Dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        experiment = str(item.get("experiment") or "").strip()
        success_metric = str(item.get("success_metric") or "").strip()
        stop_condition = str(item.get("stop_condition") or "").strip()
        if not (experiment or success_metric or stop_condition):
            continue
        rows.append(
            {
                "experiment": experiment,
                "success_metric": success_metric,
                "stop_condition": stop_condition,
            }
        )
    return rows


def _picker_normalize_string_list(value: Any) -> List[str]:
    parsed = value
    if isinstance(parsed, str):
        maybe_json = _picker_extract_json_value(parsed)
        parsed = maybe_json if maybe_json is not None else [segment.strip("- ").strip() for segment in parsed.splitlines()]
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _picker_extract_json_field(raw_text: str, field_name: str) -> Optional[Any]:
    text = _picker_strip_code_fence(raw_text)
    if not text:
        return None
    match = re.search(rf'"{re.escape(field_name)}"\s*:\s*', text)
    if not match:
        return None
    start = match.end()
    while start < len(text) and text[start].isspace():
        start += 1
    if start >= len(text):
        return None

    decoder = json.JSONDecoder()
    try:
        parsed, _end = decoder.raw_decode(text[start:])
        return parsed
    except Exception:
        pass

    for opener in ('[', '{'):
        field_start = text.find(opener, start)
        if field_start == -1:
            continue
        try:
            parsed, _end = decoder.raw_decode(text[field_start:])
        except Exception:
            continue
        return parsed

    if text[start] == '"':
        text_end = text.find('",', start + 1)
        if text_end == -1:
            text_end = text.find('"\n', start + 1)
        if text_end == -1:
            text_end = text.rfind('"')
        if text_end > start:
            try:
                return json.loads(text[start:text_end + 1])
            except Exception:
                return text[start + 1:text_end]
    return None


def _picker_extract_review_fields(raw_text: str) -> Optional[Dict[str, Any]]:
    payload = {
        "diagnosis_summary": _picker_extract_json_field(raw_text, "diagnosis_summary"),
        "parameter_adjustments": _picker_extract_json_field(raw_text, "parameter_adjustments"),
        "factor_hypotheses": _picker_extract_json_field(raw_text, "factor_hypotheses"),
        "control_rules": _picker_extract_json_field(raw_text, "control_rules"),
        "next_week_experiments": _picker_extract_json_field(raw_text, "next_week_experiments"),
    }
    if not any(value is not None for value in payload.values()):
        return None
    return payload


def _picker_normalize_optimization_review_payload(
    payload: Dict[str, Any],
    *,
    model_used: str,
    raw_text: str,
) -> Dict[str, Any]:
    if not payload and raw_text:
        extracted_payload = _picker_extract_review_fields(raw_text)
        if isinstance(extracted_payload, dict):
            payload = extracted_payload
    normalized: Dict[str, Any] = {
        "diagnosis_summary": str(payload.get("diagnosis_summary") or "").strip(),
        "parameter_adjustments": _picker_normalize_review_adjustments(payload.get("parameter_adjustments")),
        "factor_hypotheses": _picker_normalize_factor_hypotheses(payload.get("factor_hypotheses")),
        "control_rules": _picker_normalize_string_list(payload.get("control_rules")),
        "next_week_experiments": _picker_normalize_experiments(payload.get("next_week_experiments")),
        "model": model_used or str(payload.get("model") or "").strip() or "unknown",
    }
    if not normalized["diagnosis_summary"]:
        fallback_text = _picker_strip_code_fence(raw_text)
        normalized["diagnosis_summary"] = fallback_text[:800]
    return normalized


def _picker_fill_review_gaps(
    self: StockPickerService,
    review: Dict[str, Any],
    *,
    strategy: Dict[str, Any],
    prompt_payload: Dict[str, Any],
    best_payload: Dict[str, Any],
) -> Dict[str, Any]:
    metrics = best_payload.get("metrics") or {}
    params = best_payload.get("params") or {}
    worst_samples = prompt_payload.get("worst_samples") or []
    best_samples = prompt_payload.get("best_samples") or []

    def _median(values: List[float]) -> float:
        cleaned = sorted(v for v in values if isinstance(v, (int, float)))
        if not cleaned:
            return 0.0
        middle = len(cleaned) // 2
        if len(cleaned) % 2:
            return float(cleaned[middle])
        return float((cleaned[middle - 1] + cleaned[middle]) / 2)

    worst_ma20 = [self._to_float(row.get("distance_to_ma20_pct"), 0.0) for row in worst_samples]
    worst_market = [self._to_float(row.get("market_score"), 0.0) for row in worst_samples]
    best_market = [self._to_float(row.get("market_score"), 0.0) for row in best_samples]
    preferred_setup = str(params.get("preferred_setup_type") or "").strip()
    sample_distribution = metrics.get("setup_type_distribution") or {}
    breakout_count = int(self._to_float(sample_distribution.get("breakout"), 0.0))
    pullback_count = int(self._to_float(sample_distribution.get("pullback"), 0.0))

    if not review.get("parameter_adjustments"):
        adjustments: List[Dict[str, str]] = []
        ma20_limit = self._to_float(params.get("pullback_max_ma20_distance_pct") or params.get("max_ma20_distance_pct"), 0.0)
        ma20_worst = max(worst_ma20) if worst_ma20 else 0.0
        if ma20_limit > 0 and ma20_worst > max(ma20_limit * 1.5, ma20_limit + 1.0):
            suggested = max(1.5, round(min(ma20_limit, _median(worst_ma20) * 0.6 or ma20_limit - 0.5), 1))
            adjustments.append(
                {
                    "name": "pullback_max_ma20_distance_pct" if strategy.get("strategy_id") == "mainboard_swing_master" else "max_ma20_distance_pct",
                    "direction": "decrease",
                    "rationale": f"最差样本的 MA20 偏离中位数约 {_median(worst_ma20):.2f}% ，最大值达到 {ma20_worst:.2f}% ，需要把上限收紧到约 {suggested:.1f}% 附近，先切掉趋势过度延伸的伪回踩。",
                }
            )
        market_floor = self._to_float(params.get("pullback_market_score_floor") or params.get("market_score_floor"), 0.0)
        if worst_market and best_market and _median(best_market) - _median(worst_market) >= 8.0:
            suggested = round(max(market_floor, min(_median(best_market) - 4.0, _median(worst_market) + 6.0)), 1)
            adjustments.append(
                {
                    "name": "market_score_floor",
                    "direction": "increase",
                    "rationale": f"最佳样本的市场评分中位数约 {_median(best_market):.2f}，明显高于最差样本的 {_median(worst_market):.2f}。把市场分地板上调到 {suggested:.1f}，先过滤弱情绪日的高分陷阱票。",
                }
            )
        if breakout_count <= 1 and pullback_count >= 8 and preferred_setup != "pullback":
            adjustments.append(
                {
                    "name": "preferred_setup_type",
                    "direction": "bias_to_pullback",
                    "rationale": "当前样本几乎全部由 pullback 贡献，breakout 样本过少且没有形成稳定优势。下周应先以 pullback 为主策略，breakout 只保留观察仓。",
                }
            )
        review["parameter_adjustments"] = adjustments

    if not review.get("factor_hypotheses"):
        review["factor_hypotheses"] = [
            {
                "factor": "板块相对强度",
                "hypothesis": "同等个股分数下，所属题材强度靠前的样本更容易把收益走出来，弱板块中的强个股更容易回撤。",
                "expected_effect": "提高持续性，降低入选后 3 到 5 日的回撤。",
            },
            {
                "factor": "波动率/ATR 约束",
                "hypothesis": "高 ATR 样本虽然容易给出高收益，但同时贡献了尾部深回撤，应对高波动样本单独限仓或剔除。",
                "expected_effect": "压缩最差回撤和高回撤样本占比。",
            },
        ]

    if not review.get("control_rules"):
        review["control_rules"] = [
            "市场情绪评分低于优化后地板值时，减少新开仓，只保留趋势回踩型样本。",
            "股价距离 MA20 或 MA5 过远时，不因单日高分放行，先看是否属于趋势过度延伸。",
            "同一策略周内只允许小步更新参数，优先验证 1 到 2 个主变量，避免过拟合。",
        ]

    if not review.get("next_week_experiments"):
        review["next_week_experiments"] = [
            {
                "experiment": "把 pullback 的 MA20 偏离阈值收紧一档，并单独统计 T+3 / T+5 的回撤改善幅度。",
                "success_metric": "平均回撤下降至少 0.5 个点，且胜率不低于当前基线。",
                "stop_condition": "样本数跌破 8 或平均收益下降超过 1 个点。",
            },
            {
                "experiment": "把市场情绪地板提高后，观察弱情绪日的筛选结果和最大回撤变化。",
                "success_metric": "最差回撤明显收敛，高回撤样本占比下降。",
                "stop_condition": "入选样本过少，连续两周无法形成有效交易池。",
            },
        ]

    return review


def _picker_build_optimization_review(
    self: StockPickerService,
    *,
    strategy: Dict[str, Any],
    lookback_days: int,
    candidate_rows: Sequence[Dict[str, Any]],
    best_payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    metrics = best_payload.get("metrics") or {}
    params = best_payload.get("params") or {}
    horizon = int(best_payload.get("selected_horizon_days") or 5)
    sample_rows = [row for row in candidate_rows if int(row.get("horizon_days") or 0) == horizon]
    if len(sample_rows) < 8:
        return None

    def summarize(rows: Sequence[Dict[str, Any]], setup_type: str) -> Dict[str, Any]:
        filtered = [row for row in rows if not setup_type or row.get("setup_type") == setup_type]
        if not filtered:
            return {"sample_count": 0}
        avg_return = sum(float(row.get("return_pct") or 0.0) for row in filtered) / len(filtered)
        avg_drawdown = sum(float(row.get("max_drawdown_pct") or 0.0) for row in filtered) / len(filtered)
        win_rate = sum(1 for row in filtered if float(row.get("return_pct") or 0.0) > 0) / len(filtered)
        worst_drawdown = min(float(row.get("max_drawdown_pct") or 0.0) for row in filtered)
        return {
            "sample_count": len(filtered),
            "avg_return_pct": round(avg_return, 2),
            "avg_drawdown_pct": round(avg_drawdown, 2),
            "win_rate_pct": round(win_rate * 100, 2),
            "worst_drawdown_pct": round(worst_drawdown, 2),
        }

    losers = sorted(sample_rows, key=lambda row: (float(row.get("return_pct") or 0.0), float(row.get("max_drawdown_pct") or 0.0)))[:8]
    winners = sorted(sample_rows, key=lambda row: (float(row.get("return_pct") or 0.0), -float(row.get("max_drawdown_pct") or 0.0)), reverse=True)[:5]

    prompt_payload = {
        "strategy_id": strategy.get("strategy_id"),
        "strategy_name": strategy.get("name"),
        "lookback_days": lookback_days,
        "selected_horizon_days": horizon,
        "chosen_params": params,
        "aggregates": {
            "all": summarize(sample_rows, ""),
            "pullback": summarize(sample_rows, "pullback"),
            "breakout": summarize(sample_rows, "breakout"),
        },
        "best_metrics": metrics,
        "worst_samples": [
            {
                "setup_type": row.get("setup_type"),
                "score": row.get("score"),
                "return_pct": row.get("return_pct"),
                "max_drawdown_pct": row.get("max_drawdown_pct"),
                "distance_to_ma20_pct": row.get("distance_to_ma20_pct"),
                "distance_to_ma5_pct": row.get("distance_to_ma5_pct"),
                "volume_spike_factor": row.get("volume_spike_factor"),
                "market_score": row.get("market_score"),
            }
            for row in losers
        ],
        "best_samples": [
            {
                "setup_type": row.get("setup_type"),
                "score": row.get("score"),
                "return_pct": row.get("return_pct"),
                "max_drawdown_pct": row.get("max_drawdown_pct"),
                "distance_to_ma20_pct": row.get("distance_to_ma20_pct"),
                "distance_to_ma5_pct": row.get("distance_to_ma5_pct"),
                "volume_spike_factor": row.get("volume_spike_factor"),
                "market_score": row.get("market_score"),
            }
            for row in winners
        ],
    }

    prompt = (
        "你是A股波段交易系统的策略复盘员。基于给定回测摘要，输出下周参数优化和新因子实验建议。"
        "不要空话，不要写风险提示套话，只做归因和可执行实验。"
        "输出严格 JSON，对象包含字段："
        "diagnosis_summary, parameter_adjustments, factor_hypotheses, control_rules, next_week_experiments。"
        "parameter_adjustments 是数组，每项包含 name, direction, rationale。"
        "factor_hypotheses 是数组，每项包含 factor, hypothesis, expected_effect。"
        "control_rules 是数组字符串。"
        "next_week_experiments 是数组，每项包含 experiment, success_metric, stop_condition。\n\n"
        f"{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
    )

    system_prompt = (
        "你是量化选股策略优化器。你的任务是做闭环复盘："
        "根据收益、平均回撤、最差回撤、setup_type 分布，优先提出降低回撤和尾部风险的建议，"
        "其次才提高收益。禁止输出营销文案。"
    )

    configured_models = [
        str((entry.get("model_name") or "")).strip()
        for entry in (getattr(self.config, "llm_model_list", []) or [])
        if str((entry.get("model_name") or "")).strip()
    ]
    primary_model = str(getattr(self.config, "litellm_model", "") or "")
    model_order: List[str] = []
    for name in (primary_model, "openai/gpt-5.4", "deepseek/deepseek-reasoner", "deepseek/deepseek-chat", *configured_models):
        model_name = str(name or "").strip()
        if model_name and model_name not in model_order:
            model_order.append(model_name)

    last_error: Optional[Exception] = None
    response_text = ""
    model_used = ""
    repair_system_prompt = (
        "你是 JSON 修复器。"
        "把输入内容整理为严格 JSON，只输出 JSON 对象。"
        "保留字段：diagnosis_summary, parameter_adjustments, factor_hypotheses, control_rules, next_week_experiments。"
        "如果某字段缺失，用空数组或空字符串补齐。"
        "不要输出 Markdown 代码块。"
    )
    for model_name in model_order:
        try:
            if model_name.startswith("openai/"):
                generated, _usage = self.analyzer._call_openai_responses_stream_fallback(
                    model=model_name,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    config=self.config,
                    max_tokens=1200,
                    temperature=0.2,
                    timeout_seconds=75,
                )
                response_text = str(generated or "").strip()
                model_used = model_name
            elif model_name.startswith("deepseek/"):
                response_text = self._call_openai_compatible_chat_completion(
                    model=model_name,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    max_tokens=1200,
                    temperature=0.2,
                    timeout_seconds=75,
                )
                model_used = model_name
            else:
                override = copy(self.config)
                override.litellm_model = model_name
                override.litellm_fallback_models = []
                previous_override = getattr(self.analyzer, "_config_override", None)
                self.analyzer._config_override = override
                try:
                    generated, model_used, _usage = self.analyzer._call_litellm(
                        prompt,
                        {"temperature": 0.2, "max_output_tokens": 1200, "timeout_seconds": 75},
                        system_prompt=system_prompt,
                        stream=False,
                    )
                    response_text = str(generated or "").strip()
                finally:
                    self.analyzer._config_override = previous_override
            parsed = _picker_extract_json_block(response_text)
            if not isinstance(parsed, dict) and response_text:
                parsed = _picker_extract_review_fields(response_text)
            if not isinstance(parsed, dict) and response_text:
                repair_prompt = (
                    "把下面这段策略复盘输出修复成严格 JSON。"
                    "不要补充新观点，只保留原有信息并整理为结构化字段。\n\n"
                    f"{response_text}"
                )
                repair_models: List[str] = []
                for repair_name in ("deepseek/deepseek-reasoner", "deepseek/deepseek-chat", "openai/gpt-5.4"):
                    if repair_name not in repair_models:
                        repair_models.append(repair_name)
                if model_name not in repair_models:
                    repair_models.append(model_name)
                for repair_model in repair_models:
                    try:
                        if repair_model.startswith("openai/"):
                            repaired_text, _usage = self.analyzer._call_openai_responses_stream_fallback(
                                model=repair_model,
                                prompt=repair_prompt,
                                system_prompt=repair_system_prompt,
                                config=self.config,
                                max_tokens=900,
                                temperature=0.0,
                                timeout_seconds=60,
                            )
                            repaired = str(repaired_text or "").strip()
                            parsed = _picker_extract_json_block(repaired)
                            if not isinstance(parsed, dict):
                                parsed = _picker_extract_review_fields(repaired)
                            if isinstance(parsed, dict):
                                response_text = repaired
                                model_used = repair_model
                                break
                        elif repair_model.startswith("deepseek/"):
                            repaired = self._call_openai_compatible_chat_completion(
                                model=repair_model,
                                prompt=repair_prompt,
                                system_prompt=repair_system_prompt,
                                max_tokens=900,
                                temperature=0.0,
                                timeout_seconds=60,
                            )
                            parsed = _picker_extract_json_block(repaired)
                            if not isinstance(parsed, dict):
                                parsed = _picker_extract_review_fields(repaired)
                            if isinstance(parsed, dict):
                                response_text = repaired
                                model_used = repair_model
                                break
                        else:
                            override = copy(self.config)
                            override.litellm_model = repair_model
                            override.litellm_fallback_models = []
                            previous_override = getattr(self.analyzer, "_config_override", None)
                            self.analyzer._config_override = override
                            try:
                                repaired_text, _used_model, _usage = self.analyzer._call_litellm(
                                    repair_prompt,
                                    {"temperature": 0.0, "max_output_tokens": 900, "timeout_seconds": 60},
                                    system_prompt=repair_system_prompt,
                                    stream=False,
                                )
                                repaired = str(repaired_text or "").strip()
                            finally:
                                self.analyzer._config_override = previous_override
                            parsed = _picker_extract_json_block(repaired)
                            if not isinstance(parsed, dict):
                                parsed = _picker_extract_review_fields(repaired)
                            if isinstance(parsed, dict):
                                response_text = repaired
                                model_used = repair_model
                                break
                    except Exception:
                        continue
            if isinstance(parsed, dict) and (
                "diagnosis_summary" in parsed
                or "parameter_adjustments" in parsed
                or "factor_hypotheses" in parsed
            ):
                normalized_review = _picker_normalize_optimization_review_payload(
                    parsed,
                    model_used=model_used,
                    raw_text=response_text,
                )
                return _picker_fill_review_gaps(
                    self,
                    normalized_review,
                    strategy=strategy,
                    prompt_payload=prompt_payload,
                    best_payload=best_payload,
                )
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Stock picker optimization review generation failed for %s via %s: %s",
                strategy.get("strategy_id"),
                model_name,
                exc,
            )
            continue

    if last_error is not None:
        logger.warning("Stock picker optimization review fallback summary used for %s: %s", strategy.get("strategy_id"), last_error)
    if response_text:
        normalized_review = _picker_normalize_optimization_review_payload(
            {},
            model_used=model_used or "fallback",
            raw_text=response_text,
        )
        return _picker_fill_review_gaps(
            self,
            normalized_review,
            strategy=strategy,
            prompt_payload=prompt_payload,
            best_payload=best_payload,
        )
    return None


def _picker_get_market_sentiment(self: StockPickerService) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    errors: List[str] = []
    for use_cache in (False, True):
        try:
            snapshot = self._build_market_snapshot(use_cache=use_cache) or {}
            if snapshot:
                break
        except Exception as exc:
            errors.append(str(exc))
    if not snapshot:
        snapshot = self._get_latest_market_snapshot()
    if not snapshot:
        if errors:
            raise RuntimeError(f"failed to load market sentiment: {' | '.join(errors[:2])}")
        raise RuntimeError("failed to load market sentiment")

    stats = snapshot.get("stats") or {}
    up_count = self._to_float(stats.get("up_count"))
    down_count = self._to_float(stats.get("down_count"))
    breadth = round(up_count / max(up_count + down_count, 1.0), 4) if (up_count + down_count) > 0 else None
    return {
        "market": "cn",
        "score": round(self._to_float(snapshot.get("score"), 50.0), 2),
        "regime": str(snapshot.get("regime") or "中性"),
        "summary": str(snapshot.get("summary") or "A股情绪数据暂不可用"),
        "breadth": breadth,
        "limit_up_count": self._to_float(stats.get("limit_up_count")) if stats else None,
        "limit_down_count": self._to_float(stats.get("limit_down_count")) if stats else None,
        "updated_at": snapshot.get("updated_at") or datetime.now().isoformat(),
    }


def _picker_query_strategy_backtest_rows(
    self: StockPickerService,
    *,
    strategy_id: Optional[str] = None,
    horizon_days: Optional[int] = None,
    scan_date_from: Optional[date] = None,
    scan_date_to: Optional[date] = None,
    top_n: int = 5,
) -> List[tuple[StockSelectionBacktest, StockSelectionCandidate, StockSelectionRun]]:
    conditions = [
        StockSelectionCandidate.selected.is_(True),
        StockSelectionCandidate.rank <= max(int(top_n or 5), 1),
    ]
    if strategy_id:
        conditions.append(StockSelectionCandidate.strategy_id == strategy_id)
    if horizon_days is not None:
        conditions.append(StockSelectionBacktest.horizon_days == int(horizon_days))
    if scan_date_from is not None:
        conditions.append(StockSelectionRun.scan_date >= scan_date_from)
    if scan_date_to is not None:
        conditions.append(StockSelectionRun.scan_date <= scan_date_to)

    with self.db.get_session() as session:
        rows = session.execute(
            select(StockSelectionBacktest, StockSelectionCandidate, StockSelectionRun)
            .join(
                StockSelectionCandidate,
                StockSelectionBacktest.candidate_id == StockSelectionCandidate.id,
            )
            .join(
                StockSelectionRun,
                StockSelectionCandidate.run_id == StockSelectionRun.id,
            )
            .where(and_(*conditions))
            .order_by(
                desc(StockSelectionRun.scan_date),
                StockSelectionCandidate.rank.asc(),
                StockSelectionBacktest.horizon_days.asc(),
                desc(StockSelectionRun.created_at),
            )
        ).all()
    return list(rows)


def _picker_build_backtest_metrics(
    completed_rows: Sequence[tuple[StockSelectionBacktest, StockSelectionCandidate, StockSelectionRun]],
) -> Dict[str, Any]:
    if not completed_rows:
        return {
            "completed_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "neutral_count": 0,
            "win_rate_pct": None,
            "avg_return_pct": None,
            "avg_max_drawdown_pct": None,
            "worst_drawdown_pct": None,
        }

    returns = [float(row[0].return_pct or 0.0) for row in completed_rows if row[0].return_pct is not None]
    drawdowns = [float(row[0].max_drawdown_pct or 0.0) for row in completed_rows if row[0].max_drawdown_pct is not None]
    win_count = sum(1 for row in completed_rows if str(row[0].outcome or "").lower() == "win")
    loss_count = sum(1 for row in completed_rows if str(row[0].outcome or "").lower() == "loss")
    neutral_count = sum(1 for row in completed_rows if str(row[0].outcome or "").lower() == "neutral")
    completed_count = len(completed_rows)
    return {
        "completed_count": completed_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "neutral_count": neutral_count,
        "win_rate_pct": round(win_count / completed_count * 100.0, 2) if completed_count else None,
        "avg_return_pct": round(sum(returns) / len(returns), 2) if returns else None,
        "avg_max_drawdown_pct": round(sum(drawdowns) / len(drawdowns), 2) if drawdowns else None,
        "worst_drawdown_pct": round(min(drawdowns), 2) if drawdowns else None,
    }


def _picker_get_strategy_backtest_summary(
    self: StockPickerService,
    *,
    strategy_id: Optional[str] = None,
    horizon_days: int = 5,
    scan_date_from: Optional[date] = None,
    scan_date_to: Optional[date] = None,
    top_n: int = 5,
) -> Dict[str, Any]:
    rows = self._query_strategy_backtest_rows(
        strategy_id=strategy_id,
        horizon_days=horizon_days,
        scan_date_from=scan_date_from,
        scan_date_to=scan_date_to,
        top_n=top_n,
    )
    completed_rows = [row for row in rows if str(row[0].status or "").lower() == "completed"]
    pending_count = sum(1 for row in rows if str(row[0].status or "").lower() != "completed")
    metrics = self._build_backtest_metrics(completed_rows)

    strategy_distribution: Dict[str, Dict[str, Any]] = {}
    setup_type_distribution: Dict[str, Dict[str, Any]] = {}
    scan_dates: set[str] = set()
    stock_codes: set[str] = set()

    for backtest, candidate, run in rows:
        strategy_key = str(run.strategy_id or candidate.strategy_id or "unknown")
        bucket = strategy_distribution.setdefault(
            strategy_key,
            {
                "strategy_id": strategy_key,
                "strategy_name": str(run.strategy_name or strategy_key),
                "total_evaluations": 0,
                "pending_count": 0,
                "_completed_rows": [],
            },
        )
        bucket["total_evaluations"] += 1
        if str(backtest.status or "").lower() == "completed":
            bucket["_completed_rows"].append((backtest, candidate, run))
        else:
            bucket["pending_count"] += 1
        if run.scan_date is not None:
            scan_dates.add(run.scan_date.isoformat())
        if candidate.code:
            stock_codes.add(candidate.code)
        setup_key = str(candidate.setup_type or "mixed")
        setup_bucket = setup_type_distribution.setdefault(
            setup_key,
            {
                "setup_type": setup_key,
                "total_evaluations": 0,
                "pending_count": 0,
                "_completed_rows": [],
            },
        )
        setup_bucket["total_evaluations"] += 1
        if str(backtest.status or "").lower() == "completed":
            setup_bucket["_completed_rows"].append((backtest, candidate, run))
        else:
            setup_bucket["pending_count"] += 1

    distribution_items: List[Dict[str, Any]] = []
    for item in strategy_distribution.values():
        item_metrics = self._build_backtest_metrics(item.pop("_completed_rows"))
        distribution_items.append(
            {
                **item,
                **item_metrics,
            }
        )
    distribution_items.sort(
        key=lambda item: (
            -int(item.get("completed_count") or 0),
            -float(item.get("win_rate_pct") or 0.0),
            str(item.get("strategy_name") or ""),
        )
    )
    setup_items: List[Dict[str, Any]] = []
    for item in setup_type_distribution.values():
        item_metrics = self._build_backtest_metrics(item.pop("_completed_rows"))
        setup_items.append({**item, **item_metrics})
    setup_items.sort(
        key=lambda item: (
            -int(item.get("completed_count") or 0),
            -float(item.get("win_rate_pct") or 0.0),
            str(item.get("setup_type") or ""),
        )
    )

    latest_scan_date = None
    if rows:
        latest_date = max((row[2].scan_date for row in rows if row[2].scan_date is not None), default=None)
        latest_scan_date = latest_date.isoformat() if latest_date is not None else None

    strategy_name = None
    if strategy_id:
        try:
            strategy_name = self._resolve_strategy(strategy_id).get("name")
        except Exception:
            strategy_name = strategy_id

    return {
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "horizon_days": int(horizon_days),
        "top_n": int(top_n),
        "total_evaluations": len(rows),
        "pending_count": pending_count,
        "scan_count": len(scan_dates),
        "stock_count": len(stock_codes),
        "latest_scan_date": latest_scan_date,
        "strategy_distribution": distribution_items,
        "setup_type_distribution": setup_items,
        **metrics,
    }


def _picker_get_strategy_backtest_results(
    self: StockPickerService,
    *,
    strategy_id: Optional[str] = None,
    horizon_days: int = 5,
    scan_date_from: Optional[date] = None,
    scan_date_to: Optional[date] = None,
    top_n: int = 5,
    page: int = 1,
    limit: int = 30,
) -> Dict[str, Any]:
    rows = self._query_strategy_backtest_rows(
        strategy_id=strategy_id,
        horizon_days=horizon_days,
        scan_date_from=scan_date_from,
        scan_date_to=scan_date_to,
        top_n=top_n,
    )
    total = len(rows)
    offset = max(page - 1, 0) * limit
    page_rows = rows[offset: offset + limit]
    items: List[Dict[str, Any]] = []
    for backtest, candidate, run in page_rows:
        items.append(
            {
                "id": int(backtest.id),
                "candidate_id": int(candidate.id),
                "run_id": int(run.id),
                "strategy_id": str(run.strategy_id or candidate.strategy_id or ""),
                "strategy_name": str(run.strategy_name or candidate.strategy_id or ""),
                "code": candidate.code,
                "name": candidate.name,
                "scan_date": run.scan_date.isoformat() if run.scan_date else None,
                "rank": int(candidate.rank or 0),
                "score": round(float(candidate.score or 0.0), 2),
                "setup_type": candidate.setup_type,
                "horizon_days": int(backtest.horizon_days or 0),
                "status": backtest.status,
                "entry_date": backtest.entry_date.isoformat() if backtest.entry_date else None,
                "exit_date": backtest.exit_date.isoformat() if backtest.exit_date else None,
                "entry_price": backtest.entry_price,
                "exit_price": backtest.exit_price,
                "return_pct": backtest.return_pct,
                "max_drawdown_pct": backtest.max_drawdown_pct,
                "outcome": backtest.outcome,
            }
        )
    return {
        "total": total,
        "page": int(page),
        "limit": int(limit),
        "items": items,
    }


def _picker_run_strategy_backtest_refresh(
    self: StockPickerService,
    *,
    strategy_id: Optional[str] = None,
    horizon_days: int = 5,
    top_n: int = 5,
    max_candidates: int = 2000,
) -> Dict[str, Any]:
    stats = self.backfill_backtests(
        strategy_id=strategy_id,
        max_candidates=max(max_candidates, 200),
    )
    summary = self.get_strategy_backtest_summary(
        strategy_id=strategy_id,
        horizon_days=horizon_days,
        top_n=top_n,
    )
    return {
        **stats,
        "summary": summary,
    }


def _picker_run_weekly_optimization(
    self: StockPickerService,
    *,
    lookback_days: int = 90,
    max_candidates: int = 3000,
    send_notification: bool = True,
) -> Dict[str, Any]:
    backtest_stats = self.backfill_backtests(max_candidates=max_candidates)
    strategy_payloads: List[Dict[str, Any]] = []
    strategy_map = {item["strategy_id"]: item for item in self.list_strategies()}
    for strategy_id in CORE_SCHEDULED_STRATEGIES:
        strategy = strategy_map.get(strategy_id)
        if not strategy:
            continue
        for horizon_days in (3, 5):
            try:
                payload = self.optimize_strategy(
                    strategy_id=strategy_id,
                    lookback_days=lookback_days,
                    selected_horizon_days=horizon_days,
                )
                strategy_payloads.append(payload)
            except Exception as exc:
                logger.warning(
                    "Stock picker weekly optimization failed for %s T+%s: %s",
                    strategy_id,
                    horizon_days,
                    exc,
                )
                strategy_payloads.append(
                    {
                        "strategy_id": strategy_id,
                        "strategy_name": strategy.get("name"),
                        "lookback_days": lookback_days,
                        "selected_horizon_days": horizon_days,
                        "status": "error",
                        "params": {},
                        "metrics": {"error": str(exc)},
                    }
                )
    payload = {
        "backtest_stats": backtest_stats,
        "strategies": strategy_payloads,
        "generated_at": datetime.now().isoformat(),
    }
    if send_notification:
        try:
            self._send_weekly_optimization_notification(payload)
        except Exception as exc:
            logger.warning("Stock picker weekly optimization notification failed: %s", exc)
    return payload


def _picker_build_weekly_optimization_markdown(
    self: StockPickerService,
    payload: Dict[str, Any],
) -> str:
    backtest_stats = payload.get("backtest_stats") or {}
    strategies = payload.get("strategies") or []
    generated_at = str(payload.get("generated_at") or datetime.now().isoformat())
    completed = [item for item in strategies if str(item.get("status") or "").lower() == "completed"]
    completed.sort(
        key=lambda item: (
            self._to_float((item.get("metrics") or {}).get("objective"), -9999.0),
            self._to_float((item.get("metrics") or {}).get("sample_count"), 0.0),
        ),
        reverse=True,
    )
    lines = [
        "# 每周策略优化报告",
        "",
        f"- 生成时间：{generated_at[:19].replace('T', ' ')}",
        f"- 回测补算：processed {int(self._to_float(backtest_stats.get('processed'), 0.0))} / completed {int(self._to_float(backtest_stats.get('completed'), 0.0))} / pending {int(self._to_float(backtest_stats.get('pending'), 0.0))} / errors {int(self._to_float(backtest_stats.get('errors'), 0.0))}",
        f"- 完成优化策略数：{len(completed)} / {len(strategies)}",
        "",
        "## 策略概览",
        "",
    ]

    if not completed:
        lines.extend(["- 本周没有形成可用的策略优化结果。"])
        return "\n".join(lines).strip()

    for item in completed:
        metrics = item.get("metrics") or {}
        params = item.get("params") or {}
        llm_review = item.get("llm_review") if isinstance(item.get("llm_review"), dict) else {}
        adjustments = llm_review.get("parameter_adjustments") or []
        factors = llm_review.get("factor_hypotheses") or []
        experiments = llm_review.get("next_week_experiments") or []
        control_rules = llm_review.get("control_rules") or []
        lines.extend(
            [
                f"### {item.get('strategy_name') or item.get('strategy_id')}",
                f"- 周期：T+{int(self._to_float(item.get('selected_horizon_days'), 5.0))}",
                f"- 样本：{int(self._to_float(metrics.get('sample_count'), 0.0))}，胜率：{self._to_float(metrics.get('win_rate_pct'), 0.0):.2f}% ，平均收益：{self._to_float(metrics.get('avg_return_pct'), 0.0):.2f}% ，平均回撤：{self._to_float(metrics.get('avg_max_drawdown_pct'), 0.0):.2f}% ，最差回撤：{self._to_float(metrics.get('worst_drawdown_pct'), 0.0):.2f}%",
                f"- 关键阈值：score >= {params.get('min_score_threshold', '-')}, volume >= {params.get('volume_spike_multiplier', '-')}, MA20偏离 <= {params.get('max_ma20_distance_pct', '-')}, 市场分 >= {params.get('market_score_floor', '-')}",
            ]
        )
        diagnosis_summary = str(llm_review.get("diagnosis_summary") or metrics.get("llm_summary") or "").strip()
        if diagnosis_summary:
            lines.append(f"- 复盘结论：{diagnosis_summary}")
        if adjustments:
            lines.append("- 参数建议：")
            for row in adjustments[:3]:
                lines.append(
                    f"  - {row.get('name') or '-'} / {row.get('direction') or '-'}：{row.get('rationale') or ''}"
                )
        if factors:
            lines.append("- 因子假设：")
            for row in factors[:2]:
                lines.append(
                    f"  - {row.get('factor') or '-'}：{row.get('hypothesis') or ''}；预期效果：{row.get('expected_effect') or ''}"
                )
        if control_rules:
            lines.append("- 控制规则：")
            for rule in control_rules[:3]:
                lines.append(f"  - {rule}")
        if experiments:
            lines.append("- 下周实验：")
            for row in experiments[:2]:
                lines.append(
                    f"  - {row.get('experiment') or '-'}；成功标准：{row.get('success_metric') or ''}；停止条件：{row.get('stop_condition') or ''}"
                )
        lines.extend(["", "---", ""])

    return "\n".join(lines).strip()


def _picker_send_weekly_optimization_notification(
    self: StockPickerService,
    payload: Dict[str, Any],
) -> None:
    NotificationService().send(self._build_weekly_optimization_markdown(payload))


def _picker_build_notification_markdown(self: StockPickerService, payload: Dict[str, Any]) -> str:
    scan_date = payload.get("scan_date") or date.today().isoformat()
    strategy_name = payload.get("strategy_name") or payload.get("strategy_id") or "stock picker"
    market_snapshot = payload.get("market_snapshot") or {}
    us_snapshot = payload.get("us_market_snapshot") or {}
    candidates = (payload.get("candidates") or [])[:_MAX_REPORT_CANDIDATES]
    lines = [
        f"# {scan_date} 收盘后选股最终版",
        "",
        f"- 策略：{strategy_name}",
        "- 范围：沪深主板（非 ST）",
        "- 说明：已完成新闻复核与 LLM 个股化操作手册生成。",
        f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
        f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
        f"- 扫描结果：入选 {payload.get('selected_count', 0)} / 命中 {payload.get('matched_count', 0)} / 扫描 {payload.get('total_scanned', 0)}",
        "",
    ]

    summary_markdown = payload.get("summary_markdown")
    if summary_markdown:
        lines.extend(["## 摘要", "", summary_markdown.strip(), ""])

    if not candidates:
        lines.extend(["## 候选股", "", "- 今日没有符合条件的标的。"])
        return "\n".join(lines)

    lines.extend(["## 候选股与次日操作手册", ""])
    for candidate in candidates:
        reasons = candidate.get("reasons") or []
        lines.append(
            f"### {candidate.get('rank', '-')}. {candidate.get('name') or candidate.get('code')} ({candidate.get('code')})"
        )
        lines.append(f"- 形态：{candidate.get('setup_type') or '未分类'}")
        lines.append(f"- 分数：{candidate.get('score')}")
        if candidate.get("analysis_summary"):
            lines.append(f"- 量化结论：{candidate.get('analysis_summary')}")
        if reasons:
            lines.append(f"- 触发原因：{'；'.join(str(item) for item in reasons[:6])}")
        if candidate.get("stop_loss") is not None or candidate.get("take_profit") is not None:
            lines.append(
                f"- 风控：止损 {candidate.get('stop_loss') or '-'}，止盈 {candidate.get('take_profit') or '-'}"
            )
        lines.extend(["", candidate.get("action_plan_markdown") or "暂无次日操作手册。", "", "---", ""])

    return "\n".join(lines).strip()


StockPickerService._format_price = staticmethod(_picker_format_price)
StockPickerService._strategy_catalog = _picker_strategy_catalog
StockPickerService.list_strategies = staticmethod(_picker_list_strategies)
StockPickerService._to_float = _picker_to_float
StockPickerService._score_intel_payload = _picker_score_intel_payload
StockPickerService._derive_trade_levels = _picker_derive_trade_levels
StockPickerService._build_stock_profile = _picker_build_stock_profile
StockPickerService._picker_compact_review_text = _picker_compact_review_text
StockPickerService._repair_action_plan = _picker_repair_action_plan
StockPickerService._build_template_action_plan = _picker_build_template_action_plan
StockPickerService._call_openai_compatible_chat_completion = _picker_call_openai_compatible_chat_completion
StockPickerService._build_action_plan = _picker_build_action_plan
StockPickerService._build_run_summary_markdown = _picker_build_run_summary_markdown
StockPickerService._send_scan_notification = _picker_send_scan_notification
StockPickerService._safe_json_loads = _picker_safe_json_loads
StockPickerService._backtest_to_dict = _picker_backtest_to_dict
StockPickerService._candidate_to_dict = _picker_candidate_to_dict
StockPickerService._run_to_dict = _picker_run_to_dict
StockPickerService._get_latest_optimization = _picker_get_latest_optimization
StockPickerService._save_scan_run = _picker_save_scan_run
StockPickerService._update_run_optimization = _picker_update_run_optimization
StockPickerService._load_forward_bars = _picker_load_forward_bars
StockPickerService._evaluate_candidate_horizon = _picker_evaluate_candidate_horizon
StockPickerService._load_universe_from_local_db = _picker_load_universe_from_local_db
StockPickerService._resolve_scan_trade_date = _picker_resolve_scan_trade_date
StockPickerService._is_reusable_run_payload = _picker_is_reusable_run_payload
StockPickerService.run_scheduled_scan = _picker_run_scheduled_scan
StockPickerService._get_latest_market_snapshot = _picker_get_latest_market_snapshot
StockPickerService._build_optimization_review = _picker_build_optimization_review
StockPickerService.get_market_sentiment = _picker_get_market_sentiment
StockPickerService._query_strategy_backtest_rows = _picker_query_strategy_backtest_rows
StockPickerService._build_backtest_metrics = staticmethod(_picker_build_backtest_metrics)
StockPickerService.get_strategy_backtest_summary = _picker_get_strategy_backtest_summary
StockPickerService.get_strategy_backtest_results = _picker_get_strategy_backtest_results
StockPickerService.run_strategy_backtest_refresh = _picker_run_strategy_backtest_refresh
StockPickerService.run_weekly_optimization = _picker_run_weekly_optimization
StockPickerService._build_notification_markdown = _picker_build_notification_markdown
StockPickerService._build_weekly_optimization_markdown = _picker_build_weekly_optimization_markdown
StockPickerService._send_weekly_optimization_notification = _picker_send_weekly_optimization_notification
