# -*- coding: utf-8 -*-
"""Stock picker API endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.services.stock_picker_service import StockPickerService

router = APIRouter()


class StrategyOption(BaseModel):
    strategy_id: str
    name: str
    description: str
    skill_id: str
    category: str
    params: Dict[str, Any]


class StrategyListResponse(BaseModel):
    strategies: List[StrategyOption]


class PickerScanRequest(BaseModel):
    strategy_id: Optional[str] = None
    scan_date: Optional[date] = None
    max_candidates: Optional[int] = Field(default=None, ge=1, le=10)
    strategy_params: Dict[str, Any] = Field(default_factory=dict)
    send_notification: bool = False
    force_refresh: bool = False


class PickerRunSummary(BaseModel):
    id: int
    query_id: str
    strategy_id: str
    strategy_name: str
    scan_date: Optional[str] = None
    status: str
    total_scanned: int
    matched_count: int
    selected_count: int
    market_snapshot: Optional[Dict[str, Any]] = None
    us_market_snapshot: Optional[Dict[str, Any]] = None
    optimization: Optional[Dict[str, Any]] = None
    summary_markdown: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class PickerCandidate(BaseModel):
    id: int
    run_id: int
    code: str
    name: Optional[str] = None
    strategy_id: str
    scan_date: Optional[str] = None
    setup_type: str
    score: float
    rank: int
    selected: bool
    operation_advice: Optional[str] = None
    analysis_summary: Optional[str] = None
    action_plan_markdown: Optional[str] = None
    reasons: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    market_context: Dict[str, Any] = Field(default_factory=dict)
    news_context: Optional[Any] = None
    llm_model: Optional[str] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    backtests: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: Optional[str] = None


class PickerRunDetail(PickerRunSummary):
    candidates: List[PickerCandidate] = Field(default_factory=list)
    backtest_stats: Optional[Dict[str, Any]] = None


class MarketSentimentResponse(BaseModel):
    market: str
    score: float
    regime: str
    summary: str
    breadth: Optional[float] = None
    limit_up_count: Optional[float] = None
    limit_down_count: Optional[float] = None
    updated_at: Optional[str] = None


@router.get("/strategies", response_model=StrategyListResponse)
async def get_picker_strategies():
    return StrategyListResponse(strategies=[StrategyOption(**item) for item in StockPickerService.list_strategies()])


@router.get("/runs", response_model=List[PickerRunSummary])
async def list_picker_runs(limit: int = 20):
    service = StockPickerService()
    return [PickerRunSummary(**item) for item in service.list_runs(limit=limit)]


@router.get("/runs/{run_id}", response_model=PickerRunDetail)
async def get_picker_run(run_id: int):
    service = StockPickerService()
    payload = service.get_run_detail(run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="picker run not found")
    return PickerRunDetail(**payload)


@router.get("/market-sentiment", response_model=MarketSentimentResponse)
async def get_picker_market_sentiment():
    service = StockPickerService()
    return MarketSentimentResponse(**service.get_market_sentiment())


@router.post("/scan", response_model=PickerRunDetail)
async def run_picker_scan(request: PickerScanRequest):
    service = StockPickerService()
    try:
        payload = service.run_scan(
            strategy_id=request.strategy_id,
            scan_date=request.scan_date,
            max_candidates=request.max_candidates,
            strategy_params=request.strategy_params,
            send_notification=request.send_notification,
            force_refresh=request.force_refresh,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return PickerRunDetail(**payload)


@router.post("/backtest")
async def backfill_picker_backtests(strategy_id: Optional[str] = None):
    service = StockPickerService()
    return service.backfill_backtests(strategy_id=strategy_id)
