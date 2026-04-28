import type React from 'react';
import { useEffect, useMemo, useState } from 'react';
import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Button, Card, EmptyState, InlineAlert, Input, Select } from '../components/common';
import {
  stockPickerApi,
  type PickerCandidate,
  type PickerRun,
  type PickerStrategyOption,
} from '../api/stockPicker';

type StrategyParamValues = Record<string, string>;

const PARAM_LABELS: Record<string, string> = {
  min_score_threshold: '最低分数',
  volume_spike_multiplier: '倍量系数',
  max_ma20_distance_pct: '距 MA20 最大偏离',
  max_ma5_distance_pct: '距 MA5 最大偏离',
  market_score_floor: 'A股情绪分下限',
  preferred_setup_type: '偏好形态',
  pullback_min_score_threshold: '回踩-最低分数',
  pullback_volume_spike_multiplier: '回踩-倍量系数',
  pullback_max_ma20_distance_pct: '回踩-距 MA20',
  pullback_max_ma5_distance_pct: '回踩-距 MA5',
  pullback_market_score_floor: '回踩-A股情绪分',
  breakout_min_score_threshold: '突破-最低分数',
  breakout_volume_spike_multiplier: '突破-倍量系数',
  breakout_max_ma20_distance_pct: '突破-距 MA20',
  breakout_max_ma5_distance_pct: '突破-距 MA5',
  breakout_market_score_floor: '突破-A股情绪分',
};

const PARAM_HINTS: Record<string, string> = {
  min_score_threshold: '分数越高越严格，常用范围 60-85',
  volume_spike_multiplier: '近 8 日倍量阈值，常用范围 1.2-2.5',
  max_ma20_distance_pct: '收盘价贴近 MA20 的最大偏离百分比',
};

const METRIC_LABELS: Record<string, string> = {
  distance_to_ma20_pct: '距 MA20 偏离',
  distance_to_ma5_pct: '距 MA5 偏离',
  volume_spike_factor: '倍量系数',
  trend_stack_up: '均线多头向上',
  ma20_up: 'MA20 向上',
  ma60_up: 'MA60 向上',
  macd_bull: 'MACD 多头',
  kdj_bull: 'KDJ 多头',
  effective_min_score_threshold: '生效分数阈值',
  scan_family: '扫描族',
  sample_count: '样本数',
  avg_return_pct: '平均收益',
  win_rate_pct: '胜率',
  objective: '目标值',
  median_return_pct: '收益中位数',
  loss_rate_pct: '亏损率',
  high_drawdown_rate_pct: '高回撤率',
  tail_loss_rate_pct: '尾部亏损率',
  profit_factor: '盈亏因子',
  llm_summary: 'LLM 总结',
};

const PENDING_STATUSES = new Set(['queued', 'running', 'enriching']);

const StockPickerPage: React.FC = () => {
  const [strategies, setStrategies] = useState<PickerStrategyOption[]>([]);
  const [runs, setRuns] = useState<PickerRun[]>([]);
  const [selectedRun, setSelectedRun] = useState<PickerRun | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [selectedStrategy, setSelectedStrategy] = useState('');
  const [strategyParams, setStrategyParams] = useState<StrategyParamValues>({});
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    document.title = '选股 - DSA';
  }, []);

  const selectedStrategyMeta = useMemo(
    () => strategies.find((item) => item.strategyId === selectedStrategy) ?? null,
    [selectedStrategy, strategies],
  );

  const selectedStrategyParams = useMemo(
    () => normalizeParams(selectedStrategyMeta?.params),
    [selectedStrategyMeta],
  );

  const optimizationSummary = useMemo(
    () => buildOptimizationSummary(selectedRun?.optimization),
    [selectedRun],
  );

  const strategyOptions = useMemo(
    () => strategies.map((item) => ({ value: item.strategyId, label: item.name })),
    [strategies],
  );

  useEffect(() => {
    if (!selectedStrategyMeta) {
      return;
    }
    setStrategyParams(selectedStrategyParams);
  }, [selectedStrategyMeta, selectedStrategyParams]);

  useEffect(() => {
    void loadData();
  }, []);

  useEffect(() => {
    if (!selectedRun || !PENDING_STATUSES.has(selectedRun.status)) {
      return;
    }

    const timer = window.setInterval(() => {
      void refreshSelectedRun(selectedRun.id, false);
    }, 5000);

    return () => window.clearInterval(timer);
  }, [selectedRun]);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const [nextStrategies, nextRuns] = await Promise.all([
        stockPickerApi.getStrategies(),
        stockPickerApi.listRuns(),
      ]);
      setStrategies(nextStrategies);
      setSelectedStrategy((prev) => prev || nextStrategies[0]?.strategyId || '');
      setRuns(nextRuns);
      const preferredRunId =
        selectedRunId !== null && nextRuns.some((item) => item.id === selectedRunId)
          ? selectedRunId
          : nextRuns[0]?.id ?? null;
      if (preferredRunId !== null) {
        setSelectedRunId(preferredRunId);
        setSelectedRun(await stockPickerApi.getRun(preferredRunId));
      } else {
        setSelectedRunId(null);
        setSelectedRun(null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '选股数据加载失败');
    } finally {
      setLoading(false);
    }
  }

  async function refreshSelectedRun(runId: number, toggleLoading = true) {
    if (toggleLoading) {
      setLoading(true);
    }
    try {
      const [detail, nextRuns] = await Promise.all([
        stockPickerApi.getRun(runId),
        stockPickerApi.listRuns(),
      ]);
      setSelectedRunId(runId);
      setSelectedRun(detail);
      setRuns(nextRuns);
    } catch (err) {
      setError(err instanceof Error ? err.message : '选股详情加载失败');
    } finally {
      if (toggleLoading) {
        setLoading(false);
      }
    }
  }

  function handleParamChange(key: string, value: string) {
    setStrategyParams((current) => ({ ...current, [key]: value }));
  }

  function handleResetParams() {
    setStrategyParams(selectedStrategyParams);
  }

  function handleApplyOptimizedParams() {
    const optimizedParams = normalizeParams(selectedRun?.optimization?.params);
    if (Object.keys(optimizedParams).length) {
      setStrategyParams(optimizedParams);
    }
  }

  async function handleScan() {
    setScanning(true);
    setError(null);
    try {
      const detail = await stockPickerApi.scan({
        strategyId: selectedStrategy,
        maxCandidates: 5,
        strategyParams: normalizeOutgoingParams(strategyParams),
        sendNotification: true,
        forceRefresh: false,
      });
      setSelectedRunId(detail.id);
      setSelectedRun(detail);
      setRuns(await stockPickerApi.listRuns());
      setSelectedRun(await stockPickerApi.getRun(detail.id));
    } catch (err) {
      setError(err instanceof Error ? err.message : '选股扫描失败');
    } finally {
      setScanning(false);
    }
  }

  return (
    <div className="grid gap-4 px-4 pb-6 pt-4 lg:grid-cols-[25rem_minmax(0,1fr)] lg:px-6">
      <aside className="hidden self-start lg:block">
        <div className="space-y-4">
          <Card title="收盘后选股" subtitle="Daily Scan" className="space-y-4">
            <Select
              label="策略"
              value={selectedStrategy}
              onChange={setSelectedStrategy}
              options={strategyOptions}
              placeholder="选择策略"
            />
            {selectedStrategyMeta ? (
              <p className="text-sm leading-6 text-secondary-text">{selectedStrategyMeta.description}</p>
            ) : null}

            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="text-sm font-semibold text-foreground">扫描参数</p>
                <div className="flex gap-2">
                  <Button variant="ghost" size="sm" onClick={handleResetParams}>
                    恢复默认
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleApplyOptimizedParams}
                    disabled={!optimizationSummary.hasParams}
                  >
                    套用优化值
                  </Button>
                </div>
              </div>

              {Object.keys(strategyParams).length ? (
                <div className="overflow-hidden rounded-2xl border border-border/60">
                  <table className="w-full border-collapse text-sm">
                    <thead className="bg-card/80 text-secondary-text">
                      <tr>
                        <th className="px-3 py-2 text-left font-medium">参数</th>
                        <th className="px-3 py-2 text-left font-medium">当前值</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(strategyParams).map(([key, value]) => (
                        <tr key={key} className="border-t border-border/60">
                          <td className="px-3 py-3 align-top">
                            <p className="font-medium text-foreground">{PARAM_LABELS[key] ?? key}</p>
                            <p className="mt-1 text-xs text-secondary-text">{PARAM_HINTS[key] ?? key}</p>
                          </td>
                          <td className="px-3 py-3 align-top">
                            <Input
                              value={value}
                              onChange={(event) => handleParamChange(key, event.target.value)}
                              placeholder="输入数值"
                            />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-sm text-secondary-text">当前策略没有可调参数。</p>
              )}
            </div>

            <Button isLoading={scanning} loadingText="扫描中..." onClick={handleScan}>
              立即扫描并推送飞书
            </Button>

            <p className="text-xs leading-5 text-secondary-text">
              扫描范围固定为沪深主板非 ST 标的。每次最多保留 10 只候选，仅对前 5 只生成消息面复核和次日操作手册。
            </p>
          </Card>

          <Card title="最近扫描" subtitle="Runs">
            <div className="flex flex-col gap-2">
              {runs.map((run) => (
                <button
                  key={run.id}
                  type="button"
                  onClick={() => void refreshSelectedRun(run.id)}
                  className={`rounded-2xl border px-4 py-3 text-left transition-colors ${
                    selectedRunId === run.id
                      ? 'border-cyan/40 bg-cyan/10'
                      : 'border-border/60 bg-card/50 hover:bg-hover'
                  }`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-sm font-semibold text-foreground">{run.strategyName}</p>
                    <StatusBadge status={run.status} />
                  </div>
                  <p className="mt-1 text-xs text-secondary-text">{run.scanDate}</p>
                  <p className="mt-1 text-xs text-secondary-text">
                    入选 {run.selectedCount} / 命中 {run.matchedCount} / 扫描 {run.totalScanned}
                  </p>
                </button>
              ))}
              {!runs.length ? (
                <EmptyState
                  title="还没有扫描记录"
                  description="先执行一次选股扫描，页面会显示候选股、回测和优化总结。"
                  className="border-none bg-transparent px-0 py-0 shadow-none"
                />
              ) : null}
            </div>
          </Card>
        </div>
      </aside>

      <main className="min-w-0">
        {error ? <InlineAlert title="操作失败" message={error} variant="danger" className="mb-4" /> : null}

        {!selectedRun && !loading ? (
          <EmptyState
            title="暂无选股结果"
            description="点击左侧“立即扫描并推送飞书”，生成收盘后选股结果。"
          />
        ) : null}

        {selectedRun ? (
          <div className="space-y-4">
            {PENDING_STATUSES.has(selectedRun.status) ? (
              <InlineAlert
                title="后台处理中"
                message="候选股已经生成，系统正在补充消息面、次日操作手册、回测和优化总结，页面会自动刷新。"
                variant="info"
              />
            ) : null}

            <Card title={selectedRun.strategyName} subtitle={selectedRun.scanDate || 'Latest'}>
              <div className="grid gap-3 md:grid-cols-5">
                <Metric title="扫描股票" value={selectedRun.totalScanned} />
                <Metric title="命中条件" value={selectedRun.matchedCount} />
                <Metric title="入选候选" value={selectedRun.selectedCount} />
                <Metric
                  title="A股情绪"
                  value={String(
                    (selectedRun.marketSnapshot as Record<string, unknown> | undefined)?.regime || '中性',
                  )}
                />
                <Metric title="状态" value={formatRunStatus(selectedRun.status)} />
              </div>
            </Card>

            <div className="grid gap-4 xl:grid-cols-[1.35fr_0.9fr]">
              <Card title="扫描摘要" subtitle="Summary">
                <Markdown remarkPlugins={[remarkGfm]}>{selectedRun.summaryMarkdown || ''}</Markdown>
              </Card>
              <OptimizationSummaryCard summary={optimizationSummary} />
            </div>

            <div className="space-y-4">
              {selectedRun.candidates?.length ? (
                selectedRun.candidates.map((candidate) => (
                  <CandidateCard key={candidate.id} candidate={candidate} />
                ))
              ) : (
                <EmptyState
                  title="本次没有符合条件的候选股"
                  description="可以适当下调最低分数、放宽倍量系数，或切换到更宽松的策略后重新扫描。"
                />
              )}
            </div>
          </div>
        ) : null}
      </main>
    </div>
  );
};

const Metric: React.FC<{ title: string; value: string | number }> = ({ title, value }) => (
  <div className="rounded-2xl border border-border/60 bg-card/50 px-4 py-3">
    <p className="text-xs uppercase tracking-[0.24em] text-secondary-text">{title}</p>
    <p className="mt-2 text-lg font-semibold text-foreground">{value}</p>
  </div>
);

const StatusBadge: React.FC<{ status: string }> = ({ status }) => {
  const tone =
    status === 'completed'
      ? 'border-success/30 bg-success/10 text-success'
      : status === 'failed'
        ? 'border-danger/30 bg-danger/10 text-danger'
        : 'border-cyan/30 bg-cyan/10 text-cyan';
  return (
    <span className={`rounded-full border px-2.5 py-1 text-[11px] font-medium ${tone}`}>
      {formatRunStatus(status)}
    </span>
  );
};

const getSectorLabel = (value: unknown): string | null => {
  if (!value || typeof value !== 'object') return null;
  const record = value as Record<string, unknown>;
  const name = typeof record.name === 'string' ? record.name : '';
  if (!name) return null;
  const rank = typeof record.rank === 'number' ? record.rank : null;
  const source = typeof record.source === 'string' ? record.source : '';
  const suffix = source === 'peer_capital_proxy'
    ? '资金代理'
    : source === 'peer_momentum'
      ? '候选共振'
      : '官方板块';
  return rank ? `${name} · #${rank} · ${suffix}` : `${name} · ${suffix}`;
};

const CandidateCard: React.FC<{ candidate: PickerCandidate }> = ({ candidate }) => (
  <Card
    title={`${candidate.rank}. ${candidate.name} (${candidate.code})`}
    subtitle={`${candidate.setupType} | score ${candidate.score}`}
  >
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_320px]">
      <div className="space-y-3">
        <p className="text-sm leading-6 text-secondary-text">{candidate.analysisSummary}</p>
        {Array.isArray(candidate.metrics?.themeBoards) && candidate.metrics.themeBoards.length ? (
          <div className="space-y-2">
            <p className="text-xs uppercase tracking-[0.18em] text-secondary-text/80">所属板块</p>
            <div className="flex flex-wrap gap-2">
              {candidate.metrics.themeBoards.slice(0, 6).map((board: unknown) => (
                <span
                  key={String(board)}
                  className="rounded-full border border-border/60 bg-card/60 px-3 py-1 text-xs text-secondary-text"
                >
                  {String(board)}
                </span>
              ))}
            </div>
          </div>
        ) : null}
        {getSectorLabel(candidate.metrics?.themeHotSector) || getSectorLabel(candidate.metrics?.themeHotCapitalSector) ? (
          <div className="grid gap-2 md:grid-cols-2">
            {getSectorLabel(candidate.metrics?.themeHotSector) ? (
              <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/8 px-3 py-2 text-xs text-emerald-300">
                <span className="mr-2 text-[10px] uppercase tracking-[0.18em] text-emerald-200/80">强势板块</span>
                {getSectorLabel(candidate.metrics?.themeHotSector)}
              </div>
            ) : null}
            {getSectorLabel(candidate.metrics?.themeHotCapitalSector) ? (
              <div className="rounded-2xl border border-cyan/20 bg-cyan/10 px-3 py-2 text-xs text-cyan">
                <span className="mr-2 text-[10px] uppercase tracking-[0.18em] text-cyan/80">资金板块</span>
                {getSectorLabel(candidate.metrics?.themeHotCapitalSector)}
              </div>
            ) : null}
          </div>
        ) : null}
        {candidate.reasons.length ? (
          <div className="flex flex-wrap gap-2">
            {candidate.reasons.map((reason) => (
              <span
                key={reason}
                className="rounded-full border border-cyan/20 bg-cyan/10 px-3 py-1 text-xs text-cyan"
              >
                {reason}
              </span>
            ))}
          </div>
        ) : null}
        <div className="grid gap-3 md:grid-cols-2">
          <Metric title="止损" value={candidate.stopLoss ?? '-'} />
          <Metric title="止盈" value={candidate.takeProfit ?? '-'} />
        </div>
        <div className="rounded-2xl border border-border/60 bg-card/50 px-4 py-4">
          <p className="mb-3 text-sm font-semibold text-foreground">次日操作手册</p>
          <Markdown remarkPlugins={[remarkGfm]}>
            {candidate.actionPlanMarkdown || '该票暂未生成次日操作手册。'}
          </Markdown>
        </div>
      </div>

      <div className="space-y-3">
        <Card title="指标快照" subtitle="关键信息" padding="sm">
          <MetricsSummary metrics={candidate.metrics} />
        </Card>
      </div>
    </div>
  </Card>
);

const METRIC_HIGHLIGHT_KEYS = [
  'themeStrengthLabel',
  'themePrimaryBoards',
  'themeHotSector',
  'themeHotCapitalSector',
  'themeScoreDelta',
  'themeFinalScore',
  'stockMainNetInflow',
  'marketScore',
  'pctChg',
  'amountRatio',
  'volumeSpikeFactor',
  'distanceToMa20Pct',
  'distanceToMa5Pct',
  'atrPct',
  'institutionStyle',
  'weakMoneyFollow',
] as const;

const MetricsSummary: React.FC<{ metrics: Record<string, unknown> }> = ({ metrics }) => {
  const entries = buildMetricHighlights(metrics);
  if (!entries.length) {
    return <p className="text-sm text-secondary-text">暂无指标快照。</p>;
  }

  return (
    <div className="space-y-2 rounded-2xl border border-border/60 bg-card/40 px-3 py-3">
      {entries.map(([label, value]) => (
        <div key={label} className="border-b border-border/40 pb-2 last:border-b-0 last:pb-0">
          <p className="text-[11px] uppercase tracking-[0.16em] text-secondary-text">{label}</p>
          <p className="mt-1 text-sm leading-6 text-foreground">{value}</p>
        </div>
      ))}
    </div>
  );
};

const MetricsTable: React.FC<{ entries: Array<[string, string]> }> = ({ entries }) => {
  if (!entries.length) {
    return <p className="text-sm text-secondary-text">暂无数据。</p>;
  }

  return (
    <div className="space-y-2 rounded-2xl border border-border/60 bg-card/40 px-3 py-3">
      {entries.map(([label, value]) => (
        <div key={label} className="border-b border-border/40 pb-2 last:border-b-0 last:pb-0">
          <p className="text-[11px] uppercase tracking-[0.16em] text-secondary-text">{label}</p>
          <p className="mt-1 text-sm leading-6 text-foreground">{value}</p>
        </div>
      ))}
    </div>
  );
};

const OptimizationSummaryCard: React.FC<{
  summary: ReturnType<typeof buildOptimizationSummary>;
}> = ({ summary }) => (
  <Card title="回测优化总结" subtitle="Optimization" className="space-y-4">
    {summary.available ? (
      <>
        <div className="grid gap-3 sm:grid-cols-2">
          <Metric title="状态" value={summary.statusLabel} />
          <Metric title="样本数" value={summary.sampleCount} />
          <Metric title="回看窗口" value={`${summary.lookbackDays} 天`} />
          <Metric title="评估周期" value={`T+${summary.horizonDays}`} />
        </div>

        {summary.paragraphs.map((paragraph) => (
          <p key={paragraph} className="text-sm leading-6 text-secondary-text">
            {paragraph}
          </p>
        ))}

        {summary.params.length ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">建议参数</p>
            <MetricsTable entries={summary.params} />
          </div>
        ) : null}

        {summary.metrics.length ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">关键指标</p>
            <MetricsTable entries={summary.metrics} />
          </div>
        ) : null}

        {summary.diagnosisSummary ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">LLM 复盘结论</p>
            <p className="text-sm leading-6 text-secondary-text">{summary.diagnosisSummary}</p>
          </div>
        ) : null}

        {summary.parameterAdjustments.length ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">参数调整建议</p>
            <div className="space-y-2">
              {summary.parameterAdjustments.map((item) => (
                <div key={`${item.name}-${item.direction}`} className="rounded-2xl border border-border/60 bg-card/50 px-4 py-3">
                  <p className="text-sm font-medium text-foreground">
                    {item.name} · {item.direction}
                  </p>
                  <p className="mt-1 text-sm leading-6 text-secondary-text">{item.rationale}</p>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {summary.factorHypotheses.length ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">新因子假设</p>
            <div className="space-y-2">
              {summary.factorHypotheses.map((item) => (
                <div key={item.factor} className="rounded-2xl border border-border/60 bg-card/50 px-4 py-3">
                  <p className="text-sm font-medium text-foreground">{item.factor}</p>
                  <p className="mt-1 text-sm leading-6 text-secondary-text">{item.hypothesis}</p>
                  <p className="mt-1 text-xs text-secondary-text">预期效果：{item.expectedEffect}</p>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {summary.controlRules.length ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">控制规则</p>
            <div className="space-y-2">
              {summary.controlRules.map((item) => (
                <p key={item} className="rounded-2xl border border-border/60 bg-card/50 px-4 py-3 text-sm leading-6 text-secondary-text">
                  {item}
                </p>
              ))}
            </div>
          </div>
        ) : null}

        {summary.nextWeekExperiments.length ? (
          <div className="space-y-2">
            <p className="text-sm font-semibold text-foreground">下周实验计划</p>
            <div className="space-y-2">
              {summary.nextWeekExperiments.map((item) => (
                <div key={item.experiment} className="rounded-2xl border border-border/60 bg-card/50 px-4 py-3">
                  <p className="text-sm font-medium text-foreground">{item.experiment}</p>
                  <p className="mt-1 text-sm text-secondary-text">成功指标：{item.successMetric}</p>
                  <p className="mt-1 text-sm text-secondary-text">停止条件：{item.stopCondition}</p>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </>
    ) : (
      <EmptyState
        title="暂无优化总结"
        description="本次回测样本还不够，后续积累足够样本后这里会自动展示优化建议。"
        className="border-none bg-transparent px-0 py-0 shadow-none"
      />
    )}
  </Card>
);

function normalizeParams(params: unknown): StrategyParamValues {
  const entries = objectEntries(params);
  return Object.fromEntries(entries.map(([key, value]) => [key, value == null ? '' : String(value)]));
}

function normalizeOutgoingParams(params: StrategyParamValues): Record<string, number> {
  const outgoing: Record<string, number> = {};
  Object.entries(params).forEach(([key, value]) => {
    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
      outgoing[key] = numeric;
    }
  });
  return outgoing;
}

function objectEntries(input: unknown): Array<[string, unknown]> {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return [];
  }
  return Object.entries(input as Record<string, unknown>);
}

function buildMetricHighlights(metrics: Record<string, unknown> | undefined): Array<[string, string]> {
  if (!metrics) {
    return [];
  }
  return METRIC_HIGHLIGHT_KEYS
    .map((key) => {
      const value = metrics[key];
      const rendered = formatMetricSummaryValue(key, value);
      if (!rendered) {
        return null;
      }
      return [METRIC_LABELS[key] ?? key, rendered] as [string, string];
    })
    .filter((item): item is [string, string] => Boolean(item));
}

function formatCellValue(value: unknown, suffix = ''): string {
  if (value == null) {
    return '-';
  }
  if (typeof value === 'boolean') {
    return value ? '是' : '否';
  }
  if (typeof value === 'number') {
    const rendered = Number.isInteger(value) ? String(value) : value.toFixed(2);
    return suffix ? `${rendered}${suffix}` : rendered;
  }
  if (typeof value === 'string') {
    return value;
  }
  return JSON.stringify(value);
}

function formatMetricSummaryValue(key: string, value: unknown): string | null {
  if (value == null || value === '') {
    return null;
  }
  if (key === 'themePrimaryBoards' && Array.isArray(value)) {
    const boards = value.map((item) => String(item)).filter(Boolean);
    return boards.length ? boards.join(' / ') : null;
  }
  if (key === 'themeHotSector' || key === 'themeHotCapitalSector') {
    return getSectorLabel(value);
  }
  if (key === 'stockMainNetInflow' && typeof value === 'number') {
    return formatMoneyFlow(value);
  }
  if (key === 'themeScoreDelta' || key === 'pctChg' || key === 'distanceToMa20Pct' || key === 'distanceToMa5Pct' || key === 'atrPct') {
    return `${Number(value).toFixed(2)}%`;
  }
  if (key === 'amountRatio' || key === 'volumeSpikeFactor' || key === 'themeFinalScore' || key === 'marketScore') {
    return Number(value).toFixed(2);
  }
  if (typeof value === 'boolean') {
    return value ? '是' : '否';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(2);
  }
  if (typeof value === 'string') {
    return value;
  }
  return null;
}

function formatMoneyFlow(value: number): string {
  const absValue = Math.abs(value);
  if (absValue >= 100000000) {
    return `${(value / 100000000).toFixed(2)} 亿元`;
  }
  if (absValue >= 10000) {
    return `${(value / 10000).toFixed(2)} 万元`;
  }
  return `${value.toFixed(0)} 元`;
}

function formatRunStatus(status: string): string {
  if (status === 'completed') {
    return '已完成';
  }
  if (status === 'failed') {
    return '失败';
  }
  if (status === 'running') {
    return '扫描中';
  }
  if (status === 'enriching') {
    return '补充中';
  }
  if (status === 'queued') {
    return '排队中';
  }
  return '处理中';
}

function buildOptimizationSummary(optimization: unknown) {
  const entries = objectEntries(optimization);
  const optimizationObject = Object.fromEntries(entries);
  const params = objectEntries(optimizationObject.params);
  const metrics = objectEntries(optimizationObject.metrics);
  const llmReview = optimizationObject.llm_review as Record<string, unknown> | undefined;
  const metricsObject = (optimizationObject.metrics as Record<string, unknown> | undefined) ?? {};
  const status = typeof optimizationObject.status === 'string' ? optimizationObject.status : 'unknown';
  const sampleCount = Number(metricsObject.sample_count ?? 0);
  const lookbackDays = Number(optimizationObject.lookback_days ?? 0);
  const horizonDays = Number(optimizationObject.selected_horizon_days ?? 0);
  const avgReturn = Number(metricsObject.avg_return_pct ?? 0);
  const winRate = Number(metricsObject.win_rate_pct ?? 0);
  const llmSummaryRaw = typeof metricsObject.llm_summary === 'string' ? metricsObject.llm_summary : '';

  const paragraphs: string[] = [];
  if (status === 'completed') {
    paragraphs.push(
      `最近 ${lookbackDays || 90} 天回测中，当前参数组合样本数为 ${sampleCount}，主要参考周期是 T+${horizonDays || 5}。`,
    );
    paragraphs.push(
      `该组参数的平均收益约为 ${Number.isFinite(avgReturn) ? avgReturn.toFixed(2) : '0.00'}%，胜率约为 ${Number.isFinite(winRate) ? winRate.toFixed(2) : '0.00'}%。`,
    );
    paragraphs.push('当前优化器只会调整量化阈值参数，不会自动新增或删除量化因子；若要升级策略因子，需要改策略逻辑或 YAML。');
  } else if (status === 'insufficient_data') {
    paragraphs.push('历史样本暂时不足，当前展示的是默认参数或最近一次可用参数。');
  } else {
    paragraphs.push('优化任务尚未形成稳定结论，建议继续积累样本后再观察。');
  }

  const keyParamOrder = [
    'preferred_setup_type',
    'min_score_threshold',
    'volume_spike_multiplier',
    'max_ma20_distance_pct',
    'market_score_floor',
    'pullback_min_score_threshold',
    'breakout_min_score_threshold',
  ];
  const keyMetricOrder = [
    'avg_return_pct',
    'win_rate_pct',
    'avg_max_drawdown_pct',
    'worst_drawdown_pct',
    'profit_factor',
  ];

  const paramsMap = new Map(params);
  const metricsMap = new Map(metrics);
  const diagnosisSummary = sanitizeOptimizationNarrative(
    typeof llmReview?.diagnosis_summary === 'string' ? llmReview.diagnosis_summary : llmSummaryRaw,
  );

  return {
    available: entries.length > 0,
    hasParams: params.length > 0,
    statusLabel: formatOptimizationStatus(status),
    sampleCount: Number.isFinite(sampleCount) ? sampleCount : 0,
    lookbackDays: Number.isFinite(lookbackDays) && lookbackDays > 0 ? lookbackDays : 90,
    horizonDays: Number.isFinite(horizonDays) && horizonDays > 0 ? horizonDays : 5,
    paragraphs,
    params: keyParamOrder
      .filter((key) => paramsMap.has(key))
      .slice(0, 4)
      .map((key) => [PARAM_LABELS[key] ?? key, formatCellValue(paramsMap.get(key))] as [string, string]),
    metrics: keyMetricOrder
      .filter((key) => metricsMap.has(key))
      .slice(0, 4)
      .map((key) => [METRIC_LABELS[key] ?? key, formatCellValue(metricsMap.get(key))] as [string, string]),
    diagnosisSummary,
    parameterAdjustments: normalizeReviewAdjustments(llmReview?.parameter_adjustments),
    factorHypotheses: normalizeFactorHypotheses(llmReview?.factor_hypotheses),
    controlRules: normalizeStringList(llmReview?.control_rules),
    nextWeekExperiments: normalizeExperiments(llmReview?.next_week_experiments),
  };
}

function sanitizeOptimizationNarrative(value: string): string {
  const raw = String(value || '').trim();
  if (!raw) {
    return '';
  }
  let normalized = raw;
  if ((normalized.startsWith('{') && normalized.endsWith('}')) || (normalized.startsWith('[') && normalized.endsWith(']'))) {
    try {
      const parsed = JSON.parse(normalized) as Record<string, unknown>;
      if (typeof parsed.summary === 'string') {
        normalized = parsed.summary;
      } else if (typeof parsed.diagnosis_summary === 'string') {
        normalized = parsed.diagnosis_summary;
      } else {
        normalized = '';
      }
    } catch {
      normalized = normalized.replace(/^[\[{]\s*/, '').replace(/\s*[\]}]$/, '');
    }
  }
  normalized = normalized
    .replace(/```json|```/gi, ' ')
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .replace(/^["']|["']$/g, '')
    .trim();
  if (normalized.length > 180) {
    normalized = `${normalized.slice(0, 180).trim()}...`;
  }
  return normalized;
}

function normalizeReviewAdjustments(value: unknown) {
  if (!Array.isArray(value)) {
    return [] as Array<{ name: string; direction: string; rationale: string }>;
  }
  return value
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }
      const record = item as Record<string, unknown>;
      return {
        name: String(record.name ?? ''),
        direction: String(record.direction ?? ''),
        rationale: String(record.rationale ?? ''),
      };
    })
    .filter((item): item is { name: string; direction: string; rationale: string } => Boolean(item?.name));
}

function normalizeFactorHypotheses(value: unknown) {
  if (!Array.isArray(value)) {
    return [] as Array<{ factor: string; hypothesis: string; expectedEffect: string }>;
  }
  return value
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }
      const record = item as Record<string, unknown>;
      return {
        factor: String(record.factor ?? ''),
        hypothesis: String(record.hypothesis ?? ''),
        expectedEffect: String(record.expected_effect ?? ''),
      };
    })
    .filter((item): item is { factor: string; hypothesis: string; expectedEffect: string } => Boolean(item?.factor));
}

function normalizeExperiments(value: unknown) {
  if (!Array.isArray(value)) {
    return [] as Array<{ experiment: string; successMetric: string; stopCondition: string }>;
  }
  return value
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }
      const record = item as Record<string, unknown>;
      return {
        experiment: String(record.experiment ?? ''),
        successMetric: String(record.success_metric ?? ''),
        stopCondition: String(record.stop_condition ?? ''),
      };
    })
    .filter((item): item is { experiment: string; successMetric: string; stopCondition: string } => Boolean(item?.experiment));
}

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => String(item ?? '').trim()).filter(Boolean);
}

function formatOptimizationStatus(status: string): string {
  if (status === 'completed') {
    return '已完成';
  }
  if (status === 'insufficient_data') {
    return '样本不足';
  }
  return '处理中';
}

export default StockPickerPage;
