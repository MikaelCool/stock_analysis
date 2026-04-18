import type React from 'react';
import { useEffect, useMemo, useState } from 'react';
import { Button, Card, EmptyState, InlineAlert, Select } from '../components/common';
import {
  stockPickerApi,
  type PickerBacktestResultItem,
  type PickerBacktestRunResponse,
  type PickerBacktestSummary,
  type PickerStrategyOption,
} from '../api/stockPicker';

const PAGE_SIZE = 30;

const HORIZON_OPTIONS = [
  { value: '1', label: 'T+1' },
  { value: '3', label: 'T+3' },
  { value: '5', label: 'T+5' },
  { value: '10', label: 'T+10' },
];

const DATE_INPUT_CLASS =
  'input-surface input-focus-glow h-11 w-full rounded-xl border bg-transparent px-4 text-sm transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';

const BacktestPage: React.FC = () => {
  const [strategies, setStrategies] = useState<PickerStrategyOption[]>([]);
  const [strategyId, setStrategyId] = useState('');
  const [horizonDays, setHorizonDays] = useState('5');
  const [scanDateFrom, setScanDateFrom] = useState('');
  const [scanDateTo, setScanDateTo] = useState('');
  const [summary, setSummary] = useState<PickerBacktestSummary | null>(null);
  const [results, setResults] = useState<PickerBacktestResultItem[]>([]);
  const [totalResults, setTotalResults] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runInfo, setRunInfo] = useState<PickerBacktestRunResponse | null>(null);

  useEffect(() => {
    document.title = '选股总回测 - DSA';
  }, []);

  const strategyOptions = useMemo(
    () => [
      { value: '', label: '全部策略' },
      ...strategies.map((item) => ({ value: item.strategyId, label: item.name })),
    ],
    [strategies],
  );

  async function loadPage(nextPage = 1) {
    setLoading(true);
    setError(null);
    try {
      const [nextStrategies, nextSummary, nextResults] = await Promise.all([
        stockPickerApi.getStrategies(),
        stockPickerApi.getBacktestSummary({
          strategyId: strategyId || undefined,
          horizonDays: Number(horizonDays),
          scanDateFrom: scanDateFrom || undefined,
          scanDateTo: scanDateTo || undefined,
          topN: 5,
        }),
        stockPickerApi.getBacktestResults({
          strategyId: strategyId || undefined,
          horizonDays: Number(horizonDays),
          scanDateFrom: scanDateFrom || undefined,
          scanDateTo: scanDateTo || undefined,
          topN: 5,
          page: nextPage,
          limit: PAGE_SIZE,
        }),
      ]);
      setStrategies(nextStrategies);
      setSummary(nextSummary);
      setResults(nextResults.items);
      setTotalResults(nextResults.total);
      setPage(nextResults.page);
    } catch (err) {
      setError(err instanceof Error ? err.message : '选股总回测加载失败');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function handleRefresh() {
    await loadPage(1);
  }

  async function handleRunBacktest() {
    setRunning(true);
    setError(null);
    try {
      const payload = await stockPickerApi.runBacktestRefresh({
        strategyId: strategyId || undefined,
        horizonDays: Number(horizonDays),
        topN: 5,
        maxCandidates: 3000,
      });
      setRunInfo(payload);
      await loadPage(1);
    } catch (err) {
      setError(err instanceof Error ? err.message : '补算选股回测失败');
    } finally {
      setRunning(false);
    }
  }

  const totalPages = Math.max(1, Math.ceil(totalResults / PAGE_SIZE));

  return (
    <div className="space-y-4 px-4 pb-6 pt-4 lg:px-6">
      <Card title="选股策略总回测" subtitle="仅统计每次选股前 5 只候选股">
        <div className="grid gap-3 lg:grid-cols-[15rem_10rem_10rem_10rem_auto_auto]">
          <Select
            label="策略"
            value={strategyId}
            onChange={setStrategyId}
            options={strategyOptions}
            placeholder="全部策略"
          />
          <Select label="周期" value={horizonDays} onChange={setHorizonDays} options={HORIZON_OPTIONS} />
          <div>
            <p className="mb-2 text-sm font-medium text-foreground">开始日期</p>
            <input
              type="date"
              value={scanDateFrom}
              onChange={(event) => setScanDateFrom(event.target.value)}
              className={DATE_INPUT_CLASS}
            />
          </div>
          <div>
            <p className="mb-2 text-sm font-medium text-foreground">结束日期</p>
            <input
              type="date"
              value={scanDateTo}
              onChange={(event) => setScanDateTo(event.target.value)}
              className={DATE_INPUT_CLASS}
            />
          </div>
          <div className="flex items-end">
            <Button onClick={handleRefresh} variant="secondary" className="w-full">
              刷新结果
            </Button>
          </div>
          <div className="flex items-end">
            <Button onClick={handleRunBacktest} isLoading={running} loadingText="补算中..." className="w-full">
              补算回测
            </Button>
          </div>
        </div>

        <p className="mt-3 text-sm leading-6 text-secondary-text">
          这个页面直接统计每日选股 run 中排名前 5 的股票，不再使用旧的问股历史回测表。核心指标包括胜率、平均收益、最大回撤、策略分布和形态拆分。
        </p>
      </Card>

      {error ? <InlineAlert title="回测失败" message={error} variant="danger" /> : null}
      {runInfo ? (
        <InlineAlert
          title="补算完成"
          message={`本次补算处理 ${runInfo.processed} 条，新增完成 ${runInfo.completed} 条，仍待观察 ${runInfo.pending} 条。`}
          variant="success"
        />
      ) : null}

      {loading ? (
        <Card title="加载中" subtitle="Backtest">
          <p className="text-sm text-secondary-text">正在加载选股总回测结果...</p>
        </Card>
      ) : null}

      {!loading && summary ? (
        <>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
            <MetricCard title="胜率" value={formatPct(summary.winRatePct)} />
            <MetricCard title="平均收益" value={formatPct(summary.avgReturnPct)} />
            <MetricCard title="平均最大回撤" value={formatPct(summary.avgMaxDrawdownPct)} />
            <MetricCard title="最差回撤" value={formatPct(summary.worstDrawdownPct)} />
            <MetricCard title="完成 / 总数" value={`${summary.completedCount} / ${summary.totalEvaluations}`} />
          </div>

          <div className="grid gap-4 xl:grid-cols-[0.92fr_1.08fr]">
            <Card title="策略分布" subtitle={`周期 ${formatHorizon(summary.horizonDays)} | 扫描日 ${summary.scanCount}`}>
              {summary.strategyDistribution.length ? (
                <TableCard
                  headers={['策略', '完成 / 总数', '胜率', '平均收益', '最差回撤']}
                  rows={summary.strategyDistribution.map((item) => [
                    item.strategyName,
                    `${item.completedCount} / ${item.totalEvaluations}`,
                    formatPct(item.winRatePct),
                    formatPct(item.avgReturnPct),
                    formatPct(item.worstDrawdownPct),
                  ])}
                />
              ) : (
                <EmptyState
                  title="暂无策略分布"
                  description="当前筛选条件下还没有完成的选股回测。"
                  className="border-none bg-transparent px-0 py-0 shadow-none"
                />
              )}
            </Card>

            <Card title="样本概览" subtitle="Scope">
              <div className="grid gap-3 sm:grid-cols-2">
                <MetricCard title="统计股票数" value={summary.stockCount} compact />
                <MetricCard title="待完成样本" value={summary.pendingCount} compact />
                <MetricCard title="盈利 / 亏损 / 中性" value={`${summary.winCount} / ${summary.lossCount} / ${summary.neutralCount}`} compact />
                <MetricCard title="最近扫描日" value={summary.latestScanDate || '-'} compact />
              </div>
            </Card>
          </div>

          <Card title="形态拆分" subtitle="按突破启动 / 趋势回踩拆开看">
            {summary.setupTypeDistribution.length ? (
              <TableCard
                headers={['形态', '完成 / 总数', '胜率', '平均收益', '最差回撤']}
                rows={summary.setupTypeDistribution.map((item) => [
                  formatSetupType(item.setupType),
                  `${item.completedCount} / ${item.totalEvaluations}`,
                  formatPct(item.winRatePct),
                  formatPct(item.avgReturnPct),
                  formatPct(item.worstDrawdownPct),
                ])}
              />
            ) : (
              <EmptyState
                title="暂无形态拆分"
                description="当前筛选条件下还没有足够的 breakout / pullback 样本。"
                className="border-none bg-transparent px-0 py-0 shadow-none"
              />
            )}
          </Card>

          <Card title="回测明细" subtitle={`第 ${page} 页 / 共 ${totalPages} 页`}>
            {results.length ? (
              <>
                <div className="overflow-hidden rounded-2xl border border-border/60">
                  <table className="w-full border-collapse text-sm">
                    <thead className="bg-card/80 text-secondary-text">
                      <tr>
                        <th className="px-3 py-2 text-left font-medium">扫描日</th>
                        <th className="px-3 py-2 text-left font-medium">策略</th>
                        <th className="px-3 py-2 text-left font-medium">股票</th>
                        <th className="px-3 py-2 text-left font-medium">排名</th>
                        <th className="px-3 py-2 text-left font-medium">形态</th>
                        <th className="px-3 py-2 text-left font-medium">周期</th>
                        <th className="px-3 py-2 text-left font-medium">收益</th>
                        <th className="px-3 py-2 text-left font-medium">回撤</th>
                        <th className="px-3 py-2 text-left font-medium">结果</th>
                        <th className="px-3 py-2 text-left font-medium">状态</th>
                      </tr>
                    </thead>
                    <tbody>
                      {results.map((item) => (
                        <tr key={item.id} className="border-t border-border/60">
                          <td className="px-3 py-3 text-secondary-text">{item.scanDate || '-'}</td>
                          <td className="px-3 py-3 text-secondary-text">{item.strategyName}</td>
                          <td className="px-3 py-3">
                            <p className="font-medium text-foreground">
                              {item.name || item.code} ({item.code})
                            </p>
                          </td>
                          <td className="px-3 py-3 text-secondary-text">{item.rank}</td>
                          <td className="px-3 py-3 text-secondary-text">{formatSetupType(item.setupType)}</td>
                          <td className="px-3 py-3 text-secondary-text">{formatHorizon(item.horizonDays)}</td>
                          <td className={`px-3 py-3 ${Number(item.returnPct || 0) >= 0 ? 'text-success' : 'text-danger'}`}>
                            {formatPct(item.returnPct)}
                          </td>
                          <td className="px-3 py-3 text-secondary-text">{formatPct(item.maxDrawdownPct)}</td>
                          <td className="px-3 py-3 text-secondary-text">{formatOutcome(item.outcome)}</td>
                          <td className="px-3 py-3 text-secondary-text">{formatStatus(item.status)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="mt-4 flex items-center justify-between gap-3">
                  <p className="text-sm text-secondary-text">
                    当前显示 {results.length} 条，累计 {totalResults} 条。
                  </p>
                  <div className="flex gap-2">
                    <Button variant="ghost" onClick={() => void loadPage(page - 1)} disabled={page <= 1 || loading}>
                      上一页
                    </Button>
                    <Button variant="ghost" onClick={() => void loadPage(page + 1)} disabled={page >= totalPages || loading}>
                      下一页
                    </Button>
                  </div>
                </div>
              </>
            ) : (
              <EmptyState
                title="暂无回测明细"
                description="先点击“补算回测”，或者调整筛选条件后再刷新。"
                className="border-none bg-transparent px-0 py-0 shadow-none"
              />
            )}
          </Card>
        </>
      ) : null}
    </div>
  );
};

const MetricCard: React.FC<{ title: string; value: string | number; compact?: boolean }> = ({
  title,
  value,
  compact = false,
}) => (
  <div className={`rounded-2xl border border-border/60 bg-card/50 ${compact ? 'px-4 py-3' : 'px-4 py-4'}`}>
    <p className="text-xs uppercase tracking-[0.22em] text-secondary-text">{title}</p>
    <p className={`${compact ? 'mt-2 text-base' : 'mt-3 text-2xl'} font-semibold text-foreground`}>
      {value}
    </p>
  </div>
);

const TableCard: React.FC<{ headers: string[]; rows: string[][] }> = ({ headers, rows }) => (
  <div className="overflow-hidden rounded-2xl border border-border/60">
    <table className="w-full border-collapse text-sm">
      <thead className="bg-card/80 text-secondary-text">
        <tr>
          {headers.map((header) => (
            <th key={header} className="px-3 py-2 text-left font-medium">
              {header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={`${row[0]}-${index}`} className="border-t border-border/60">
            {row.map((cell, cellIndex) => (
              <td
                key={`${row[0]}-${cellIndex}`}
                className={`px-3 py-3 ${cellIndex === 0 ? 'font-medium text-foreground' : 'text-secondary-text'}`}
              >
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

function formatPct(value?: number | null): string {
  if (value == null || Number.isNaN(value)) {
    return '-';
  }
  return `${value.toFixed(2)}%`;
}

function formatHorizon(value: number): string {
  return `T+${value}`;
}

function formatOutcome(value?: string | null): string {
  if (!value) {
    return '-';
  }
  if (value === 'win') {
    return '盈利';
  }
  if (value === 'loss') {
    return '亏损';
  }
  if (value === 'neutral') {
    return '中性';
  }
  return value;
}

function formatStatus(value: string): string {
  if (value === 'completed') {
    return '已完成';
  }
  if (value === 'pending') {
    return '待完成';
  }
  return value;
}

function formatSetupType(value: string): string {
  if (value === 'pullback') {
    return '趋势回踩';
  }
  if (value === 'breakout') {
    return '突破启动';
  }
  if (value === 'mixed') {
    return '混合';
  }
  return value || '-';
}

export default BacktestPage;
