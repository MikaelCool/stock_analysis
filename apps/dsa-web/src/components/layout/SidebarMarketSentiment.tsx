import React, { useEffect, useState } from 'react';
import type { PickerMarketSentiment } from '../../api/stockPicker';
import { stockPickerApi } from '../../api/stockPicker';

type SidebarMarketSentimentProps = {
  compact?: boolean;
};

export const SidebarMarketSentiment: React.FC<SidebarMarketSentimentProps> = ({ compact = false }) => {
  const [marketSentiment, setMarketSentiment] = useState<PickerMarketSentiment | null>(null);

  useEffect(() => {
    let cancelled = false;

    const loadMarketSentiment = async () => {
      try {
        const payload = await stockPickerApi.getMarketSentiment();
        if (!cancelled) {
          setMarketSentiment(payload);
        }
      } catch {
        if (!cancelled) {
          setMarketSentiment(null);
        }
      }
    };

    void loadMarketSentiment();
    const timer = window.setInterval(() => {
      if (document.hidden) return;
      void loadMarketSentiment();
    }, 60000);

    const onVisibilityChange = () => {
      if (!document.hidden) {
        void loadMarketSentiment();
      }
    };
    document.addEventListener('visibilitychange', onVisibilityChange);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, []);

  return (
    <div className="rounded-[1.5rem] border border-[var(--shell-sidebar-border)] bg-card/72 p-3 shadow-soft-card backdrop-blur-sm">
      <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-secondary-text">Market</p>
      <p className="mt-2 text-sm font-semibold text-foreground">
        {marketSentiment?.market ?? '沪A大盘情绪'}
      </p>
      <div className="mt-2 flex items-end gap-2">
        <span className="text-2xl font-semibold text-foreground">
          {marketSentiment ? marketSentiment.score.toFixed(0) : '--'}
        </span>
        <span className="pb-1 text-xs text-secondary-text">
          {marketSentiment?.regime ?? '加载中'}
        </span>
      </div>
      <p className={`mt-2 text-xs leading-5 text-secondary-text ${compact ? 'line-clamp-3' : ''}`}>
        {marketSentiment?.summary ?? '正在获取最新大盘情绪分数。'}
      </p>
      {marketSentiment?.updatedAt ? (
        <p className="mt-2 text-[11px] text-secondary-text/80">
          更新于 {new Date(marketSentiment.updatedAt).toLocaleTimeString('zh-CN', { hour12: false })}
        </p>
      ) : null}
    </div>
  );
};
