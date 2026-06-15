'use client';

import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { ThroughputPoint } from '@/lib/schemas';

interface Props {
  data: ThroughputPoint[];
  height?: number;
}

/**
 * Stacked area chart of ingested vs curated vs rejected docs/minute.
 * Time axis is rendered as `HH:mm`; the source supplies ISO timestamps.
 */
export function ThroughputTimeline({ data, height = 260 }: Props) {
  const formatted = data.map((p) => ({
    ...p,
    label: new Date(p.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
  }));

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={formatted} margin={{ top: 12, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis dataKey="label" className="text-xs" stroke="currentColor" />
        <YAxis className="text-xs" stroke="currentColor" />
        <Tooltip
          contentStyle={{
            background: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 6,
            fontSize: 12,
          }}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Area
          type="monotone"
          dataKey="ingested"
          stackId="1"
          fill="hsl(var(--primary) / 0.4)"
          stroke="hsl(var(--primary))"
        />
        <Area
          type="monotone"
          dataKey="curated"
          stackId="2"
          fill="hsl(142 71% 45% / 0.4)"
          stroke="hsl(142 71% 45%)"
        />
        <Area
          type="monotone"
          dataKey="rejected"
          stackId="3"
          fill="hsl(var(--destructive) / 0.4)"
          stroke="hsl(var(--destructive))"
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
