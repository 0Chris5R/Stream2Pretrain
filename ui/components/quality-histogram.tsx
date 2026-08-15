'use client';

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { QualityHistogram } from '@/lib/schemas';

interface Props {
  data: QualityHistogram;
  height?: number;
  series?: 'buckets' | 'edu_buckets';
}

/**
 * Bar chart for one explicitly named 0..5 quality signal.
 * Buckets are produced by the processor and arrive sorted ascending.
 */
export function QualityHistogramChart({ data, height = 220, series = 'buckets' }: Props) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data[series]} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          dataKey="score"
          tickFormatter={(v) => Number(v).toFixed(1)}
          className="text-xs"
          stroke="currentColor"
        />
        <YAxis allowDecimals={false} className="text-xs" stroke="currentColor" />
        <Tooltip
          contentStyle={{
            background: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 6,
            fontSize: 12,
          }}
          formatter={(value) => [Number(value ?? 0).toLocaleString(), 'documents']}
          labelFormatter={(score) => `score ${Number(score).toFixed(2)}`}
        />
        <Bar dataKey="count" fill="hsl(var(--primary))" radius={[2, 2, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
