'use client';

import { useEffect, useRef, useState } from 'react';
import { Line, LineChart, ResponsiveContainer } from 'recharts';

import { ThroughputPointSchema, type ThroughputPoint } from '@/lib/schemas';

const MAX_POINTS = 60;

interface Props {
  /** Endpoint that emits Server-Sent Events with `ThroughputPoint` JSON. */
  url?: string;
  height?: number;
}

/**
 * Rolling 60-point sparkline subscribed to a Server-Sent Events stream.
 * The connection is lazily established and cleaned up on unmount; transient
 * errors are silently retried by the browser.
 */
export function ThroughputSpark({ url = '/api/throughput/sse', height = 60 }: Props) {
  const [points, setPoints] = useState<ThroughputPoint[]>([]);
  const ref = useRef<EventSource | null>(null);

  useEffect(() => {
    const source = new EventSource(url);
    ref.current = source;
    source.addEventListener('message', (ev) => {
      try {
        const parsed = ThroughputPointSchema.safeParse(JSON.parse(ev.data));
        if (!parsed.success) return;
        setPoints((prev) => [...prev.slice(-(MAX_POINTS - 1)), parsed.data]);
      } catch {
        // Ignore malformed frames; the stream is best-effort.
      }
    });
    source.addEventListener('error', () => {
      // EventSource will auto-reconnect; nothing to do here.
    });
    return () => {
      source.close();
      ref.current = null;
    };
  }, [url]);

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={points}>
        <Line
          type="monotone"
          dataKey="curated"
          dot={false}
          isAnimationActive={false}
          stroke="hsl(var(--primary))"
          strokeWidth={1.5}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
