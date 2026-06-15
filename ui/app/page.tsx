import Link from 'next/link';
import { Activity, Database, FileSignature, Layers, ListChecks } from 'lucide-react';

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ThroughputSpark } from '@/components/throughput-spark';

const cards = [
  {
    href: '/dashboard',
    title: 'Live dashboard',
    description: 'Per-minute ingest/curate/reject; quality histogram; per-source acceptance.',
    icon: Activity,
  },
  {
    href: '/sources',
    title: 'Source feeds',
    description: 'List, edit, enable/disable SourceFeed CRDs; see poll state and error rate.',
    icon: ListChecks,
  },
  {
    href: '/decon',
    title: 'Decon attestations',
    description: 'Signed contamination attestations; cosign verify in one click.',
    icon: FileSignature,
  },
  {
    href: '/as-of',
    title: 'As-of query',
    description: 'Travel the gold table back in time; mixture by source over the as-of view.',
    icon: Database,
  },
  {
    href: '/mixture',
    title: 'Mixture A/B',
    description: 'Compare two MixtureRecipe branches; perplexity delta of the proxy LM.',
    icon: Layers,
  },
];

export default function HomePage() {
  return (
    <div className="space-y-8">
      <header className="space-y-3">
        <h1 className="text-3xl font-semibold tracking-tight">
          Streaming pretraining data, on demand.
        </h1>
        <p className="max-w-2xl text-muted-foreground">
          Stream2Pretrain ingests AI-research feeds in real time, decontaminates against benchmark
          n-grams with signed per-snapshot attestations, scores quality with FineWeb-Edu, and
          publishes Iceberg-V3 gold partitions you can pin with `as_of(timestamp)`.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Live curated docs/min</CardTitle>
          <CardDescription>SSE stream from Prometheus; reconnects automatically.</CardDescription>
        </CardHeader>
        <CardContent>
          <ThroughputSpark height={80} />
        </CardContent>
      </Card>

      <section className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
        {cards.map((card) => {
          const Icon = card.icon;
          return (
            <Link key={card.href} href={card.href} className="group">
              <Card className="h-full transition-shadow group-hover:shadow-md">
                <CardHeader>
                  <div className="flex items-center gap-2">
                    <Icon className="h-5 w-5 text-primary" />
                    <CardTitle className="text-base">{card.title}</CardTitle>
                  </div>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground">{card.description}</p>
                </CardContent>
              </Card>
            </Link>
          );
        })}
      </section>
    </div>
  );
}
