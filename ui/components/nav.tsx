'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
  Activity,
  BookOpen,
  Database,
  FileSignature,
  Layers,
  ListChecks,
  Radio,
} from 'lucide-react';

import { cn } from '@/lib/utils';

const items = [
  { href: '/dashboard', label: 'Dashboard', icon: Activity },
  { href: '/documents', label: 'Documents', icon: BookOpen },
  { href: '/sources', label: 'Sources', icon: ListChecks },
  { href: '/decon', label: 'Benchmark Safety', icon: FileSignature },
  { href: '/datasets', label: 'Datasets', icon: Database },
  { href: '/mixture', label: 'Mixture', icon: Layers },
] as const;

export function TopNav() {
  const pathname = usePathname();
  return (
    <nav className="sticky top-0 z-30 border-b bg-background/85 backdrop-blur">
      <div className="container flex h-14 items-center gap-2 overflow-x-auto sm:gap-6">
        <Link href="/dashboard" className="flex shrink-0 items-center gap-2 font-semibold">
          <Radio className="h-5 w-5 text-primary" />
          <span className="hidden sm:inline">Stream2Pretrain</span>
        </Link>
        <ul className="flex shrink-0 items-center gap-1 text-sm">
          {items.map((item) => {
            const Icon = item.icon;
            const active = pathname === item.href;
            return (
              <li key={item.href}>
                <Link
                  href={item.href}
                  className={cn(
                    'inline-flex items-center gap-2 whitespace-nowrap rounded-md px-3 py-1.5 transition-colors hover:bg-accent hover:text-accent-foreground',
                    active && 'bg-accent text-accent-foreground',
                  )}
                >
                  <Icon className="h-4 w-4" />
                  {item.label}
                </Link>
              </li>
            );
          })}
        </ul>
      </div>
    </nav>
  );
}
