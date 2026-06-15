'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Activity, Database, FileSignature, Layers, ListChecks, Radio } from 'lucide-react';

import { cn } from '@/lib/utils';

const items = [
  { href: '/', label: 'Home', icon: Radio },
  { href: '/dashboard', label: 'Dashboard', icon: Activity },
  { href: '/sources', label: 'Sources', icon: ListChecks },
  { href: '/decon', label: 'Decon', icon: FileSignature },
  { href: '/as-of', label: 'As-Of', icon: Database },
  { href: '/mixture', label: 'Mixture', icon: Layers },
] as const;

export function TopNav() {
  const pathname = usePathname();
  return (
    <nav className="sticky top-0 z-30 border-b bg-background/85 backdrop-blur">
      <div className="container flex h-14 items-center gap-6">
        <Link href="/" className="flex items-center gap-2 font-semibold">
          <Radio className="h-5 w-5 text-primary" />
          <span>Stream2Pretrain</span>
        </Link>
        <ul className="flex items-center gap-1 text-sm">
          {items.slice(1).map((item) => {
            const Icon = item.icon;
            const active = pathname === item.href;
            return (
              <li key={item.href}>
                <Link
                  href={item.href}
                  className={cn(
                    'inline-flex items-center gap-2 rounded-md px-3 py-1.5 transition-colors hover:bg-accent hover:text-accent-foreground',
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
