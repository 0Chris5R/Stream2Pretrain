import type { Metadata } from 'next';
import './globals.css';

import { Providers } from '@/components/providers';
import { TopNav } from '@/components/nav';

export const metadata: Metadata = {
  title: 'Stream2Pretrain Cockpit',
  description:
    'Streaming-first LLM pretraining data curator: ingest, decontaminate, classify, mixture.',
};

interface RootLayoutProps {
  children: React.ReactNode;
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <html lang="en">
      <body className="min-h-screen overflow-x-hidden bg-background text-foreground">
        <Providers>
          <TopNav />
          <main className="container py-8">{children}</main>
        </Providers>
      </body>
    </html>
  );
}
