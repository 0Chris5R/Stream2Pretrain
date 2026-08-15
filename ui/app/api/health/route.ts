import { NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(): Promise<NextResponse> {
  const localMode = process.env.S2P_LOCAL_MODE === '1';
  return NextResponse.json({
    status: 'ok',
    local_mode: localMode,
    source_control_plane: localMode ? 'local-sourcefeed-scheduler' : 'kubernetes',
    mixture_backend: localMode ? 'future-work' : 'controller',
  });
}
