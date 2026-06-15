/**
 * POST /api/decon/verify
 *
 * Server-side verification of a Decon-Gate attestation. The default path
 * matches the Python signer in `processor/decon_gate.py`:
 *
 *   1. Reconstruct the signed payload bytes with the SAME canonical JSON
 *      serializer the Python side uses (recursive `sort_keys=True`,
 *      `separators=(',', ':')`).
 *   2. Extract the Ed25519 public key from the attached self-signed X.509
 *      certificate (`signer_cert`).
 *   3. Run `crypto.verify('ed25519', payload, publicKey, signature)`.
 *
 * Set `S2P_USE_COSIGN=1` to opt into a cosign verify-blob shell-out for
 * attestations produced by the Sigstore keyless backend. We never run
 * cosign by default because a misconfigured Fulcio backend would block
 * the request for tens of seconds.
 */
import { NextResponse } from 'next/server';
import crypto, { type KeyObject } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { z } from 'zod';

import { DeconAttestationSchema } from '@/lib/schemas';
import { canonicalJSONStringify } from '@/lib/canonical-json';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const RequestSchema = z.object({ snapshot_id: z.number().int().nonnegative() });

const DECON_GATE_URL = process.env.DECON_GATE_URL ?? 'http://decon-gate.stream2pretrain.svc:8081';
const COSIGN_BIN = process.env.COSIGN_BIN ?? 'cosign';
const USE_COSIGN = process.env.S2P_USE_COSIGN === '1';

interface ExecResult {
  code: number;
  stdout: string;
  stderr: string;
}

function exec(cmd: string, args: string[], cwd: string): Promise<ExecResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => (stdout += chunk.toString()));
    child.stderr.on('data', (chunk) => (stderr += chunk.toString()));
    child.on('error', reject);
    child.on('close', (code) => resolve({ code: code ?? -1, stdout, stderr }));
  });
}

function publicKeyFromCert(certPem: string): KeyObject {
  const cert = new crypto.X509Certificate(certPem);
  return cert.publicKey;
}

function verifyEd25519(payload: Buffer, signatureB64: string, certPem: string): boolean {
  const publicKey = publicKeyFromCert(certPem);
  const signature = Buffer.from(signatureB64, 'base64');
  // Ed25519 signatures are exactly 64 bytes; reject anything else fast.
  if (signature.length !== 64) return false;
  return crypto.verify(null, payload, publicKey, signature);
}

export async function POST(req: Request): Promise<NextResponse> {
  const body = await req.json().catch(() => null);
  const parsed = RequestSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ detail: 'invalid request body' }, { status: 400 });
  }
  const { snapshot_id } = parsed.data;

  let attestationResp: Response;
  try {
    attestationResp = await fetch(`${DECON_GATE_URL}/attestations/${snapshot_id}`, {
      cache: 'no-store',
    });
  } catch {
    return NextResponse.json({ detail: 'decon-gate unreachable' }, { status: 502 });
  }

  if (!attestationResp.ok) {
    return NextResponse.json(
      { detail: `decon-gate returned ${attestationResp.status}` },
      { status: 502 },
    );
  }

  const attestationJson = await attestationResp.json();
  const attestation = DeconAttestationSchema.safeParse(attestationJson);
  if (!attestation.success) {
    return NextResponse.json(
      { detail: `attestation failed schema validation: ${attestation.error.message}` },
      { status: 502 },
    );
  }

  const { signature, signer_cert, ...payload } = attestation.data;
  const canonical = Buffer.from(canonicalJSONStringify(payload), 'utf8');
  const verified_at = new Date().toISOString();
  const signerSubject = extractSubject(signer_cert);

  if (USE_COSIGN) {
    const dir = await mkdtemp(path.join(tmpdir(), 's2p-verify-'));
    try {
      const blobPath = path.join(dir, 'attestation.json');
      const sigPath = path.join(dir, 'attestation.sig');
      const certPath = path.join(dir, 'signer.pem');
      await writeFile(blobPath, canonical);
      await writeFile(sigPath, signature);
      await writeFile(certPath, signer_cert);
      const result = await exec(
        COSIGN_BIN,
        [
          'verify-blob',
          '--certificate',
          certPath,
          '--signature',
          sigPath,
          '--insecure-ignore-tlog',
          blobPath,
        ],
        dir,
      );
      const ok = result.code === 0;
      return NextResponse.json({
        ok,
        snapshot_id,
        backend: 'cosign',
        message: ok
          ? `cosign verify-blob: signature valid for snapshot ${snapshot_id}`
          : `cosign exited ${result.code}`,
        signer_subject: signerSubject,
        verified_at,
      });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }

  let ok = false;
  let message: string;
  try {
    ok = verifyEd25519(canonical, signature, signer_cert);
    message = ok
      ? `ed25519 verify: signature valid for snapshot ${snapshot_id}`
      : `ed25519 verify: signature invalid for snapshot ${snapshot_id}`;
  } catch (err) {
    message = `ed25519 verify failed: ${(err as Error).message}`;
  }

  return NextResponse.json({
    ok,
    snapshot_id,
    backend: 'ed25519',
    message,
    signer_subject: signerSubject,
    verified_at,
  });
}

function extractSubject(pem: string): string | null {
  try {
    const cert = new crypto.X509Certificate(pem);
    return cert.subject.replace(/\n/g, ', ');
  } catch {
    const match = pem.match(/Subject: (.+)/);
    return match ? match[1].trim() : null;
  }
}
