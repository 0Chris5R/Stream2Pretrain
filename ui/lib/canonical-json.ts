/**
 * RFC 8785 / Python `json.dumps(..., sort_keys=True, separators=(',', ':'))`
 * compatible canonical serializer.
 *
 * The Python signer in `processor/decon_gate.py` recursively sorts every
 * object's keys before signing. The UI's verify route must produce the
 * exact same byte sequence to reconstruct the signed payload, otherwise
 * Ed25519 verification fails at the first non-trivial nested object
 * (notably the `per_benchmark_hits` map in DeconAttestation).
 *
 * Notes:
 *   - We sort by Unicode code-point (the default String.compare order),
 *     matching Python's `sorted(dict.keys())` behaviour for ASCII keys.
 *   - Numbers are serialised via JSON.stringify on the leaf, matching the
 *     RFC 8785 behaviour for integers and IEEE-754 doubles in our wire.
 *   - `undefined` properties are dropped, matching `JSON.stringify`.
 *   - `null`, booleans, strings, arrays, and plain objects are supported.
 *     Dates / Maps / Sets must be converted by the caller first.
 */
export function canonicalJSONStringify(value: unknown): string {
  if (typeof value === 'bigint') {
    return value.toString(10);
  }
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const parts = value.map((item) =>
      item === undefined ? 'null' : canonicalJSONStringify(item),
    );
    return `[${parts.join(',')}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj)
    .filter((k) => obj[k] !== undefined)
    .sort();
  const parts = keys.map((k) => `${JSON.stringify(k)}:${canonicalJSONStringify(obj[k])}`);
  return `{${parts.join(',')}}`;
}
