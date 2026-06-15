// Stream2Pretrain - load test for the submit API.
//
// Profile:
//   - 10s ramp from 0 to 100 RPS
//   - 60s steady at 100 RPS
//   - 10s ramp down
//
// Assertions (k6 thresholds):
//   - 99% of POST /submit complete in < 2s
//   - error rate < 1%
//
// Usage:
//   k6 run -e SUBMIT_URL=http://localhost:8000/submit tests/load/k6_submit.js
//
// Notes:
//   - The submit API fetches each URL synchronously, so the upstream's latency
//     is part of the measured time. For pure-pipeline benchmarking, point the
//     test at a local nginx serving a static page rather than at arxiv.
//   - Set FEED to a SourceFeed name declared in the cluster; defaults to
//     'manual-submit' which is always allowlisted.

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const submitUrl = __ENV.SUBMIT_URL || "http://localhost:8000/submit";
const feed = __ENV.FEED || "manual-submit";
// Default URL list. Override with -e URLS="https://a,https://b,..."
const urls = (
  __ENV.URLS ||
  [
    "https://export.arxiv.org/abs/2402.00159",
    "https://export.arxiv.org/abs/2406.17557",
    "https://export.arxiv.org/abs/2406.11794",
    "https://huggingface.co/blog/leaderboard-medicalllm",
  ].join(",")
).split(",");

const submitErrors = new Counter("s2p_submit_errors");
const submitLatency = new Trend("s2p_submit_latency_ms", true);

export const options = {
  scenarios: {
    submit_burst: {
      executor: "ramping-arrival-rate",
      startRate: 0,
      timeUnit: "1s",
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { duration: "10s", target: 100 },
        { duration: "60s", target: 100 },
        { duration: "10s", target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(99)<2000"],
    s2p_submit_errors: ["count<50"],
  },
};

export default function () {
  const url = urls[Math.floor(Math.random() * urls.length)];
  const payload = JSON.stringify({ url, source_feed: feed });
  const res = http.post(submitUrl, payload, {
    headers: { "Content-Type": "application/json" },
    timeout: "10s",
  });

  submitLatency.add(res.timings.duration);
  const ok = check(res, {
    "status is 201": (r) => r.status === 201,
    "doc_id present": (r) => !!(r.json() && r.json().doc_id),
  });
  if (!ok) {
    submitErrors.add(1);
  }
}

export function handleSummary(data) {
  return {
    stdout:
      "Stream2Pretrain submit-API load summary\n" +
      JSON.stringify(
        {
          requests: data.metrics.http_reqs && data.metrics.http_reqs.values.count,
          errors:
            data.metrics.s2p_submit_errors &&
            data.metrics.s2p_submit_errors.values.count,
          p99_ms:
            data.metrics.http_req_duration &&
            data.metrics.http_req_duration.values["p(99)"],
        },
        null,
        2
      ) +
      "\n",
  };
}
