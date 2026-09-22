// k6 load test script — Problem 2 (Concurrency & Graceful Degradation)
// Owner: Person C
// Run with: k6 run tests/load/k6_p2.js
// Requires: client-api running at http://localhost:8080
//           a valid access token set in TOKEN env var

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

// ─────────────────────────────────────────────────────────────
// Custom metrics
// ─────────────────────────────────────────────────────────────
const bmkgLatency = new Trend('bmkg_only_latency', true);
const errorRate   = new Rate('error_rate');

// ─────────────────────────────────────────────────────────────
// Test configuration
// ─────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    // Scenario 1: 50 VUs sustained for 60s (mixed BMKG + PVMBG)
    sustained_load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '60s',
      tags: { scenario: 'mixed' },
    },
  },
  thresholds: {
    // P2 success criteria
    bmkg_only_latency: ['p(95)<300'],    // BMKG-only p95 < 300ms
    error_rate:        ['rate<0.01'],    // error rate < 1% (excl. 429)
    http_req_failed:   ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN    = __ENV.TOKEN    || 'replace-me-with-valid-access-token';

const headers = {
  'Authorization': `Bearer ${TOKEN}`,
  'X-Correlation-ID': 'k6-load-test',
};

// ─────────────────────────────────────────────────────────────
// Main test function
// ─────────────────────────────────────────────────────────────
export default function () {
  // TODO: implement actual request scenarios
  // Alternate between BMKG-only and mixed requests

  const start = Date.now();
  const res = http.get(`${BASE_URL}/v1/events?type=SEISMIC&limit=20`, { headers });
  const latency = Date.now() - start;

  bmkgLatency.add(latency);

  const ok = check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'not 500': (r) => r.status !== 500,
  });

  if (res.status !== 200 && res.status !== 429) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
  }

  sleep(0.1);
}
