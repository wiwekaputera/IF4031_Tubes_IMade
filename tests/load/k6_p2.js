// k6 load test script — Problem 2 (Concurrency & Graceful Degradation)
// Run with:
//   k6 run tests/load/k6_p2.js
//
// Prerequisites:
//   1. Stack running (bash scripts/demo/up.sh)
//   2. client-api reachable at BASE_URL (default: http://localhost:8080)
//   3. token-service reachable at TOKEN_URL (default: http://localhost:8083/oauth/token)
//
// Tips for high concurrency / sustained load testing:
//   - If running sustained test >60s, run with ACCESS_TOKEN_TTL=300 on token-service
//     to prevent token expiration midway.
//   - To test full 500 RPS without rate-limit throttling, set CLIENT_API_RPS_LIMIT=1000
//     and CLIENT_API_BURST=100 on client-api.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

// ─────────────────────────────────────────────────────────────
// Custom metrics
// ─────────────────────────────────────────────────────────────
const bmkgLatency   = new Trend('bmkg_only_latency', true);
const errorRate     = new Rate('error_rate');
const rateLimitRate = new Rate('rate_limit_rate');

http.setResponseCallback(http.expectedStatuses(200, 429));

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
    bmkg_only_latency: ['p(95)<300'],    // BMKG-only p95 < 300ms (only successful 200 responses)
    error_rate:        ['rate<0.01'],    // error rate < 1% (excl. 429)
    http_req_failed:   ['rate<0.01'],    // 429 is treated as expected, not failure
  },
};

const BASE_URL     = __ENV.BASE_URL     || 'http://localhost:8080';
const TOKEN_URL    = __ENV.TOKEN_URL    || 'http://localhost:8083/oauth/token';
const CLIENT_ID    = __ENV.CLIENT_ID    || 'client-field-team';
const CLIENT_SECRET = __ENV.CLIENT_SECRET || 'change-me-field-team-secret';

// ─────────────────────────────────────────────────────────────
// Setup: Fetch fresh access token before VU execution
// ─────────────────────────────────────────────────────────────
export function setup() {
  if (__ENV.TOKEN) {
    return { token: __ENV.TOKEN };
  }

  const payload = {
    grant_type: 'client_credentials',
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
  };

  const res = http.post(TOKEN_URL, payload, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });

  if (res.status === 200) {
    const body = res.json();
    return { token: body.access_token };
  }

  console.warn(`[k6 setup] Failed to fetch token from ${TOKEN_URL} (status ${res.status}). Using fallback env.`);
  return { token: __ENV.TOKEN || 'placeholder-token' };
}

// ─────────────────────────────────────────────────────────────
// Main test function
// ─────────────────────────────────────────────────────────────
export default function (data) {
  const token = (data && data.token) || __ENV.TOKEN;
  const headers = {
    'Authorization': `Bearer ${token}`,
    'X-Correlation-ID': `k6-vu${__VU}-iter${__ITER}`,
  };

  const start = Date.now();
  const res = http.get(`${BASE_URL}/v1/events?type=SEISMIC&limit=20`, { headers });
  const latency = Date.now() - start;

  if (res.status === 200) {
    bmkgLatency.add(latency);
    rateLimitRate.add(0);
    errorRate.add(0);
  } else if (res.status === 429) {
    rateLimitRate.add(1);
    errorRate.add(0);
  } else {
    rateLimitRate.add(0);
    errorRate.add(1);
  }

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'not 500':              (r) => r.status !== 500,
  });

  sleep(0.1);
}
