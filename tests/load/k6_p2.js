// k6 load test script — Problem 2 (Concurrency & Graceful Degradation)
//
// Recommended: bash scripts/demo/p2-load.sh
//   (sets PVMBG delay to 3 s, runs this script, restores the delay)
//
// Manual run:
//   set -a; source .env; set +a
//   k6 run -e CLIENT_ID=$CLIENT_FIELD_TEAM_ID -e CLIENT_SECRET=$CLIENT_FIELD_TEAM_SECRET tests/load/k6_p2.js
//
// What it does (P2 criteria #1 and #2):
//   - Two scenarios run AT THE SAME TIME against client-api (:8080):
//       seismic_only : GET /v1/events?type=SEISMIC  (needs BMKG data only)
//       volcanic     : GET /v1/events?type=VOLCANIC (needs PVMBG data)
//     Default 25 + 25 = 50 parallel VUs, sustained for 60 s.
//   - bmkg_only_latency (SEISMIC, HTTP 200 only) must have p95 < 300 ms even
//     while PVMBG is configured with a 3 s delay.
//   - HTTP 429 is a controlled rejection: it is NOT counted as a failure, but
//     its total is reported (rate_limited_total).
//   - Each VU fetches its own access token and refreshes it before expiry, so
//     the default 60 s ACCESS_TOKEN_TTL never produces 401s mid-test.
//   - Summary prints p50/p95/p99 for every trend.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

// ─────────────────────────────────────────────────────────────
// Custom metrics
// ─────────────────────────────────────────────────────────────
const bmkgLatency    = new Trend('bmkg_only_latency', true);   // SEISMIC, 200 only
const volcanicLatency = new Trend('volcanic_latency', true);    // VOLCANIC, 200 only
const errorRate      = new Rate('error_rate');                 // non-200/429 responses
const rateLimited    = new Counter('rate_limited_total');      // number of 429s
const tokenFetches   = new Counter('token_fetches_total');

http.setResponseCallback(http.expectedStatuses(200, 429));

// ─────────────────────────────────────────────────────────────
// Test configuration
// ─────────────────────────────────────────────────────────────
const TOTAL_VUS = parseInt(__ENV.VUS || '50', 10);
const DURATION  = __ENV.DURATION || '60s';
const SEISMIC_VUS = Math.ceil(TOTAL_VUS / 2);
const VOLCANIC_VUS = Math.max(TOTAL_VUS - SEISMIC_VUS, 1);

export const options = {
  scenarios: {
    seismic_only: {
      executor: 'constant-vus',
      vus: SEISMIC_VUS,
      duration: DURATION,
      exec: 'seismicOnly',
      tags: { scenario: 'seismic_only' },
    },
    volcanic: {
      executor: 'constant-vus',
      vus: VOLCANIC_VUS,
      duration: DURATION,
      exec: 'volcanic',
      tags: { scenario: 'volcanic' },
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    bmkg_only_latency: ['p(95)<300'],   // P2 #1: BMKG-only p95 < 300 ms
    error_rate:        ['rate<0.01'],   // P2 #2: errors (excluding 429) < 1%
    http_req_failed:   ['rate<0.01'],   // 429 is an expected status, not a failure
  },
};

const BASE_URL      = __ENV.BASE_URL      || 'http://localhost:8080';
const TOKEN_URL     = __ENV.TOKEN_URL     || 'http://localhost:8083/oauth/token';
const CLIENT_ID     = __ENV.CLIENT_ID     || 'client-field-team';
const CLIENT_SECRET = __ENV.CLIENT_SECRET || 'change-me-field-team-secret';
const SLEEP_S       = parseFloat(__ENV.SLEEP_S || '0.1');

// ─────────────────────────────────────────────────────────────
// Per-VU token cache (refreshed 10 s before expiry)
// ─────────────────────────────────────────────────────────────
let token = __ENV.TOKEN || '';
let tokenExpiresAt = __ENV.TOKEN ? Number.MAX_SAFE_INTEGER : 0;

function getToken() {
  if (token && Date.now() < tokenExpiresAt - 10000) {
    return token;
  }
  const res = http.post(
    TOKEN_URL,
    { grant_type: 'client_credentials', client_id: CLIENT_ID, client_secret: CLIENT_SECRET },
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, tags: { name: 'token' } },
  );
  tokenFetches.add(1);
  if (res.status !== 200) {
    console.warn(`[k6] token request failed: HTTP ${res.status}`);
    return token;
  }
  const body = res.json();
  token = body.access_token;
  tokenExpiresAt = Date.now() + (body.expires_in || 60) * 1000;
  return token;
}

function queryEvents(type, latencyTrend) {
  const headers = {
    Authorization: `Bearer ${getToken()}`,
    'X-Correlation-ID': `k6-${type.toLowerCase()}-vu${__VU}-iter${__ITER}`,
  };
  const res = http.get(`${BASE_URL}/v1/events?type=${type}&limit=20`, {
    headers,
    tags: { name: `events_${type}` },
  });

  if (res.status === 200) {
    latencyTrend.add(res.timings.duration);
    errorRate.add(0);
  } else if (res.status === 429) {
    rateLimited.add(1);
    errorRate.add(0);
  } else {
    errorRate.add(1);
  }

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'not 5xx':              (r) => r.status < 500,
  });

  sleep(SLEEP_S);
}

export function seismicOnly() {
  queryEvents('SEISMIC', bmkgLatency);
}

export function volcanic() {
  queryEvents('VOLCANIC', volcanicLatency);
}
