// k6 load tests against client-api on the Kubernetes deployment.
// Run them through the scripts in scripts/demo/m2/load/, which prepare the
// cluster (rate limit, token lifetime, trace sampling) and record replicas.
//
// SCENARIO=load        VUS (default 10000) ramp in RAMP, hold HOLD (>= 120 s).
//                      The orchestrator scales the aggregator and rolls it out
//                      during the hold; every error is counted per phase
//                      (phases are given in seconds since the hold started).
// SCENARIO=throughput  VUS constant for DURATION; compare http_reqs/s between
//                      runs with 1 and 3 aggregator replicas.
// SCENARIO=shed        MEDIA_VUS media clients and FIELD_VUS field-team clients
//                      at once; media should be refused (503) while the field
//                      team keeps being served.
//
// Responses: 200 = served; 429 (rate limit) and 503 (load shedding) are
// controlled refusals; anything else, or no response, is an error.

import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const SCENARIO = __ENV.SCENARIO || 'load';
const BASE_URL = __ENV.BASE_URL || 'http://localhost:30080';
const TOKEN_URL = __ENV.TOKEN_URL || 'http://localhost:30083/oauth/token';
const SLEEP_S = parseFloat(__ENV.SLEEP_S || '0.5');
const PATH = __ENV.PATH_QUERY || '/v1/events?limit=20';

const served = new Counter('served_total');
const controlled = new Counter('controlled_refusals_total');
const unexpected = new Counter('unexpected_errors_total');
const errorRate = new Rate('unexpected_error_rate');
const servedLatency = new Trend('served_latency', true);
const fieldServed = new Rate('field_team_served_rate');
const fieldLatency = new Trend('field_team_latency', true);
const mediaRefused = new Rate('media_refused_rate');

http.setResponseCallback(http.expectedStatuses(200, 429, 503));

const VUS = parseInt(__ENV.VUS || '10000', 10);
const RAMP = __ENV.RAMP || '30s';
const HOLD = __ENV.HOLD || '150s';
const DURATION = __ENV.DURATION || '60s';
const MEDIA_VUS = parseInt(__ENV.MEDIA_VUS || '3000', 10);
const FIELD_VUS = parseInt(__ENV.FIELD_VUS || '50', 10);

// Phase boundaries in seconds after the hold starts (load scenario).
const RAMP_S = parseInt(RAMP, 10);
const SCALE_UP_AT = parseInt(__ENV.SCALE_UP_AT || '20', 10);
const SCALE_DOWN_AT = parseInt(__ENV.SCALE_DOWN_AT || '60', 10);
const ROLL_AT = parseInt(__ENV.ROLL_AT || '95', 10);

function scenarios() {
  if (SCENARIO === 'throughput') {
    return { throughput: { executor: 'constant-vus', vus: VUS, duration: DURATION, exec: 'fieldTeam' } };
  }
  if (SCENARIO === 'shed') {
    return {
      media: { executor: 'constant-vus', vus: MEDIA_VUS, duration: DURATION, exec: 'media' },
      field_team: { executor: 'constant-vus', vus: FIELD_VUS, duration: DURATION, exec: 'fieldTeam' },
    };
  }
  return {
    load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP, target: VUS },
        { duration: HOLD, target: VUS },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '10s',
      exec: 'fieldTeam',
    },
  };
}

const thresholds = { unexpected_error_rate: ['rate<0.01'] };
if (SCENARIO === 'load') {
  for (const p of ['ramp', 'replicas_3', 'replicas_5', 'replicas_2', 'rolling_update', 'tail']) {
    thresholds[`unexpected_errors_total{phase:${p}}`] = p === 'rolling_update' ? ['count<1'] : ['count>=0'];
    thresholds[`served_total{phase:${p}}`] = ['count>=0'];
  }
}
if (SCENARIO === 'shed') {
  thresholds.field_team_served_rate = ['rate>0.99'];
  thresholds.media_refused_rate = ['rate>=0'];
  thresholds['field_team_latency'] = ['p(95)>=0'];
}

export const options = {
  scenarios: scenarios(),
  thresholds,
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  setupTimeout: '60s',
};

function fetchToken(id, secret) {
  const res = http.post(TOKEN_URL,
    { grant_type: 'client_credentials', client_id: id, client_secret: secret },
    { responseType: 'text', tags: { name: 'token' } });
  if (res.status !== 200) {
    throw new Error(`token for ${id}: HTTP ${res.status}`);
  }
  return res.json().access_token;
}

// One token per client for all VUs: the orchestrator raises the access-token
// lifetime for the test, so 10,000 VUs do not each ask token-service.
export function setup() {
  return {
    field: fetchToken(__ENV.FIELD_ID || 'client-field-team', __ENV.FIELD_SECRET || ''),
    media: SCENARIO === 'shed' ? fetchToken(__ENV.MEDIA_ID || 'client-media', __ENV.MEDIA_SECRET || '') : '',
    holdStartsAt: Date.now() + RAMP_S * 1000,
  };
}

function phase(data) {
  if (SCENARIO !== 'load') return SCENARIO;
  const t = (Date.now() - data.holdStartsAt) / 1000;
  if (t < 0) return 'ramp';
  if (t < SCALE_UP_AT) return 'replicas_3';
  if (t < SCALE_DOWN_AT) return 'replicas_5';
  if (t < ROLL_AT) return 'replicas_2';
  if (t < ROLL_AT + 40) return 'rolling_update';
  return 'tail';
}

function call(token, scope, data) {
  const tags = { phase: phase(data), scope };
  const res = http.get(`${BASE_URL}${PATH}`, {
    headers: { Authorization: `Bearer ${token}`, 'X-Correlation-ID': `k6-${scope}-${__VU}-${__ITER}` },
    tags,
  });
  const ok = res.status === 200;
  const refused = res.status === 429 || res.status === 503;
  if (ok) {
    served.add(1, tags);
    servedLatency.add(res.timings.duration, tags);
  } else if (refused) {
    controlled.add(1, tags);
  } else {
    unexpected.add(1, tags);
  }
  errorRate.add(!ok && !refused, tags);
  return res;
}

export function fieldTeam(data) {
  const res = call(data.field, 'field_team', data);
  if (SCENARIO === 'shed') {
    fieldServed.add(res.status === 200);
    if (res.status === 200) fieldLatency.add(res.timings.duration);
  }
  sleep(SLEEP_S);
}

export function media(data) {
  const res = call(data.media, 'media', data);
  mediaRefused.add(res.status === 503 || res.status === 429);
  sleep(SLEEP_S);
}
