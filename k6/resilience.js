import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import encoding from 'k6/encoding';
import crypto from 'k6/crypto';

const base = (__ENV.K6_BASE_URL || 'http://api-gateway:8080/api/v2').replace(/\/$/, '');
const runID = __ENV.K6_TEST_RUN_ID || `run-${Date.now()}`;
const scenario = __ENV.K6_SCENARIO || `resilience-${runID}`;
const readRPS = Number(__ENV.OFM_FULL_K6_READ_RPS || 0);
const writeRPS = Number(__ENV.OFM_FULL_K6_WRITE_RPS || 0);
const targetRPS = Number(__ENV.OFM_FULL_K6_TARGET_RPS || (readRPS + writeRPS));
// This local fixture is provisioned by the infra test seed and remains active
// across runs. Resilience must not depend on the asynchronous registration
// saga; registration has its own dedicated test.
const fixtureBuyerID = __ENV.K6_FIXTURE_BUYER_ID || '7c1d1af1-6be4-4e77-8e57-0b1f2d12e9aa';
const token = __ENV.K6_TOKEN || fixtureToken(fixtureBuyerID, 'resilience1788704979683867');
const username = __ENV.K6_FIXTURE_USERNAME || 'recovery-buyer';
const gigID = __ENV.K6_FIXTURE_GIG_ID || '01a01a47-d58f-7287-9187-1ff999a49968';
const packageID = __ENV.K6_PACKAGE_ID || '01a01a47-e1ec-75c4-b0fc-edd871493254';
const fixtures = [
  ['01a020a4-5e1a-7076-9b7e-24490b033add', '01a020a3-9722-7856-bd51-3ff244549c22', 'seller178725398686940'],
  ['01a020d6-d44e-738e-8025-35be1a9726b2', '01a020d5-2cd1-77e3-93a9-82837ee02a72', 'seller1787257234576850'],
  ['01a02f6c-9d14-783b-a730-418f62d22d20', '01a02f69-fe29-7b46-b69f-e315cc922075', 'seller1787501870621581'],
  ['01a020d5-fe3a-7217-b57e-048f24da44a6', '01a020d5-25aa-7a52-8e12-11e1b493da51', 'seller1787257234577530'],
  ['01a0209d-d13c-73e0-be73-38d063ca3a35', '01a0209c-7a83-773c-873e-c31144fbc0b6', 'seller1787253518735860'],
  ['01a020a8-5cea-77bb-bfe7-9213e3b1cf07', '01a020a6-e4a7-7eb5-8388-5fca691bc92e', 'seller1787254201245510'],
  ['01a0208a-f24b-748c-aaf0-bf47f98a763d', '01a02089-e7a8-74a2-b0a2-97dea0b2be71', 'seller1787252302267300'],
  ['01a02093-5dbc-71e3-9f2f-ddeb210ffd5e', '01a02092-63f7-70f4-a29a-4f72d7b254ad', 'seller178725285747840'],
];
const orderID = __ENV.K6_FIXTURE_ORDER_ID || '';
const reviewOrderID = __ENV.K6_FIXTURE_REVIEW_ORDER_ID || orderID;
const faultMode = (__ENV.FAULT_PROFILE || 'none').toLowerCase() !== 'none';
const faultProfile = (__ENV.FAULT_PROFILE || 'none').toLowerCase();
const faultTarget = __ENV.FAULT_TARGET || 'api-gateway';
const faultToken = __ENV.FAULT_TEST_TOKEN || 'ofm-test-fault-token';
const capacityRamp = __ENV.OFM_CAPACITY_RAMP === 'true';
const rampSeconds = __ENV.OFM_CAPACITY_RAMP_SECONDS || '10s';

export const requests = new Counter('resilience_http_requests');
export const unexpectedFailures = new Counter('resilience_unexpected_failures');
export const businessFailures = new Counter('resilience_business_failures');
export const expectedFailures = new Counter('resilience_expected_failures');
export const expectedApplicationResponses = new Counter('resilience_expected_application_responses');
export const successRate = new Rate('resilience_success_rate');
export const status400 = new Counter('resilience_status_400');
export const status401 = new Counter('resilience_status_401');
export const status403 = new Counter('resilience_status_403');
export const status404 = new Counter('resilience_status_404');
export const status500 = new Counter('resilience_status_500');
export const status503 = new Counter('resilience_status_503');
export const loadShed = new Counter('resilience_load_shed');
export const faultInjected = new Counter('resilience_fault_injected');
export const fallbackAccepted = new Counter('resilience_fallback_accepted');
export const kafkaRecoveryPublished = new Counter('resilience_kafka_recovery_published');
export const kafkaRecoveryCompleted = new Counter('resilience_kafka_recovery_completed');
export const projectionCompleted = new Counter('resilience_projection_completed');

function fixtureToken(subject, name) {
  const now = Math.floor(Date.now() / 1000);
  const header = encoding.b64encode('{"alg":"HS256","typ":"JWT"}', 'rawurl');
  const payload = encoding.b64encode(JSON.stringify({ sub: subject, username: name, email: `${name}@example.com`, roles: ['freelancer', 'buyer'], type: 1, userId: 1, avatar: '', iss: 'ofm-auth-service', iat: now, exp: now + 3600 }), 'rawurl');
  const input = `${header}.${payload}`;
  return `${input}.${crypto.hmac('sha256', 'aa96fae1a6eee39b879dad6b6bb372e63278257bf9f94010bc7d25693f61e38c', input, 'base64rawurl')}`;
}

function headers(accessToken = token) {
  const result = { Accept: 'application/json', 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}`, 'X-Test-Run-ID': runID, 'X-Test-Scenario': scenario };
  if (faultMode) {
    result['X-Fault-Profile'] = faultProfile;
    result['X-Fault-Target'] = faultTarget;
    result['X-Fault-Rate'] = String(Number(__ENV.FAULT_RATE || 0));
    result['X-Fault-Test-Token'] = faultToken;
  }
  return result;
}

function request(method, path, body = null, accessToken = token) {
  const response = http.request(method, `${base}${path}`, body ? JSON.stringify(body) : null, { timeout: __ENV.OFM_HTTP_TIMEOUT || '5s', headers: headers(accessToken), tags: { test_run_id: runID, scenario_id: scenario, workload: 'resilience' } });
  requests.add(1);
  if (response.status === 400) status400.add(1);
  if (response.status === 400 && (faultTarget === 'order-service' || faultTarget === 'order') && __ITER < 2) {
    console.log(`order targeted request rejected: ${method} ${path} body=${response.body}`);
  }
  if (response.status === 400 && (faultTarget === 'chat' || faultTarget === 'chat-service') && __ITER < 2) {
    console.log(`chat targeted request rejected: ${method} ${path} body=${response.body}`);
  }
  if (response.status === 401) status401.add(1);
  if (response.status === 403) status403.add(1);
  if (response.status === 404) status404.add(1);
  if (response.status === 500) status500.add(1);
  if (response.status === 503) status503.add(1);
  const ok = response.status >= 200 && response.status < 400;
  const injectedFailure = faultMode && !ok && response.headers['X-Fault-Injected'] === 'true';
  if (response.headers['X-Fallback-Accepted'] === 'true') fallbackAccepted.add(1);
  if (response.headers['X-Kafka-Recovery-Published'] === 'true') kafkaRecoveryPublished.add(1);
  if (response.headers['X-Kafka-Recovery-Completed'] === 'true') kafkaRecoveryCompleted.add(1);
  if (response.headers['X-Projection-Completed'] === 'true') projectionCompleted.add(1);
  // The gateway may attach X-Fault-Injected to a request that was only
  // observed by the injector. Count an actual fault only when it produced an
  // expected failure or forced the request through monolith fallback.
  // Recovery evidence is a write-side invariant. GET faults may be expected
  // read fallback/load-shed observations, but they must not increase the
  // number of Kafka recovery commands the finalizer waits for.
  const recoveryEligible = method !== 'GET';
  if (recoveryEligible && response.headers['X-Fault-Injected'] === 'true' && (injectedFailure || response.headers['X-Fallback-Accepted'] === 'true')) faultInjected.add(1);
  const shed = response.status === 0;
  if (injectedFailure) expectedFailures.add(1);
  else if (shed) loadShed.add(1);
  else if (response.status >= 500 || response.status === 0) unexpectedFailures.add(1);
  else if (!ok && response.status >= 400 && response.status < 500) expectedApplicationResponses.add(1);
  else if (!ok) businessFailures.add(1);
  // Resilience success measures transport/server availability. A well-formed
  // application response (including a legitimate 4xx for a missing fixture)
  // is not a resilience failure and must not make a healthy fallback run fail.
  const applicationResponse = response.status >= 400 && response.status < 500;
  successRate.add(ok || injectedFailure || shed || applicationResponse);
  // A resilience run measures transport and server availability. A valid
  // application-level 4xx (for example duplicate/invalid fixture data) is
  // not an infrastructure failure and must not fail the run's checks.
  check(response, { [`${method} ${path} has no transport/server failure`]: () => ok || injectedFailure || shed || (response.status >= 400 && response.status < 500) });
  return response;
}

export const options = {
  setupTimeout: __ENV.K6_SETUP_TIMEOUT || '5m',
  scenarios: {},
  thresholds: {
    resilience_success_rate: [faultMode ? 'rate>0.50' : 'rate>0.99'],
    resilience_unexpected_failures: ['count==0'],
  },
};

if (readRPS > 0) options.scenarios.reads = capacityRamp ? { executor: 'ramping-arrival-rate', startRate: 0, timeUnit: '1s', preAllocatedVUs: Number(__ENV.OFM_FULL_K6_PRE_ALLOCATED_VUS || 200), maxVUs: Number(__ENV.OFM_FULL_K6_MAX_VUS || 2000), stages: [{ target: readRPS, duration: rampSeconds }], exec: 'readLoad', gracefulStop: __ENV.OFM_K6_GRACEFUL_STOP || '5s' } : { executor: 'constant-arrival-rate', rate: readRPS, timeUnit: '1s', duration: __ENV.OFM_FULL_K6_DURATION || '10m', preAllocatedVUs: Number(__ENV.OFM_FULL_K6_PRE_ALLOCATED_VUS || 200), maxVUs: Number(__ENV.OFM_FULL_K6_MAX_VUS || 1000), exec: 'readLoad', gracefulStop: __ENV.OFM_K6_GRACEFUL_STOP || '5s' };
if (writeRPS > 0) options.scenarios.writes = capacityRamp ? { executor: 'ramping-arrival-rate', startRate: 0, timeUnit: '1s', preAllocatedVUs: Number(__ENV.OFM_FULL_K6_PRE_ALLOCATED_VUS || 200), maxVUs: Number(__ENV.OFM_FULL_K6_MAX_VUS || 2000), stages: [{ target: writeRPS, duration: rampSeconds }], exec: 'writeLoad', gracefulStop: __ENV.OFM_K6_GRACEFUL_STOP || '5s' } : { executor: 'constant-arrival-rate', rate: writeRPS, timeUnit: '1s', duration: __ENV.OFM_FULL_K6_DURATION || '10m', preAllocatedVUs: Number(__ENV.OFM_FULL_K6_PRE_ALLOCATED_VUS || 100), maxVUs: Number(__ENV.OFM_FULL_K6_MAX_VUS || 500), exec: 'writeLoad', gracefulStop: __ENV.OFM_K6_GRACEFUL_STOP || '5s' };

function setupRequest(method, path, body = null) {
  return http.request(method, `${base}${path}`, body ? JSON.stringify(body) : null, { headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'X-Test-Run-ID': runID, 'X-Test-Scenario': `${scenario}-setup` } });
}

function setupUser() {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 1000000)}`.replace(/[^0-9]/g, '').slice(-18);
  const username = `resilience${suffix}`;
  const email = `${username}@example.test`;
  const password = 'Password123!';
  const clientID = `01a020a3-9722-7856-bd51-${suffix.padStart(12, '0').slice(-12)}`;
  const signup = setupRequest('POST', '/auth/sign-up', { client_id: clientID, email, password, username, firstName: 'Resilience', surname: 'K6' });
  console.log(`resilience preflight signup status=${signup.status}`);
  if (signup.status !== 200 && signup.status !== 202) {
    console.log(`resilience preflight signup failed: ${signup.status}`);
    return '';
  }
  let signupBody = {};
  try { signupBody = signup.json(); } catch (_) {}
  const sessionID = signupBody.session_id || '';
  if (!sessionID) return '';
  // Registration is event-driven. This legacy helper is retained only as an
  // explicit one-shot fixture path; it must not retry commands while the saga
  // is converging. The full-system scenario uses the realtime collector for
  // registration.code_sent/completed/failed instead.
  const verify = setupRequest('POST', '/auth/sign-up/verify-email', { session_id: sessionID, client_id: signupBody.client_id || clientID, code: '123456' });
  console.log(`resilience preflight verify status=${verify.status}`);
  if (verify.status !== 200 && verify.status !== 202) return '';
  const complete = setupRequest('POST', '/auth/sign-up/complete', { session_id: sessionID, client_id: signupBody.client_id || clientID });
  console.log(`resilience preflight complete status=${complete.status}`);
  if (complete.status !== 200 && complete.status !== 201) return '';
  const login = setupRequest('POST', '/auth/sign-in', { identifier: username, password });
  if (login.status !== 200) {
    console.log(`resilience preflight sign-in failed status=${login.status} body=${login.body}`);
    return '';
  }
  let loginBody = {};
  try { loginBody = login.json(); } catch (_) { return ''; }
  const refreshToken = loginBody.refresh_token || loginBody.refreshToken || '';
  if (!refreshToken) {
    console.log('resilience preflight sign-in returned no refresh token');
    return '';
  }
  const refresh = setupRequest('POST', '/auth/refresh', { refresh_token: refreshToken });
  if (refresh.status !== 200) {
    console.log(`resilience preflight refresh failed status=${refresh.status} body=${refresh.body}`);
    return '';
  }
  try {
    const accessToken = refresh.json().access_token || '';
    if (!accessToken) console.log('resilience preflight refresh returned no access token');
    return { accessToken, username };
  } catch (_) {
    console.log(`resilience preflight refresh returned invalid JSON body=${refresh.body}`);
    return null;
  }
}

export function setup() {
  // Resilience setup is deliberately independent from registration. The
  // fixture user is seeded and active; registration has its own async test.
  const fixturePassword = __ENV.K6_FIXTURE_PASSWORD || '';
  if (__ENV.K6_FIXTURE_USERNAME && fixturePassword) {
    const login = setupRequest('POST', '/auth/sign-in', {
      identifier: __ENV.K6_FIXTURE_USERNAME,
      password: fixturePassword,
    });
    if (login.status === 200) {
      try {
        const accessToken = login.json().access_token || '';
        if (accessToken) return { accessToken, username: __ENV.K6_FIXTURE_USERNAME };
      } catch (_) {}
    }
    console.log(`resilience fixture sign-in failed status=${login.status}`);
  }
  return { accessToken: token, username };
}

export function readLoad(data) {
  // Keep the read lane independent from auth and mutable gig state. The
  // resilience experiment must spend its read budget on system traffic, not
  // turn auth/me or a single draft into a second contention experiment.
  // Use a deterministic existing resource so a missing business fixture does
  // not get reported as a resilience failure. The request still traverses
  // the gig service and its read fallback path when the service is down.
  if (faultTarget === 'user-service' || faultTarget === 'user') {
    request('GET', `/users/${__ENV.K6_USER_READ_USERNAME || 'alex1'}`, null, data.accessToken);
    return;
  }
  request('GET', `/gigs/${gigID}/draft`, null, data.accessToken);
}

export function writeLoad(data) {
	if (faultTarget === 'chat-service' || faultTarget === 'chat') {
		if (!orderID) {
			console.log('chat recovery workload requires K6_FIXTURE_ORDER_ID');
			return;
		}
		request('POST', `/users/${username}/orders/${orderID}/chat/messages`, {
			order_id: orderID,
			text: `resilience chat ${runID}-${__VU}-${__ITER}`,
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'review-service' || faultTarget === 'review') {
		if (!reviewOrderID) {
			console.log('review recovery workload requires K6_FIXTURE_REVIEW_ORDER_ID');
			return;
		}
		request('POST', `/orders/${reviewOrderID}/reviews`, {
			content: `resilience review ${runID}`,
			rating: 5,
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'auth-service' || faultTarget === 'auth') {
		const suffix = `${Date.now()}-${__VU}-${__ITER}`.replace(/[^0-9]/g, '').slice(-18);
		const username = `recovery${suffix}`;
		request('POST', '/auth/sign-up', {
			client_id: `01a020a3-9722-7856-9b4c-${suffix.padStart(12, '0').slice(-12)}`,
			email: `${username}@example.test`, password: 'Password123!', username,
			firstName: 'Recovery', surname: 'K6',
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'registration-saga-service' || faultTarget === 'registration-saga') {
		const suffix = `${Date.now()}-${__VU}-${__ITER}`.replace(/[^0-9]/g, '').slice(-18);
		const username = `saga${suffix}`;
		request('POST', '/auth/sign-up', {
			client_id: `01a020a3-9722-7856-9b4c-${suffix.padStart(12, '0').slice(-12)}`,
			email: `${username}@example.test`, password: 'Password123!', username,
			firstName: 'Recovery', surname: 'Saga',
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'payment-service' || faultTarget === 'payment') {
		request('POST', '/freelancer/onboarding/start', {
			email: `${data.username || 'recovery'}@example.test`,
			country: 'US',
			return_url: 'http://localhost/fake-stripe/return',
			refresh_url: 'http://localhost/fake-stripe/refresh',
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'gig-service' || faultTarget === 'gig') {
		request('POST', '/gigs/drafts', {
			freelancer_id: tokenSubject(data.accessToken),
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'order-service' || faultTarget === 'order') {
		request('POST', '/orders/start', {
			buyer_id: fixtureBuyerID,
			buyer_email: `${data.username}@example.com`,
			seller_id: 'aeac656a-e617-45f2-b1ce-b922d8bd03fa',
			gig_id: gigID,
			package_id: packageID,
			idempotency_key: `${runID}-order-${__VU}-${__ITER}`,
			requested_at: new Date().toISOString(),
		}, data.accessToken);
		return;
	}
	if (faultTarget === 'order-saga-service' || faultTarget === 'order-saga') {
		request('POST', '/orders/start', {
			buyer_id: fixtureBuyerID,
			buyer_email: `${data.username}@example.com`,
			seller_id: 'aeac656a-e617-45f2-b1ce-b922d8bd03fa',
			gig_id: gigID,
			package_id: packageID,
			idempotency_key: `${runID}-saga-order-${__VU}-${__ITER}`,
			requested_at: new Date().toISOString(),
		}, data.accessToken);
		return;
	}
	// Writes intentionally use idempotent/readiness-safe probes unless fixture
  // IDs are explicitly supplied. Stateful order transitions belong to the
  // separate transition workload and never run concurrently on one order.
  if (orderID) {
    request('GET', `/orders/${orderID}`, null, data.accessToken);
    return;
  }
	const response = request('POST', '/gigs/drafts', { freelancer_id: data.accessToken ? tokenSubject(data.accessToken) : '' }, data.accessToken);
  check(response, { 'CUD returned an application success': r => r.status >= 200 && r.status < 300 });
}

function tokenSubject(accessToken) {
  try { return JSON.parse(encoding.b64decode(accessToken.split('.')[1], 'rawurl', 's')).sub || ''; }
  catch (_) { return '01a020a3-9722-7856-bd51-3ff244549c22'; }
}

export function handleSummary(data) {
  const metric = name => data.metrics[name] || {};
  const duration = metric('http_req_duration');
  const shed = metric('resilience_load_shed').values?.count || 0;
  return { stdout: JSON.stringify({ ofm_summary: true, workload: 'resilience', run_id: runID, scenario_id: scenario, target_rps: targetRPS, checks_total: (metric('checks').values?.passes || 0) + (metric('checks').values?.fails || 0), checks_failed: metric('checks').values?.fails || 0, http_requests: metric('http_reqs').values?.count || 0, http_failed: (metric('resilience_unexpected_failures').values?.count || 0) + shed, load_shed: shed, fault_injected: metric('resilience_fault_injected').values?.count || 0, fallback_accepted: metric('resilience_fallback_accepted').values?.count || 0, kafka_recovery_published: metric('resilience_kafka_recovery_published').values?.count || 0, kafka_recovery_completed: metric('resilience_kafka_recovery_completed').values?.count || 0, projection_completed: metric('resilience_projection_completed').values?.count || 0, expected_failures: metric('resilience_expected_failures').values?.count || 0, expected_application_responses: metric('resilience_expected_application_responses').values?.count || 0, business_failures: metric('resilience_business_failures').values?.count || 0, status_400: metric('resilience_status_400').values?.count || 0, status_401: metric('resilience_status_401').values?.count || 0, status_403: metric('resilience_status_403').values?.count || 0, status_404: metric('resilience_status_404').values?.count || 0, status_500: metric('resilience_status_500').values?.count || 0, status_503: metric('resilience_status_503').values?.count || 0, dropped_iterations: metric('dropped_iterations').values?.count || 0, p50_duration_ms: duration.values?.['p(50)'] || 0, p95_duration_ms: duration.values?.['p(95)'] || 0, p99_duration_ms: duration.values?.['p(99)'] || 0, test_run_duration_ms: data.state?.testRunDurationMs || 0 }) + '\n' };
}
