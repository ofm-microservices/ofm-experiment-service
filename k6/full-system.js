import http from 'k6/http';
import ws from 'k6/ws';
import exec from 'k6/execution';
import { check as k6Check } from 'k6';
import { Counter } from 'k6/metrics';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import { sleep } from 'k6';
import { FormData } from './formdata.js';

function check(value, checks) {
  // Arrival-rate scenarios stop starting work at the load-window boundary.
  // A VU that is interrupted at that boundary can still unwind its callback
  // and reach a final event check. Those checks describe an intentionally
  // stopped iteration, not an application failure; do not record or log
  // them as failed k6 checks.
  if (!loadWindowOpen()) return true;
  const passed = k6Check(value, checks);
  if (!passed) console.log(`CHECK_FAILED ${Object.keys(checks).join('|')}`);
  return passed;
}

function abortFlow(reason, details = {}) {
  console.log(JSON.stringify({ event: 'full-system.flow_aborted', reason, ...details }));
  fullSystemAbortedFlows.add(1);
  check(false, { [`full-system flow completed: ${reason}`]: () => false });
}

const base = (__ENV.K6_BASE_URL || 'http://api.ofm.local/api/v2').replace(/\/$/, '');
const payment = (__ENV.K6_PAYMENT_URL || 'http://payment.ofm.local/v1').replace(/\/$/, '');
const realtime = (__ENV.K6_WS_URL || 'ws://realtime-service:8082/ws').replace(/\/$/, '');
const runID = __ENV.K6_TEST_RUN_ID || `run-${Date.now()}`;
const scenario = __ENV.K6_SCENARIO || `full-system-${runID}`;
const faultProfile = (__ENV.FAULT_PROFILE || 'none').toLowerCase();
const faultTarget = __ENV.FAULT_TARGET || 'api-gateway';
const faultRate = String(Number(__ENV.FAULT_RATE || 0));
const faultToken = __ENV.FAULT_TEST_TOKEN || 'ofm-test-fault-token';
// Recovery notifications remain event-driven. A faulted gig service can be
// unavailable for the configured recovery window and then drain commands;
// allow that bounded recovery time without polling or guessing a sleep.
// Under the local mixed workload a projection notification can arrive after
// the HTTP command has committed while Kafka/CDC drains other events. Keep
// this a bounded WS wait; it is not polling and it avoids declaring a healthy
// event path failed at an arbitrary 60-second boundary.
const asyncCommandTimeout = durationMilliseconds(__ENV.OFM_FULL_K6_STAGE_TIMEOUT || '5m', 300000);
// Bound the asynchronous business-flow drain independently from the load
// window so saga, CDC, and realtime work can finish after HTTP commands.
const fullSystemFlowTimeoutMs = durationMilliseconds(__ENV.OFM_FULL_K6_FLOW_TIMEOUT || '120s', 120000);
// A service-down fault can make the preliminary GET fail. Keep the target's
// CUD lane enabled in that case so the experiment actually exercises fallback
// and recovery instead of silently skipping the operation.
function isFaultTarget(service) {
  return faultTarget === service || faultTarget === `${service}-service`;
}
function secret(name) {
  try { return open(`/run/secrets/ofm/${name}`).trim(); } catch (_) { return ''; }
}
function fixtureToken(subject, username, roles = ['freelancer', 'buyer']) {
  const now = Math.floor(Date.now() / 1000);
  const header = encoding.b64encode('{"alg":"HS256","typ":"JWT"}', 'rawurl');
  const payload = encoding.b64encode(JSON.stringify({sub: subject, username, email: `${username}@example.com`, roles, type: 1, userId: 1, avatar: '', iss: 'ofm-auth-service', iat: now, exp: now + 3600}), 'rawurl');
  const input = `${header}.${payload}`;
  return `${input}.${crypto.hmac('sha256', 'aa96fae1a6eee39b879dad6b6bb372e63278257bf9f94010bc7d25693f61e38c', input, 'base64rawurl')}`;
}
const defaultToken = __ENV.K6_TOKEN || secret('token') || fixtureToken('aeac656a-e617-45f2-b1ce-b922d8bd03fa', 'alex1');
const defaultFreelancerToken = __ENV.K6_FREELANCER_TOKEN || secret('freelancer_token') || defaultToken;
const defaultBuyerToken = __ENV.K6_BUYER_TOKEN || secret('buyer_token') || fixtureToken('7c1d1af1-6be4-4e77-8e57-0b1f2d12e9aa', 'buyer1');
const defaultAdminToken = __ENV.K6_ADMIN_TOKEN || secret('admin_token') || fixtureToken('019e0000-0000-7000-8000-000000000001', 'order-complete-admin', ['admin']);
const readTokens = {};
const maxScenarioVUs = Math.max(1, Number(__ENV.OFM_FULL_K6_MAX_VUS || 500));

export const options = {
  tags: { test_run_id: runID, scenario_id: scenario },
  scenarios: {
    write_workload: Number(__ENV.OFM_FULL_K6_WRITE_RPS || 0) > 0 ? {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.OFM_FULL_K6_WRITE_RPS),
      timeUnit: '1s',
      duration: __ENV.OFM_FULL_K6_DURATION || '5m',
      // Stop business request generation at the load-window boundary.
      gracefulStop: __ENV.OFM_FULL_K6_GRACEFUL_STOP || '0s',
      preAllocatedVUs: Math.max(1, Number(__ENV.OFM_FULL_K6_WRITE_RPS)),
      maxVUs: maxScenarioVUs,
      exec: 'default',
    } : {
      executor: 'constant-arrival-rate',
      rate: 1,
      timeUnit: '1s',
      duration: __ENV.OFM_FULL_K6_DURATION || '5m',
      gracefulStop: __ENV.OFM_FULL_K6_GRACEFUL_STOP || '0s',
      preAllocatedVUs: 1,
      maxVUs: 1,
      exec: 'default',
    },
    read_workload: Number(__ENV.OFM_FULL_K6_READ_RPS || 0) > 0 ? {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.OFM_FULL_K6_READ_RPS),
      timeUnit: '1s',
      duration: __ENV.OFM_FULL_K6_DURATION || '5m',
      gracefulStop: __ENV.OFM_FULL_K6_GRACEFUL_STOP || '0s',
      preAllocatedVUs: Math.max(1, Number(__ENV.OFM_FULL_K6_READ_RPS)),
      maxVUs: maxScenarioVUs,
      exec: 'readWorkload',
    } : {
      executor: 'constant-vus',
      vus: 1,
      duration: '1s',
      exec: 'readWorkload',
    },
  },
  thresholds: {
    checks: ['rate>0.95'],
    full_system_aborted_flows: ['count==0'],
    // A run that only creates read-lane users and never reaches the business
    // workflow is not a successful full-system experiment. This also catches
    // VUs stranded in registration/realtime waits.
    full_system_completed_flows: ['count>0'],
  },
};

export const endpointsCalled = new Counter('full_system_endpoints_called');
export const unexpectedHTTPFailures = new Counter('unexpected_http_failures');
export const interruptedHTTPRequests = new Counter('interrupted_http_requests');
export const expectedHTTPFailures = new Counter('expected_http_failures');
export const realtimeEvents = new Counter('realtime_events');
export const realtimeTerminalEvents = new Counter('realtime_terminal_events');
export const realtimeMismatchedEvents = new Counter('realtime_mismatched_events');
export const fullSystemAbortedFlows = new Counter('full_system_aborted_flows');
export const fullSystemCompletedFlows = new Counter('full_system_completed_flows');

function durationMilliseconds(value, fallback = 300000) {
  const match = String(value || '').trim().match(/^(\d+(?:\.\d+)?)(ms|s|m|h)$/);
  if (!match) return fallback;
  const amount = Number(match[1]);
  const units = { ms: 1, s: 1000, m: 60000, h: 3600000 };
  return amount * units[match[2]];
}

// The load window controls only when new iterations may start. Existing
// flows must be allowed to drain after that window; otherwise a flow started
// at 179s receives a one-second timeout merely because the load phase ended.
// The drain deadline is the only bound for an unfinished registration.
const loadDurationMs = durationMilliseconds(
  __ENV.OFM_FULL_K6_DURATION || __ENV.OFM_FULL_K6_MAX_DURATION,
  300000,
);
const drainDurationMs = durationMilliseconds(__ENV.OFM_FULL_K6_DRAIN_TIMEOUT || '120s', 120000);
function scenarioStartMilliseconds() {
  const start = exec.scenario.startTime;
  const milliseconds = typeof start === 'number'
    ? start
    : start && typeof start.getTime === 'function'
      ? start.getTime()
      : Date.parse(String(start));
  return Number.isFinite(milliseconds) ? milliseconds : Date.now();
}

function remainingExperimentMilliseconds() {
  const deadline = scenarioStartMilliseconds() + loadDurationMs + drainDurationMs;
  return Math.max(1, deadline - Date.now());
}

function loadWindowOpen() {
  return Date.now() < scenarioStartMilliseconds() + loadDurationMs;
}

function stopFlowAfterLoad(stage) {
  console.log(JSON.stringify({ event: 'full-system.flow_stopped_after_load', stage, load_duration_ms: loadDurationMs }));
}

// Kubernetes runner consumes this compact line from the Job log and persists
// it in the ClickHouse run registry. Keeping the payload small avoids sending
// high-cardinality k6 samples through the control API.
export function handleSummary(data) {
  const metric = (name) => data.metrics[name] || {};
  const checks = metric('checks');
  const requests = metric('http_reqs');
  const failed = metric('http_req_failed');
  const duration = metric('http_req_duration');
  return {
    stdout: JSON.stringify({
      ofm_summary: true,
      run_id: runID,
      scenario_id: scenario,
      checks_total: (checks.values?.passes || 0) + (checks.values?.fails || 0),
      checks_failed: checks.values?.fails || 0,
      http_requests: requests.values?.count || 0,
      // k6's built-in http_req_failed counts every non-2xx/3xx response,
      // including deliberate contract probes such as 401/404. The experiment
      // result must expose operational failures separately from those probes.
      http_failed: metric('unexpected_http_failures').values?.count || 0,
      http_transport_failed: metric('interrupted_http_requests').values?.count || 0,
      http_expected_failures: metric('expected_http_failures').values?.count || 0,
      http_failed_raw: failed.values?.fails || 0,
      unexpected_http_failures: metric('unexpected_http_failures').values?.count || 0,
      interrupted_http_requests: metric('interrupted_http_requests').values?.count || 0,
      realtime_events: metric('realtime_events').values?.count || 0,
      realtime_terminal_events: metric('realtime_terminal_events').values?.count || 0,
      realtime_mismatched_events: metric('realtime_mismatched_events').values?.count || 0,
      full_system_aborted_flows: metric('full_system_aborted_flows').values?.count || 0,
      full_system_completed_flows: metric('full_system_completed_flows').values?.count || 0,
      p50_duration_ms: duration.values?.['p(50)'] || 0,
      p95_duration_ms: duration.values?.['p(95)'] || 0,
      p99_duration_ms: duration.values?.['p(99)'] || 0,
      test_run_duration_ms: data.state?.testRunDurationMs || 0,
    }) + '\n',
  };
}

function headers(token, idempotencyKey = '') {
  const h = { Accept: 'application/json', 'Content-Type': 'application/json', 'X-Test-Run-ID': runID, 'X-Test-Scenario': scenario };
  if (token) h.Authorization = `Bearer ${token}`;
  if (idempotencyKey) h['Idempotency-Key'] = idempotencyKey;
  if (faultProfile !== 'none') {
    h['X-Fault-Profile'] = faultProfile;
    h['X-Fault-Target'] = faultTarget;
    h['X-Fault-Rate'] = faultRate;
    h['X-Fault-Test-Token'] = faultToken;
  }
  return h;
}

function authHeaders(token, idempotencyKey = '') {
  const h = { Accept: 'application/json', 'X-Test-Run-ID': runID, 'X-Test-Scenario': scenario };
  if (token) h.Authorization = `Bearer ${token}`;
  if (idempotencyKey) h['Idempotency-Key'] = idempotencyKey;
  if (faultProfile !== 'none') {
    h['X-Fault-Profile'] = faultProfile;
    h['X-Fault-Target'] = faultTarget;
    h['X-Fault-Rate'] = faultRate;
    h['X-Fault-Test-Token'] = faultToken;
  }
  return h;
}

function multipartCall(method, url, token, body, expected = [200, 201, 202, 204], idempotencyKey = '') {
  const requestHeaders = authHeaders(token, idempotencyKey);
  let requestBody = body;
  if (body && typeof body.body === 'function' && body.boundary) {
    requestBody = body.body();
    requestHeaders['Content-Type'] = `multipart/form-data; boundary=${body.boundary}`;
  }
  const res = http.request(method, url, requestBody, { headers: requestHeaders, tags: { endpoint: url, scenario_id: scenario } });
  endpointsCalled.add(1);
  // A 4xx is a valid application response. Only transport failures and 5xx
  // responses indicate that the system itself failed during the experiment.
  const ok = res.status >= 200 && res.status < 500;
  if (res.status === 0) interruptedHTTPRequests.add(1);
  else if (!ok) unexpectedHTTPFailures.add(1);
  else if (res.status >= 400) expectedHTTPFailures.add(1);
  if (!ok) console.log(`E2E failure ${method} ${url}: HTTP ${res.status} body=${res.body}`);
  check(res, { [`${method} ${url} returned a non-5xx status`]: () => ok });
  return res;
}

function directUpload(url, content, contentType = 'text/plain', expected = [200, 201, 204]) {
  const res = http.put(url, content, { headers: { 'Content-Type': contentType }, tags: { endpoint: url, scenario_id: scenario } });
  endpointsCalled.add(1);
  const ok = res.status >= 200 && res.status < 500;
  if (res.status === 0) interruptedHTTPRequests.add(1);
  else if (!ok) unexpectedHTTPFailures.add(1);
  else if (res.status >= 400) expectedHTTPFailures.add(1);
  if (!ok) console.log(`E2E failure PUT ${url}: HTTP ${res.status} body=${res.body}`);
  check(res, { [`PUT ${url} returned a non-5xx status`]: () => ok });
  return res;
}

function call(method, url, token, body, expected = [200, 201, 202, 204], idempotencyKey = '') {
  const res = http.request(method, url, body == null ? null : JSON.stringify(body), { headers: headers(token, idempotencyKey), tags: { endpoint: url, scenario_id: scenario } });
  endpointsCalled.add(1);
  const ok = res.status >= 200 && res.status < 500;
  if (res.status === 0) interruptedHTTPRequests.add(1);
  else if (!ok) unexpectedHTTPFailures.add(1);
  else if (res.status >= 400) expectedHTTPFailures.add(1);
  if (!ok) console.log(`E2E failure ${method} ${url}: HTTP ${res.status} body=${res.body}`);
  check(res, { [`${method} ${url} returned a non-5xx status`]: () => ok });
  return res;
}

// Required business commands must return a successful application response.
// `call` intentionally accepts 4xx for negative contract probes, so using it
// for authentication would otherwise let an invalid JWT silently continue the
// E2E flow.
function requiredCall(method, url, token, body, expected = [200, 201, 202, 204]) {
  const res = call(method, url, token, body, expected);
  const ok = res.status >= 200 && res.status < 300;
  check(res, { [`${method} ${url} returned 2xx`]: () => ok });
  if (!ok) console.log(`Required E2E command failed ${method} ${url}: HTTP ${res.status} body=${res.body}`);
  return res;
}

function rawCall(method, url, token, body, retryableStatuses = [], retryableErrors = []) {
  const res = http.request(method, url, body == null ? null : JSON.stringify(body), { headers: headers(token), tags: { endpoint: url, scenario_id: scenario } });
  endpointsCalled.add(1);
  if (res.status === 0) interruptedHTTPRequests.add(1);
  else if (retryableStatuses.includes(res.status) || retryableErrors.some(error => String(res.body || '').includes(error))) expectedHTTPFailures.add(1);
  else if (res.status >= 500) unexpectedHTTPFailures.add(1);
  return res;
}

function fallbackAccepted(response) {
  if (!response || !response.headers) return false;
  return String(response.headers['X-Fallback-Accepted'] || response.headers['x-fallback-accepted'] || '').toLowerCase() === 'true';
}

function callAndWaitForOrderEvent(method, url, token, body, orderID, eventType) {
  let response = null;
  let received = false;
  // realtime-service authenticates the socket with the fresh JWT and derives
  // the user route from claims.sub. It intentionally does not accept a
  // caller-supplied user_id query parameter.
  const wsURL = `${realtime}?token=${encodeURIComponent(token)}`;
  const res = ws.connect(wsURL, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (message.type === eventType && message.aggregate_id === orderID && message.test_run_id === runID) {
          received = true;
          realtimeEvents.add(1);
          socket.close();
        }
      } catch (_) {}
    });
    socket.on('error', () => socket.close());
    response = call(method, url, token, body);
    // Keep the user-scoped order socket open until the event arrives. k6 owns
    // the socket for the lifetime of the active iteration and closes it when
    // the iteration/test is terminated; an artificial 60s timeout can close
    // the socket while Kafka/realtime is still draining a valid event.
  });
  check(res, { [`${eventType} websocket handshake succeeded`]: r => r && r.status === 101 });
  check(received, { [`${eventType} event received`]: () => received });
  return response;
}

function callAndWaitForRealtimeEvent(method, url, token, body, aggregateID, eventType, timeoutMS = 30000, requestCall = call) {
  let received = false;
  let receivedEvent = null;
  let response = null;
  const wsURL = `${realtime}?token=${encodeURIComponent(token)}`;
  const res = ws.connect(wsURL, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (message.type !== eventType || (aggregateID && message.aggregate_id !== aggregateID)) return;
        if (message.test_run_id && message.test_run_id !== runID) return;
        received = true;
        receivedEvent = message;
        realtimeEvents.add(1);
        socket.close();
      } catch (_) {}
    });
    socket.on('error', () => socket.close());
    response = requestCall(method, url, token, body);
    // A deliberately injected 4xx/5xx response is terminal for this command;
    // there cannot be a success notification for it. Do not spend the whole
    // async timeout waiting for an event that must not exist.
    if (!response || response.status < 200 || response.status >= 300) {
      socket.close();
      return;
    }
    socket.setTimeout(() => socket.close(), timeoutMS);
  });
  check(res, { [`${eventType} websocket handshake succeeded`]: r => r && r.status === 101 });
  if (response && response.status >= 200 && response.status < 300) {
    check(received, { [`${eventType} event received`]: () => received });
  }
  return { response, received, event: receivedEvent };
}

function callAndWaitForRealtimeSequence(method, url, token, body, aggregateID, eventTypes, timeoutMS = 30000, requestCall = call, ordered = false, correlateOperation = false) {
  const receivedEvents = {};
  const expectedEvents = new Set(eventTypes);
  let response = null;
  let receivedCount = 0;
  let nextEvent = 0;
  const operationID = correlateOperation ? `${runID}-${eventTypes[0]}-${__VU}-${__ITER}-${Date.now()}` : '';
  const wsURL = `${realtime}?token=${encodeURIComponent(token)}`;
  const res = ws.connect(wsURL, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (aggregateID && message.aggregate_id !== aggregateID) return;
        if (message.test_run_id && message.test_run_id !== runID) return;
        const expectedType = eventTypes[nextEvent];
        if ((ordered && message.type !== expectedType) || (!ordered && !expectedEvents.has(message.type)) || receivedEvents[message.type]) return;
        // Idempotency-Key is not the realtime operation_id. The gateway and
        // downstream services may derive a fresh operation identifier while
        // preserving the command's idempotency key, so comparing the two
        // drops valid notifications and produces false WS failures. The
        // socket is already scoped by authenticated user, aggregate and
        // event type; the command is sent while this socket is open.
        receivedEvents[message.type] = message;
        receivedCount += 1;
        nextEvent += 1;
        realtimeEvents.add(1);
        if (receivedCount === expectedEvents.size) socket.close();
      } catch (_) {}
    });
    socket.on('error', () => socket.close());
    response = requestCall(method, url, token, body, undefined, operationID);
    // A failed command is a terminal outcome. Waiting for success events here
    // made every injected error consume 60–120 seconds and kept the Job alive
    // long after the configured load duration.
    if (!response || response.status < 200 || response.status >= 300) {
      socket.close();
      return;
    }
    socket.setTimeout(() => socket.close(), timeoutMS);
  });
  check(res, { [`${eventTypes.join(' then ')} websocket handshake succeeded`]: r => r && r.status === 101 });
  if (response && response.status >= 200 && response.status < 300) {
    for (const eventType of eventTypes) {
      check(receivedEvents[eventType], { [`${eventType} event received`]: () => Boolean(receivedEvents[eventType]) });
    }
  }
  return { response, received: receivedCount === expectedEvents.size, events: receivedEvents };
}

function waitForRealtimeEvent(token, aggregateID, eventType, timeoutMS = 30000) {
  let received = false;
  const wsURL = `${realtime}?token=${encodeURIComponent(token)}`;
  const res = ws.connect(wsURL, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (message.type !== eventType || (aggregateID && message.aggregate_id !== aggregateID)) return;
        if (message.test_run_id && message.test_run_id !== runID) return;
        received = true;
        realtimeEvents.add(1);
        socket.close();
      } catch (_) {}
    });
    socket.on('error', () => socket.close());
    socket.setTimeout(() => socket.close(), timeoutMS);
  });
  check(res, { [`${eventType} websocket handshake succeeded`]: r => r && r.status === 101 });
  check(received, { [`${eventType} event received`]: () => received });
  return received;
}

function responseJSON(res) {
  try { return res.json(); } catch (_) { return {}; }
}

function gigLog(phase, details) {
  console.log(JSON.stringify({ event: 'gig.flow', phase, ...details }));
}

function verifyRealtimeConnection(token, userID) {
  let ready = false;
  // userID is used only to make the expected authenticated principal explicit
  // in the check label; realtime-service derives it from the JWT subject.
  const url = `${realtime}?token=${encodeURIComponent(token)}`;
  const res = ws.connect(url, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (message.type === 'connection.ready' && message.connection_id) {
          ready = true;
          socket.close();
        }
      } catch (_) {
        socket.close();
      }
    });
    socket.on('error', () => socket.close());
    socket.setTimeout(() => socket.close(), 5000);
  });
  check(res, { [`realtime websocket handshake succeeded for ${userID}`]: r => r && r.status === 101 });
  check(res, { [`realtime websocket connection.ready received for ${userID}`]: () => ready });
}

// Runs the buyer-owned order, chat and review flow inside one authenticated
// websocket lifetime.  Kafka notifications can be delivered after the HTTP
// command returns; keeping this socket open across the whole flow prevents a
// valid notification from being published while no client is listening.
function runBuyerFlowWithPersistentSocket(orderGigID, packageID, buyer, freelancer, buyerUsername, freelancerUsername, gigID) {
  let orderID = '';
  let order = null;
  let connectionReady = false;
  let requirementsReceived = false;
  let messageReceived = false;
  let chatCreatedReceived = false;
  let orderCompletedReceived = false;
  const pendingOrderEvents = [];
  let socketResult = null;
  const wsURL = `${realtime}?token=${encodeURIComponent(buyer)}`;
  socketResult = ws.connect(wsURL, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    const applyOrderEvent = event => {
      if (event.type === 'order.requirements_completed') {
        requirementsReceived = true;
        realtimeEvents.add(1);
      }
      if (event.type === 'order.message_completed') {
        messageReceived = true;
        realtimeEvents.add(1);
      }
      if (event.type === 'chat.created') {
        chatCreatedReceived = true;
        realtimeEvents.add(1);
      }
      if (event.type === 'order.completed') {
        orderCompletedReceived = true;
        realtimeEvents.add(1);
      }
    };

    socket.on('message', raw => {
      try {
        const event = JSON.parse(raw);
        if (event.test_run_id && event.test_run_id !== runID) return;
        if (event.type === 'connection.ready') {
          connectionReady = true;
          return;
        }
        // The order-start request and the WebSocket stream are concurrent.
        // Chat creation can therefore be published before the HTTP response
        // has assigned orderID. Keep matching events until that ID exists;
        // dropping them makes the later timeout report a false WS failure.
        if (!orderID) {
          if (event.aggregate_id) pendingOrderEvents.push(event);
          return;
        }
        if (event.aggregate_id !== orderID) return;
        applyOrderEvent(event);
      } catch (_) {}
    });
    socket.on('error', () => {
      console.log(`Persistent buyer websocket error for scenario=${scenario}`);
    });

    order = call('POST', `${base}/orders/start`, buyer, { gig_id: orderGigID, package_id: packageID }, [201]);
    if (!order || order.status < 200 || order.status >= 300) {
      abortFlow('order was not created');
      socket.close();
      return;
    }
    orderID = id(responseJSON(order), 'order_id', 'resource_id', 'id');
    if (fallbackAccepted(order) && !isUUIDv7OrUUID(orderID)) {
      abortFlow('order fallback returned a non-UUID order ID', { order_id: orderID, http_status: order.status });
      socket.close();
      return;
    }
    if (!orderID) {
      abortFlow('order response did not contain order ID');
      socket.close();
      return;
    }
    for (const event of pendingOrderEvents) {
      if (event.aggregate_id === orderID) applyOrderEvent(event);
    }
    pendingOrderEvents.length = 0;
    const orderBody = responseJSON(order);
    const questions = orderBody.questions || (orderBody.snapshot && orderBody.snapshot.questions) || [];
    const answers = questions.map(q => ({ question_id: q.id || q.question_id, value: `k6 answer ${q.id || q.question_id}` }));
    const requirements = call('POST', `${base}/orders/${orderID}/requirements`, buyer, { order_id: orderID, answers });
    if (!requirements || requirements.status < 200 || requirements.status >= 300) {
      socket.close();
      return;
    }
    const messageCommand = call('POST', `${base}/orders/${orderID}/message`, buyer, { order_id: orderID, message: 'k6 full-system message' });
    if (!messageCommand || messageCommand.status < 200 || messageCommand.status >= 300) {
      socket.close();
      return;
    }
    const confirm = rawCall('POST', `${base}/orders/${orderID}/confirm`, buyer, { order_id: orderID });
    const confirmOK = confirm && confirm.status === 200 && id(responseJSON(confirm), 'payment_id', 'intent_id');
    if (!confirmOK) console.log(`E2E confirm did not produce payment intent: HTTP ${confirm ? confirm.status : 0} body=${confirm ? confirm.body : ''}`);
    check(confirm, { 'POST confirm returned payment intent': () => Boolean(confirmOK) });
    const paymentID = id(responseJSON(confirm), 'payment_id', 'intent_id') || __ENV.K6_PAYMENT_ID || '';
    if (paymentID) {
      const providerIntentID = `pi_fake_${paymentID.replaceAll('-', '')}`;
      call('POST', `${payment}/fake-stripe/webhooks/payment`, buyer, { event_id: `${scenario}-${paymentID}`, provider: 'fake-stripe', event_type: 'payment_intent.captured', payment_intent_id: paymentID, provider_intent_id: providerIntentID, order_id: orderID, status: 'captured' }, [200, 202, 204]);
    }

    let chat = null;
    const chatDeadline = Date.now() + fullSystemFlowTimeoutMs;
    const deliveryDeadline = Date.now() + fullSystemFlowTimeoutMs;
    let chatFlowStarted = false;
    let chatOperationsDone = false;
    let fileID = '';
    let exerciseChat = false;
    const continueChatFlow = () => {
      if (chatFlowStarted) return;
      chatFlowStarted = true;
      exerciseChat = (chat && chat.status === 200 && chatCreatedReceived) || isFaultTarget('chat');
      check(chat, { 'chat created before chat operations': response => response && response.status === 200 });
      check(chatCreatedReceived, { 'chat.created websocket event received before chat operations': () => chatCreatedReceived });
      if (exerciseChat && !chatOperationsDone) {
        const reservation = call('POST', `${base}/users/${buyerUsername}/orders/${orderID}/chat/attachments/upload-url`, buyer, { filename: 'k6.txt', content_type: 'text/plain', size_bytes: 12 }, [201, 200]);
        const attachment = responseJSON(reservation);
        fileID = id(attachment, 'file_id') || __ENV.K6_FILE_ID || '';
        const uploadURL = id(attachment, 'upload_url');
        if (uploadURL) directUpload(uploadURL, 'k6 attachment', 'text/plain', [200, 201, 204]);
        if (fileID) {
          call('POST', `${base}/users/${buyerUsername}/orders/${orderID}/chat/attachments/complete`, buyer, { file_id: fileID }, [200]);
          // The same uploaded object is explicitly attached to the order so
          // order_attachments is exercised separately from chat_messages.
          call('POST', `${base}/orders/${orderID}/attachments/complete`, buyer, { attachment_id: fileID, file_key: fileID }, [200]);
        }
        chatOperationsDone = true;
      }
      // Payment capture and the order-saga state update are asynchronous. A
      // delivery request can briefly arrive while the order is still in the
      // confirmation transition; retry that business response without
      // advancing to the buyer outcome.
      const deliver = call('POST', `${base}/orders/${orderID}/deliver`, freelancer, { order_id: orderID, delivery_message: 'k6 delivery', attachment_ids: fileID ? [fileID] : [] });
      if (!deliver || deliver.status < 200 || deliver.status >= 300) {
        if (Date.now() < deliveryDeadline) {
          chatFlowStarted = false;
          socket.setTimeout(continueChatFlow, 1000);
        } else {
          abortFlow('order delivery was not accepted', { order_id: orderID, http_status: deliver ? deliver.status : 0 });
          socket.close();
        }
        return;
      }

      const continueAfterOutcome = () => {
        if (exerciseChat) {
          const chatMessage = call('POST', `${base}/users/${buyerUsername}/orders/${orderID}/chat/messages`, buyer, { order_id: orderID, text: 'k6 chat', attachment_ids: fileID ? [fileID] : [] }, [201, 200]);
          const messageID = id(responseJSON(chatMessage), 'message_id', 'id') || '00000000-0000-0000-0000-000000000003';
          call('PATCH', `${base}/users/${buyerUsername}/orders/${orderID}/chat/messages/${messageID}`, buyer, { text: 'k6 edited' });
          call('DELETE', `${base}/users/${buyerUsername}/orders/${orderID}/chat/messages/${messageID}`, buyer);
        }
        call('GET', `${base}/search?query=k6&sort=0&order=0`, buyer, null, [200]);
        call('GET', `${base}/users/${freelancerUsername}`, buyer);
        call('GET', `${base}/users/${freelancerUsername}/gigs`, freelancer, null, [200]);
        call('GET', `${base}/users/${freelancerUsername}/gigs/${gigID}`, buyer);
        call('GET', `${base}/users/${buyerUsername}/orders/${orderID}?role=customer`, buyer, null, [200]);
        call('GET', `${base}/users/${buyerUsername}/orders/${orderID}/requirements`, buyer, null, [200]);
        call('GET', `${base}/users/${freelancerUsername}/orders/${orderID}/delivery`, freelancer);
        fullSystemCompletedFlows.add(1);

        // Keep the socket alive after the business flow so queued Kafka
        // notifications can be dispatched through the k6 WebSocket event loop.
        socket.setTimeout(() => {
          check(connectionReady, { 'buyer websocket connection.ready received': () => connectionReady });
          check(requirementsReceived, { 'order.requirements_completed event received': () => requirementsReceived });
          check(messageReceived, { 'order.message_completed event received': () => messageReceived });
          check(chatCreatedReceived, { 'chat.created event received': () => chatCreatedReceived });
          socket.close();
        }, fullSystemFlowTimeoutMs);
      };

      const acceptAndWaitForCompletion = () => {
        const accepted = requiredCall('POST', `${base}/orders/${orderID}/accept`, buyer, { order_id: orderID });
        if (!accepted || accepted.status < 200 || accepted.status >= 300) {
          abortFlow('order acceptance was not accepted', { order_id: orderID, http_status: accepted ? accepted.status : 0 });
          socket.close();
          return;
        }
        const completionDeadline = Date.now() + fullSystemFlowTimeoutMs;
        const waitForCompletion = () => {
          if (orderCompletedReceived) {
            const review = call('POST', `${base}/orders/${orderID}/reviews`, buyer, { order_id: orderID, rating: 5, content: 'k6' });
            check(review, { 'accepted order review was created': response => response && response.status >= 200 && response.status < 300 });
            continueAfterOutcome();
            return;
          }
          if (Date.now() >= completionDeadline) {
            check(false, { 'order.completed websocket event received before review': () => false });
            continueAfterOutcome();
            return;
          }
          socket.setTimeout(waitForCompletion, 1000);
        };
        socket.setTimeout(waitForCompletion, 1);
      };

      const outcome = Math.random();
      if (outcome < 0.34) {
        console.log(JSON.stringify({ event: 'full-system.order_outcome', order_id: orderID, outcome: 'accepted' }));
        acceptAndWaitForCompletion();
        return;
      } else if (outcome < 0.67) {
        console.log(JSON.stringify({ event: 'full-system.order_outcome', order_id: orderID, outcome: 'revision_then_accept' }));
        const revision = requiredCall('POST', `${base}/orders/${orderID}/request-revision`, buyer, { order_id: orderID, reason: 'k6 revision request' }, [200]);
        if (revision.status < 200 || revision.status >= 300) {
          abortFlow('revision request was not accepted', { order_id: orderID, http_status: revision.status });
          continueAfterOutcome();
          return;
        }
        const revisedDelivery = requiredCall('POST', `${base}/orders/${orderID}/deliver`, freelancer, { order_id: orderID, delivery_message: 'k6 revised delivery', attachment_ids: fileID ? [fileID] : [] });
        if (revisedDelivery.status < 200 || revisedDelivery.status >= 300) {
          abortFlow('revised delivery was not accepted', { order_id: orderID, http_status: revisedDelivery.status });
          continueAfterOutcome();
          return;
        }
        acceptAndWaitForCompletion();
        return;
      } else {
        console.log(JSON.stringify({ event: 'full-system.order_outcome', order_id: orderID, outcome: 'disputed_admin_settlement' }));
        const dispute = requiredCall('POST', `${base}/orders/${orderID}/dispute`, buyer, { order_id: orderID, reason: 'k6 test' }, [200]);
        if (dispute.status < 200 || dispute.status >= 300) {
          abortFlow('dispute could not be opened', { order_id: orderID, http_status: dispute.status });
          continueAfterOutcome();
          return;
        }
        // Disputes are resolved by an admin, not by the buyer who opened them.
        // A 50/50 settlement exercises both fake Stripe paths: a transfer to
        // the freelancer and a refund to the buyer.
        const resolution = requiredCall('POST', `${base}/orders/${orderID}/dispute/resolve`, defaultAdminToken, {
          freelancer_percentage: 50,
          customer_percentage: 50,
          reason: 'k6 admin resolution',
        }, [200]);
        if (resolution.status < 200 || resolution.status >= 300) {
          abortFlow('dispute could not be resolved by admin', { order_id: orderID, http_status: resolution.status });
          continueAfterOutcome();
          return;
        }
      }
      continueAfterOutcome();
    };

    // Do not sleep in the websocket callback. Timers return control to k6's
    // event loop, allowing the chat.created frame to reach the message handler.
    const waitForChat = () => {
      chat = call('GET', `${base}/users/${buyerUsername}/orders/${orderID}/chat`, buyer);
      if ((chatCreatedReceived && chat.status === 200) || Date.now() >= chatDeadline) {
        continueChatFlow();
        return;
      }
      socket.setTimeout(waitForChat, 1000);
    };
    socket.setTimeout(waitForChat, 1);
  });
  check(socketResult, { 'persistent buyer websocket handshake succeeded': r => r && r.status === 101 });
}

// The collector is a separate k6 VU because k6's websocket callback owns the
// socket for its lifetime. It receives notifications concurrently with the
// HTTP workload and never polls an operation endpoint or repeats a command.
export function collectRealtimeEvents() {
  const token = __ENV.K6_WS_TOKEN || defaultToken;
  const timeout = durationMilliseconds(__ENV.OFM_FULL_K6_DURATION || __ENV.OFM_FULL_K6_MAX_DURATION, 300000) + 30000;
  const url = `${realtime}?token=${encodeURIComponent(token)}`;
  const res = ws.connect(url, { tags: { endpoint: '/ws', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (message.type === 'connection.ready') {
          return;
        }
        if (!message.type || message.test_run_id !== runID) {
          realtimeMismatchedEvents.add(1);
          return;
        }
        realtimeEvents.add(1);
        if (message.type === 'registration.completed' || message.type === 'registration.failed' || message.status === 'completed' || message.status === 'failed') realtimeTerminalEvents.add(1);
      } catch (_) {
        realtimeMismatchedEvents.add(1);
      }
    });
    socket.on('error', () => socket.close());
    socket.setTimeout(() => socket.close(), timeout);
  });
  check(res, { 'realtime websocket collector handshake succeeded': r => r && r.status === 101 });
}

// The collector is event-driven, but it must not outlive the load window by a
// fixed five minutes. Keep a short bounded grace period for terminal WS
// events, derived from the experiment duration.
const collectorWindow = durationMilliseconds(__ENV.OFM_FULL_K6_DURATION || __ENV.OFM_FULL_K6_MAX_DURATION, 300000)
  + durationMilliseconds(__ENV.OFM_FULL_K6_DRAIN_TIMEOUT || '120s', 120000);
const collectorDuration = `${collectorWindow}ms`;
options.scenarios.realtime_collector = {
  executor: 'constant-vus',
  vus: 1,
  duration: collectorDuration,
  gracefulStop: '0s',
  exec: 'collectRealtimeEvents',
};

function id(body, ...keys) {
  for (const key of keys) if (body && body[key]) return body[key];
  return '';
}

function completeRegistrationViaRealtime(sessionID, clientID) {
  let codeSent = false;
  let completed = false;
  let failed = false;
  let completeBody = {};
  const url = `${realtime}?session_id=${encodeURIComponent(sessionID)}`;
  const timeoutMS = remainingExperimentMilliseconds();
  const registrationLog = (event, fields = {}) => {
    console.log(JSON.stringify({
      event: `registration.ws.${event}`,
      session_id: sessionID,
      client_id: clientID,
      ...fields,
    }));
  };

  // Keep the first WS phase free of nested HTTP calls. Under concurrent k6
  // websocket callbacks, issuing verify-email from the message callback could
  // lose the HTTP request entirely; the saga then correctly remained in
  // code_sent. The phase boundary is an event, not a polling interval.
  registrationLog('connect.attempt', { phase: 'code_sent', timeout_ms: timeoutMS });
  const codeSocket = ws.connect(url, { tags: { endpoint: '/ws/registration', scenario_id: scenario, phase: 'code_sent' } }, socket => {
    registrationLog('connect.open', { phase: 'code_sent' });
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        registrationLog('message', { phase: 'code_sent', type: message.type || '', aggregate_id: message.aggregate_id || '' });
        if (message.type === 'connection.ready') return;
        if (message.aggregate_id !== sessionID) {
          registrationLog('message.ignored', { phase: 'code_sent', reason: 'aggregate_id_mismatch', type: message.type || '', aggregate_id: message.aggregate_id || '' });
          return;
        }
        if (message.type === 'registration.code_sent') {
          codeSent = true;
          realtimeEvents.add(1);
          registrationLog('code_sent', { phase: 'code_sent' });
          socket.close();
        }
      } catch (err) {
        registrationLog('message.invalid', { phase: 'code_sent', error: String(err) });
      }
    });
    socket.on('error', err => { registrationLog('error', { phase: 'code_sent', error: String(err) }); socket.close(); });
    socket.on('close', () => registrationLog('close', { phase: 'code_sent', code_sent: codeSent }));
    socket.setTimeout(() => { registrationLog('timeout', { phase: 'code_sent' }); socket.close(); }, timeoutMS);
  });
  registrationLog('connect.result', { phase: 'code_sent', status: codeSocket && codeSocket.status });

  check(codeSocket, { 'registration WS handshake succeeded': r => r && r.status === 101 });
  check(codeSent, { 'registration code_sent received through WS': () => codeSent });
  if (!codeSent) {
    registrationLog('code_sent.missing', { phase: 'code_sent' });
    check(false, { 'registration completed through WS': () => false });
    return { ok: false, body: {} };
  }

  // Establish the completion socket before sending verify-email, but issue
  // the HTTP request after ws.connect returns. k6 does not reliably execute
  // synchronous HTTP requests from a websocket message callback; doing so
  // previously left every registration at code_sent without a verify request.
  let completionReady = false;
  const preparationSocket = ws.connect(url, { tags: { endpoint: '/ws/registration', scenario_id: scenario } }, socket => {
    socket.on('message', raw => {
      try {
        const message = JSON.parse(raw);
        if (message.type === 'connection.ready') {
          completionReady = true;
          // The registration connection is now registered locally. The
          // pending-event buffer covers the short gap before the listener
          // below is opened.
          socket.close();
          return;
        }
      } catch (_) {}
    });
    socket.on('error', () => socket.close());
    socket.setTimeout(() => socket.close(), timeoutMS);
  });
  check(preparationSocket, { 'registration completion WS handshake succeeded': r => r && r.status === 101 });
  check(completionReady, { 'registration completion WS registered before verify': () => completionReady });

  const verify = rawCall('POST', `${base}/auth/sign-up/verify-email`, '', {
    session_id: sessionID,
    client_id: clientID,
    code: __ENV.K6_VERIFICATION_CODE || '123456',
  });
  check(verify, { 'registration verification command accepted': r => r && r.status >= 200 && r.status < 300 });
  if (!verify || verify.status < 200 || verify.status >= 300) failed = true;

  // Listen for the terminal event emitted by verify-email. Keep this
  // socket open until the experiment deadline. Redis-backed realtime
  // buffering makes a reconnect loop unnecessary, and counting each 10-second
  // reconnect timeout as a failed check made successful registrations look
  // broken in k6.
  // The completion event is durably buffered by realtime-service, but the
  // event may be emitted between verify-email and the first terminal socket
  // registration under concurrent load. Reconnect in short bounded attempts
  // so a missed delivery cannot strand a write VU for the whole experiment.
  const terminalDeadline = Date.now() + Math.min(fullSystemFlowTimeoutMs, remainingExperimentMilliseconds());
  let completionSocket = null;
  let terminalStatus = 0;
  let terminalAttempt = 0;
  while (!completed && !failed && Date.now() < terminalDeadline) {
    terminalAttempt += 1;
    const terminalTimeoutMS = Math.min(10000, Math.max(1, terminalDeadline - Date.now()));
    registrationLog('connect.attempt', { phase: 'terminal', attempt: terminalAttempt, timeout_ms: terminalTimeoutMS });
    completionSocket = ws.connect(url, { tags: { endpoint: '/ws/registration', scenario_id: scenario, phase: 'terminal', attempt: String(terminalAttempt) } }, socket => {
      registrationLog('connect.open', { phase: 'terminal', attempt: terminalAttempt });
      socket.on('message', raw => {
        try {
          const message = JSON.parse(raw);
          registrationLog('message', { phase: 'terminal', attempt: terminalAttempt, type: message.type || '', aggregate_id: message.aggregate_id || '' });
          if (message.type === 'connection.ready') return;
          if (message.aggregate_id !== sessionID) {
            registrationLog('message.ignored', { phase: 'terminal', attempt: terminalAttempt, reason: 'aggregate_id_mismatch', type: message.type || '', aggregate_id: message.aggregate_id || '' });
            return;
          }
          if (message.type === 'registration.completed') {
            completed = true;
            realtimeEvents.add(1);
            realtimeTerminalEvents.add(1);
            registrationLog('completed', { phase: 'terminal', attempt: terminalAttempt });
            socket.close();
          } else if (message.type === 'registration.failed') {
            failed = true;
            realtimeEvents.add(1);
            realtimeTerminalEvents.add(1);
            registrationLog('failed', { phase: 'terminal', attempt: terminalAttempt, error_code: message.error_code || '' });
            socket.close();
          }
        } catch (err) {
          registrationLog('message.invalid', { phase: 'terminal', attempt: terminalAttempt, error: String(err) });
        }
      });
      socket.on('error', err => { registrationLog('error', { phase: 'terminal', attempt: terminalAttempt, error: String(err) }); socket.close(); });
      socket.on('close', () => registrationLog('close', { phase: 'terminal', attempt: terminalAttempt, completed, failed }));
      socket.setTimeout(() => { registrationLog('timeout', { phase: 'terminal', attempt: terminalAttempt }); socket.close(); }, terminalTimeoutMS);
    });
    terminalStatus = completionSocket && completionSocket.status;
    registrationLog('connect.result', { phase: 'terminal', attempt: terminalAttempt, status: terminalStatus });
  }
  check(completionSocket, { 'registration terminal WS handshake succeeded': r => r && r.status === 101 });

  // verify-email emits registration.completed. Only after that event is
  // observed may /complete issue the final auth tokens.
  if (completed && !failed) {
    const complete = rawCall('POST', `${base}/auth/sign-up/complete`, '', { session_id: sessionID, client_id: clientID });
    completeBody = responseJSON(complete);
    if (!complete || complete.status < 200 || complete.status >= 300) {
      failed = true;
      completeBody = {};
    }
    check(complete, { 'registration completion command accepted': r => r && r.status >= 200 && r.status < 300 });
  }

  check({ completed, failed }, { 'registration completed through WS': state => state.completed && !state.failed });
  registrationLog('result', { code_sent: codeSent, completed, failed });
  return { ok: completed && !failed, body: completeBody };
}

function firstPackage(body) {
  const packages = body && (body.packages || (body.gig && body.gig.packages));
  if (!Array.isArray(packages) || packages.length < 1 || packages.length > 3) return '';
  const pkg = packages[0] || {};
  return pkg.id || pkg.package_id || '';
}

function isUUIDv7OrUUID(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(value || '').trim());
}

function readDraftUntilProjected(url, token, gigID) {
  let attempt = 0;
  let delay = 0.25;
  let draft = null;
  while (attempt < 120) {
    attempt += 1;
    draft = rawCall('GET', url, token, null, [503], ['unknown identifier']);
    const body = responseJSON(draft);
    const packages = body.packages || (body.gig && body.gig.packages) || [];
    const complete = draft.status >= 200 && draft.status < 300 && body.packages_completed === true && Array.isArray(packages) && packages.length === 3;
    const mappingPending = draft.status === 400 && body.error === 'unknown identifier';
    const retryable = draft.status === 503 || mappingPending || (draft.status >= 200 && draft.status < 300 && !complete);
    gigLog('draft_read_attempt', { gig_id: gigID, attempt, http_status: draft.status, package_count: Array.isArray(packages) ? packages.length : -1, packages_completed: body.packages_completed === true, mapping_pending: mappingPending, retryable });
    if (complete || (draft.status >= 400 && !retryable)) return { response: draft, body, packages };
    sleep(delay);
    delay = Math.min(delay * 2, 5);
  }
  return { response: draft, body: responseJSON(draft), packages: [] };
}

// Read traffic is an independent arrival-rate lane. It must not wait for the
// long write workflow, otherwise the requested read RPS is never generated.
export function readWorkload() {
  if (!readTokens[__VU]) {
    const user = createUser('read');
    readTokens[__VU] = user.token;
  }
  const token = readTokens[__VU];
  if (!token) {
    check(false, { 'read workload user registration succeeded': () => false });
    return;
  }
  const response = rawCall('GET', `${base}/auth/me`, token, null);
  check(response, { 'read workload returned non-5xx': r => r && r.status > 0 && r.status < 500 });
}

function createUser(prefix) {
  const suffix = `${Date.now()}-${__VU}-${__ITER}`;
  const username = `${prefix}${suffix}`.replace(/[^a-zA-Z0-9]/g, '').slice(0, 28);
  const email = `${username}@example.test`;
  const password = __ENV.K6_TEST_PASSWORD || 'Password123!';
  // Leave client_id empty: registration-saga owns this identity and generates
  // the UUIDv7. If the gateway falls back to the monolith, the monolith owns
  // the numeric client identity instead. Always use the returned value for
  // the subsequent WebSocket and verification requests.
  const signup = requiredCall('POST', `${base}/auth/sign-up`, '', { email, password, username, firstName: prefix, surname: 'K6' }, [202]);
  const signupBody = responseJSON(signup);
  const sessionID = id(signupBody, 'session_id');
  const responseClientID = id(signupBody, 'client_id');
  if (!sessionID) return { token: '', username, userID: '' };
  const registration = completeRegistrationViaRealtime(sessionID, responseClientID);
  if (!registration.ok) return { token: '', username, userID: '' };
  return { token: registration.body.access_token || '', username, userID: registration.body.user_id || signupBody.user_id || '' };
}

function waitForUserProjection(username, token) {
  const res = rawCall('GET', `${base}/users/${username}`, token, null);
  // Registration completion and the public detailed-user read model are
  // separate boundaries. A single GET may legitimately observe the write
  // model before the read path has converged; do not turn that race into a
  // fake E2E failure or add polling here. The final projection audit is the
  // authoritative consistency check. Transport/5xx failures remain hard
  // failures.
  const healthy = res.status > 0 && res.status < 500;
  check(res, { [`user projection request returned non-5xx for ${username}`]: () => healthy });
  return healthy;
}

export default function () {
  const token = defaultToken;
  let freelancer = defaultFreelancerToken;
  let buyer = defaultBuyerToken;
  let freelancerUsername = __ENV.K6_FREELANCER_USERNAME || 'alex1';
  let buyerUsername = __ENV.K6_BUYER_USERNAME || 'buyer1';
  let freelancerUserID = __ENV.K6_FREELANCER_USER_ID || '';
  let buyerUserID = __ENV.K6_BUYER_USER_ID || '';
  if (__ENV.K6_CREATE_USERS === 'true') {
    const seller = createUser('seller');
    const customer = createUser('buyer');
    if (!seller.token || !customer.token) {
      check(false, { 'both test users registered': () => false });
      return;
    }
    // The load window controls creation of new iterations only. This
    // registration started before the deadline, so its already-running flow
    // must continue through the remaining business stages during the drain.
    freelancer = seller.token; freelancerUsername = seller.username;
    buyer = customer.token; buyerUsername = customer.username;
    freelancerUserID = seller.userID;
    buyerUserID = customer.userID;
    // Validate the authenticated realtime channel only after registration has
    // issued a real gateway JWT. The old preflight used a static fixture token
    // which could be syntactically valid but no longer represented an active
    // user after a database reset, creating false failed checks before the
    // actual E2E flow started.
    verifyRealtimeConnection(freelancer, freelancerUserID);
    verifyRealtimeConnection(buyer, buyerUserID);
    if (!waitForUserProjection(freelancerUsername, freelancer) || !waitForUserProjection(buyerUsername, buyer)) { abortFlow('user projection unavailable'); return; }
  } else {
    if (freelancerUserID) verifyRealtimeConnection(freelancer, freelancerUserID);
    if (buyerUserID) verifyRealtimeConnection(buyer, buyerUserID);
  }
  // Exercise the remaining auth surface in the same run. With fixture tokens,
  // refresh/sign-out are contract probes; with K6_CREATE_USERS the signup
  // lifecycle above is the successful path.
  const signIn = requiredCall('POST', `${base}/auth/sign-in`, '', { identifier: freelancerUsername, password: __ENV.K6_TEST_PASSWORD || 'Password123!' }, [200]);
  const signInBody = responseJSON(signIn);
  const authenticatedToken = signInBody.access_token || '';
  check(authenticatedToken, { 'sign-in returned access token': token => Boolean(token) });
  if (signIn.status < 200 || signIn.status >= 300 || !authenticatedToken) { abortFlow('authentication failed: sign-in did not return an access token'); return; }
  // Validate the token issued by the auth-service itself. Registration
  // completion is a saga boundary; sign-in is the explicit auth boundary and
  // provides the token used by the rest of this authenticated workload.
  const authMe = requiredCall('GET', `${base}/auth/me`, authenticatedToken, null, [200]);
  if (authMe.status < 200 || authMe.status >= 300) { abortFlow('authentication failed: auth/me rejected sign-in token'); return; }
  freelancer = authenticatedToken;
  call('POST', `${base}/auth/refresh`, '', { refresh_token: __ENV.K6_REFRESH_TOKEN || '' }, [200, 400, 401, 404]);
  call('POST', `${base}/auth/sign-out`, freelancer, { refresh_token: __ENV.K6_REFRESH_TOKEN || '' }, [204, 400, 401, 404]);
  if (__ENV.K6_CREATE_USERS !== 'true') {
    call('POST', `${base}/auth/sign-up`, '', {}, [400]);
    call('POST', `${base}/auth/sign-up/verify-email`, '', {}, [400]);
    call('POST', `${base}/auth/sign-up/complete`, '', {}, [400]);
  }
  const onboarding = call('POST', `${base}/freelancer/onboarding/start`, freelancer, { email: `${freelancerUsername}@example.test`, country: 'US', return_url: 'http://localhost/fake-stripe/return', refresh_url: 'http://localhost/fake-stripe/refresh' }, [200, 202]);
  const stripeAccountID = id(responseJSON(onboarding), 'stripe_account_id');
  if (!stripeAccountID) console.log(`E2E onboarding did not return stripe account: HTTP ${onboarding.status} body=${onboarding.body}`);
  if (stripeAccountID) {
    call('POST', `${payment}/fake-stripe/webhooks/connect`, freelancer, {
      event_id: `${scenario}-connect-${stripeAccountID}`,
      provider: 'fake-stripe',
      event_type: 'account.updated',
      user_id: id(responseJSON(onboarding), 'user_id'),
      stripe_account_id: stripeAccountID,
      details_submitted: true,
      charges_enabled: true,
      payouts_enabled: true,
    }, [202]);
  }
  const gigCreateWait = callAndWaitForRealtimeEvent('POST', `${base}/gigs/drafts`, freelancer, {}, '', 'gig.created', asyncCommandTimeout);
  const gig = gigCreateWait.response;
  const gigID = id(responseJSON(gig), 'gig_id', 'id') || gigCreateWait.event?.aggregate_id || '';
  gigLog('created', { gig_id: gigID, http_status: gig.status, projection_event_received: gigCreateWait.received, projection_event: gigCreateWait.event?.type || '' });
  // A dependent step must never invent a fixture identifier when the create
  // command did not return an aggregate identity. Under fallback this used to
  // turn one real availability failure into a cascade of synthetic
  // "unknown identifier" requests and overload the monolith further.
  if (!gigID || gig.status < 200 || gig.status >= 300) { abortFlow('gig draft was not created', { gig_id: gigID, http_status: gig.status }); return; }
  if (!gigCreateWait.received) { abortFlow('gig.created event was not received', { gig_id: gigID, http_status: gig.status }); return; }

  const basicInfoWait = callAndWaitForRealtimeSequence('PATCH', `${base}/gigs/${gigID}/basic-info`, freelancer, { gig_id: gigID, title: 'k6 full system gig', short_info: 'k6', description: 'k6 full system', category_id: 1001, currency: 'usd' }, gigID, ['gig.updated', 'gig.projection_completed'], asyncCommandTimeout, call, false, true);
  const basicInfo = basicInfoWait.response;
  gigLog('basic_info', { gig_id: gigID, http_status: basicInfo.status, projection_received: basicInfoWait.received, projection_events: Object.keys(basicInfoWait.events || {}) });
  if (basicInfo.status < 200 || basicInfo.status >= 300) { abortFlow('gig basic info failed', { gig_id: gigID, http_status: basicInfo.status }); return; }
  const packagesWait = callAndWaitForRealtimeSequence('PUT', `${base}/gigs/${gigID}/packages`, freelancer, { gig_id: gigID, packages: [
    { tier: 'basic', description: 'basic', delivery_days: 3, price_cents: 10000 },
    { tier: 'standard', description: 'standard', delivery_days: 5, price_cents: 20000 },
    { tier: 'premium', description: 'premium', delivery_days: 7, price_cents: 30000 },
  ] }, gigID, ['gig.updated', 'gig.projection_completed'], asyncCommandTimeout, call, false, true);
  const packages = packagesWait.response;
  gigLog('packages_command', { gig_id: gigID, http_status: packages.status, response_body: responseJSON(packages), projection_received: packagesWait.received, projection_events: Object.keys(packagesWait.events || {}) });
  if (packages.status < 200 || packages.status >= 300) { abortFlow('gig packages failed', { gig_id: gigID, http_status: packages.status }); return; }
  const requirementsWait = callAndWaitForRealtimeSequence('PUT', `${base}/gigs/${gigID}/requirements`, freelancer, { gig_id: gigID, questions: [{ content: 'What do you need?' }, { content: 'Reference?' }] }, gigID, ['gig.updated', 'gig.projection_completed'], asyncCommandTimeout, call, false, true);
  const requirements = requirementsWait.response;
  gigLog('requirements', { gig_id: gigID, http_status: requirements.status, projection_received: requirementsWait.received, projection_events: Object.keys(requirementsWait.events || {}) });
  if (requirements.status < 200 || requirements.status >= 300) { abortFlow('gig requirements failed', { gig_id: gigID, http_status: requirements.status }); return; }
  const mediaForm = new FormData();
  mediaForm.append('files', http.file('k6 cover', 'k6-cover.txt', 'text/plain'));
  mediaForm.append('files', http.file('k6 gallery', 'k6-gallery.txt', 'text/plain'));
  const mediaWait = callAndWaitForRealtimeSequence('PUT', `${base}/gigs/${gigID}/media`, freelancer, mediaForm, gigID, ['gig.updated', 'gig.projection_completed'], asyncCommandTimeout, multipartCall, false, true);
  const media = mediaWait.response;
  gigLog('media', { gig_id: gigID, http_status: media.status, projection_received: mediaWait.received, projection_events: Object.keys(mediaWait.events || {}) });
  if (media.status < 200 || media.status >= 300) { abortFlow('gig media failed', { gig_id: gigID, http_status: media.status }); return; }
  if (!mediaWait.received) {
    abortFlow('gig media projection was not completed', { gig_id: gigID, http_status: media.status });
    return;
  }
  const draftResult = readDraftUntilProjected(`${base}/gigs/${gigID}/draft`, freelancer, gigID);
  const draft = draftResult.response;
  const draftBody = draftResult.body;
  const draftPackages = draftBody.packages || (draftBody.gig && draftBody.gig.packages) || [];
  gigLog('draft_read', { gig_id: gigID, http_status: draft.status, package_count: Array.isArray(draftPackages) ? draftPackages.length : -1, package_ids: Array.isArray(draftPackages) ? draftPackages.map(pkg => pkg.id || pkg.package_id || '') : [], response_body: draftBody });
  if (draft.status < 200 || draft.status >= 300) { abortFlow('gig draft read failed', { gig_id: gigID, http_status: draft.status, response_body: draftBody }); return; }
  // PUT /packages is an asynchronous CUD command and intentionally returns a
  // minimal acknowledgement. Read the committed gig draft and use one of its
  // real package IDs for checkout; never fall back to a fixture or synthetic
  // package ID.
  const packageID = firstPackage(draftBody);
  if (!packageID) { abortFlow('gig draft has no committed packages', { gig_id: gigID, http_status: draft.status, package_count: Array.isArray(draftPackages) ? draftPackages.length : -1, response_body: draftBody }); return; }
  // A fallback draft is returned with the monolith's numeric gig/package IDs.
  // Do not mix those IDs with the original UUID gig ID when starting an order:
  // the order saga would ask gig-service to resolve a numeric package ID and
  // correctly return "invalid package tier". Keep both identifiers in the
  // same boundary for this request.
  const responseGigID = id(draftBody, 'gig_id', 'id');
  const orderGigID = responseGigID && !isUUIDv7OrUUID(responseGigID) ? responseGigID : gigID;
  let publishWait;
  let publish;
  const publishRetryDelays = [1, 5, 10, 30, 60];
  const publishMaxAttempts = publishRetryDelays.length + 1;
  for (let publishAttempt = 1; publishAttempt <= publishMaxAttempts; publishAttempt += 1) {
    // The gateway may route draft and publish independently. Re-read the
    // draft before every attempt so a fallback projection can converge in
    // either backend before publish is retried.
    const readyDraft = readDraftUntilProjected(`${base}/gigs/${gigID}/draft`, freelancer, gigID);
    const readyBody = readyDraft.body || {};
    const readyPackages = readyBody.packages || (readyBody.gig && readyBody.gig.packages) || [];
    const ready = readyDraft.response.status >= 200 && readyDraft.response.status < 300 &&
      readyBody.basic_info_completed === true && readyBody.packages_completed === true &&
      readyBody.requirements_completed === true && readyBody.media_completed === true &&
      Array.isArray(readyPackages) && readyPackages.length === 3;
    gigLog('publish_ready_check', { gig_id: gigID, attempt: publishAttempt, http_status: readyDraft.response.status, ready });
    if (!ready) {
      if (publishAttempt <= publishRetryDelays.length) sleep(publishRetryDelays[publishAttempt - 1]);
      continue;
    }
    publishWait = callAndWaitForRealtimeSequence(
      'POST', `${base}/gigs/${gigID}/publish`, freelancer,
      { gig_id: gigID }, gigID, ['gig.published', 'gig.projection_completed'], asyncCommandTimeout, call, false, true
    );
    publish = publishWait.response;
    gigLog('publish', { gig_id: gigID, attempt: publishAttempt, http_status: publish.status, projection_received: publishWait.received, projection_events: Object.keys(publishWait.events || {}) });
    if (publish.status >= 200 && publish.status < 300) break;
    const retryablePublish = publish.status === 412 || publish.status === 503;
    if (!retryablePublish || publishAttempt === publishMaxAttempts) break;
    sleep(publishRetryDelays[publishAttempt - 1]);
  }
  if (!publish || publish.status < 200 || publish.status >= 300) { abortFlow('gig publish failed', { gig_id: gigID, http_status: publish && publish.status }); return; }
  // Publish is an asynchronous command. The 202 response means the command
  // was accepted; order creation must wait for both events on the same socket
  // so the projection notification cannot arrive between two connections.
  if (!publishWait.received) {
    abortFlow('gig projection was not completed', { gig_id: gigID, http_status: publish.status });
    return;
  }

  runBuyerFlowWithPersistentSocket(orderGigID, packageID, buyer, freelancer, buyerUsername, freelancerUsername, gigID);
}
