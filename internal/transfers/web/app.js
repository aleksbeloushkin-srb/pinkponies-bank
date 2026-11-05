'use strict';

// Friendly copy per status. The chip next to it always shows the exact
// status name, so the joke never hides the fact.
const MOOD = {
  pending: 'Saddling up the ponies…',
  held: 'The money is held; the ponies wait at the gate',
  completed: 'Ponies delivered!',
  rejected: 'The ponies were turned away',
  canceled: 'The ponies went home',
};
const FINAL = new Set(['completed', 'rejected', 'canceled']);
const STEP_NAMES = {
  hold: 'Hold the money',
  screen: 'Screening',
  capture: 'Move the money',
  release: 'Give the money back',
};
const REASONS = {
  insufficient_funds: 'insufficient funds',
  account_not_found: 'unknown account',
  currency_mismatch: 'the accounts use other currencies',
  screening_declined: 'declined by screening',
  screening_unavailable: 'screening was unavailable',
  screening_review_timeout: 'screening took too long',
  hold_expired: 'the reservation expired',
};
const EXPONENT = { EUR: 2, USD: 2, GBP: 2, CHF: 2, JPY: 0 };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const byId = (id) => document.getElementById(id);

let accounts = [];       // as last read from GET /v1/accounts
let notificationsURL = '';

function newKey() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  return 'key-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
}

// toMinor converts "12.50" to 1250 without floating point.
function toMinor(text, currency) {
  const exp = EXPONENT[currency] ?? 2;
  const m = /^(\d+)(?:[.,](\d+))?$/.exec(text.trim());
  if (!m) throw new Error('Enter an amount like 12.50');
  const frac = m[2] || '';
  if (frac.length > exp) throw new Error(`${currency} has ${exp} decimal places`);
  const minor = Number(m[1] + frac.padEnd(exp, '0'));
  if (minor <= 0) throw new Error('The amount must be above zero');
  return minor;
}

function formatAmount(minor, currency) {
  const exp = EXPONENT[currency] ?? 2;
  return new Intl.NumberFormat(undefined, {
    style: 'currency', currency, minimumFractionDigits: exp, maximumFractionDigits: exp,
  }).format(minor / 10 ** exp);
}

function accountName(id) {
  const a = accounts.find((x) => x.id === id);
  return a ? a.name : id.slice(0, 8);
}

// --- Accounts ---------------------------------------------------------------

async function loadAccounts() {
  const error = byId('accounts-error');
  try {
    const res = await fetch('/v1/accounts');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const before = new Map(accounts.map((a) => [a.id, a.available_minor]));
    accounts = (await res.json()).accounts;
    error.textContent = '';
    renderAccounts(before);
  } catch (e) {
    error.textContent = `Could not read the accounts (${e.message}); the ledger may be unavailable.`;
  }
}

function cell(label, text) {
  const td = document.createElement('td');
  td.dataset.label = label;
  td.textContent = text;
  return td;
}

function renderAccounts(before) {
  const body = byId('accounts');
  body.replaceChildren(...accounts.map((a) => {
    const tr = document.createElement('tr');
    tr.append(
      cell('Account', a.name),
      cell('Posted', formatAmount(a.posted_minor, a.currency)),
      cell('Held', formatAmount(a.held_minor, a.currency)),
      cell('Available', formatAmount(a.available_minor, a.currency)),
    );
    if (before.has(a.id) && before.get(a.id) !== a.available_minor) tr.classList.add('changed');
    return tr;
  }));
  fillSelect(document.querySelector('[name="source"]'), 0);
  fillSelect(document.querySelector('[name="destination"]'), 1);
  fillSelect(byId('feed-account'), 0);
}

function fillSelect(select, defaultIndex) {
  const keep = select.value;
  select.replaceChildren(...accounts.map((a) => {
    const o = document.createElement('option');
    o.value = a.id;
    o.textContent = `${a.name} (${formatAmount(a.available_minor, a.currency)} available)`;
    return o;
  }));
  if (accounts.some((a) => a.id === keep)) select.value = keep;
  else if (accounts[defaultIndex]) select.value = accounts[defaultIndex].id;
}

// Several transfers can finish at once; refresh the balances once.
let refreshTimer = null;
function refreshAccountsSoon() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(loadAccounts, 300);
}

// --- Creating a transfer ----------------------------------------------------

async function postTransfer(body, key) {
  const res = await fetch('/v1/transfers', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  return { status: res.status, data, retryAfter: Number(res.headers.get('Retry-After')) || 0 };
}

// postWithRetry repeats the same request, with the same Idempotency-Key, on
// a network error, a 5xx or a 429. It throws if no definite answer arrives.
async function postWithRetry(body, key) {
  let lastError = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await postTransfer(body, key);
      if (res.status < 500 && res.status !== 429) return res;
      lastError = new Error(`HTTP ${res.status}`);
      if (attempt < 3) await sleep(Math.max(res.retryAfter * 1000, 1000 * attempt));
    } catch (e) {
      lastError = e;
      if (attempt < 3) await sleep(1000 * attempt);
    }
  }
  throw lastError;
}

// The Idempotency-Key belongs to what the user wants to send, not to a
// click. A retry after a network error or a lost answer reuses it, so the
// server returns the transfer it may already have created instead of
// creating a second one. Only a definite answer, or different form
// values, starts a new transfer with a new key.
let pending = null; // { intent, key }

function keyFor(req) {
  const intent = JSON.stringify(req);
  if (!pending || pending.intent !== intent) pending = { intent, key: newKey() };
  return pending.key;
}

byId('send-form').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const form = ev.currentTarget;
  const error = byId('form-error');
  error.textContent = '';
  const source = accounts.find((a) => a.id === form.source.value);
  if (!source) {
    error.textContent = 'Pick the account to send from.';
    return;
  }
  if (form.source.value === form.destination.value) {
    error.textContent = 'Pick two different accounts.';
    return;
  }
  let req;
  try {
    req = {
      source_account_id: source.id,
      destination_account_id: form.destination.value,
      amount_minor: toMinor(form.amount.value, source.currency),
      currency: source.currency,
    };
  } catch (e) {
    error.textContent = e.message;
    return;
  }
  const key = keyFor(req);
  const button = form.querySelector('button[type="submit"]');
  button.disabled = true;
  try {
    const { status, data } = await postWithRetry(req, key);
    pending = null; // a definite answer: the next submit is a new transfer
    if (status !== 201 && status !== 200) {
      error.textContent = `HTTP ${status}: ${data.message || 'request failed'}`;
      return;
    }
    addCard(req, key, data);
    refreshAccountsSoon();
  } catch {
    error.textContent = 'No answer from the server. Press "Send ponies" again: the request is ' +
      'repeated with the same Idempotency-Key, so it cannot create a second transfer.';
  } finally {
    button.disabled = false;
  }
});

// --- A transfer and its saga ------------------------------------------------

function chip(className, text) {
  const span = document.createElement('span');
  span.className = 'chip ' + className;
  span.textContent = text;
  return span;
}

function render(card, t) {
  const status = card.querySelector('[data-role="status"]');
  status.className = 'chip ' + t.status + (FINAL.has(t.status) ? '' : ' live');
  status.textContent = t.status;
  let mood = MOOD[t.status] || t.status;
  if (t.reason) mood += `: ${REASONS[t.reason] || t.reason.replaceAll('_', ' ')}`;
  card.querySelector('.mood').textContent = mood;

  const steps = card.querySelector('.steps');
  steps.replaceChildren(...t.steps.map((s) => {
    const li = document.createElement('li');
    const name = document.createElement('span');
    name.className = 'step-name';
    name.textContent = STEP_NAMES[s.step] || s.step;
    const live = s.state === 'running' || s.state === 'waiting';
    const c = chip(s.state + (live ? ' live' : ''), s.state);
    li.append(name, c);
    const detail = [s.outcome, s.attempts > 1 ? `${s.attempts} attempts` : ''].filter(Boolean).join(', ');
    if (detail) {
      const d = document.createElement('span');
      d.className = 'step-detail';
      d.textContent = detail;
      li.append(d);
    }
    return li;
  }));
}

// follow long-polls until the transfer is final. Each answer brings either
// a newer version (modified: true) or, after the soft timeout, the same one.
async function follow(card, id, version) {
  let failures = 0;
  for (;;) {
    let res;
    try {
      res = await fetch(`/v1/transfers/${id}/wait?version=${version}`);
    } catch {
      failures++;
      await sleep(Math.min(1000 * 2 ** failures, 15000));
      continue;
    }
    if (res.status === 429 || res.status === 503) {
      await sleep((Number(res.headers.get('Retry-After')) || 1) * 1000);
      continue;
    }
    if (!res.ok) {
      failures++;
      card.querySelector('.note').textContent = `Long poll answered ${res.status}, retrying…`;
      await sleep(Math.min(1000 * 2 ** failures, 15000));
      continue;
    }
    failures = 0;
    const t = await res.json();
    if (!t.modified) continue;
    version = t.version;
    render(card, t);
    refreshAccountsSoon();
    if (FINAL.has(t.status)) return;
  }
}

function addCard(req, key, created) {
  const card = byId('transfer-template').content.firstElementChild.cloneNode(true);
  card.querySelector('.amount').textContent = formatAmount(req.amount_minor, req.currency);
  card.querySelector('.meta').textContent =
    `${accountName(req.source_account_id)} → ${accountName(req.destination_account_id)} · transfer ${created.id}`;
  render(card, created);

  const note = card.querySelector('.note');
  card.querySelector('[data-action="replay"]').addEventListener('click', async () => {
    try {
      const { status, data } = await postTransfer(req, key);
      note.textContent = `Replay with the same Idempotency-Key: HTTP ${status}, ` +
        (data.id === created.id ? 'the same transfer, no second one.' : `transfer ${data.id}`);
    } catch (e) {
      note.textContent = `Replay failed: ${e.message}`;
    }
  });
  byId('transfers').prepend(card);
  follow(card, created.id, created.version);
}

// --- Notifications feed -----------------------------------------------------

async function pollFeed() {
  for (;;) {
    const account = byId('feed-account').value;
    if (account && !document.hidden) {
      try {
        const res = await fetch(`${notificationsURL}/v1/notifications?account=${account}&limit=20`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        renderFeed((await res.json()).notifications);
        byId('feed-error').textContent = '';
      } catch (e) {
        byId('feed-error').textContent = `Could not read notifications (${e.message}).`;
      }
    }
    await sleep(3000);
  }
}

function renderFeed(list) {
  const ol = byId('notifications');
  if (list.length === 0) {
    const li = document.createElement('li');
    li.textContent = 'No notifications for this account yet.';
    ol.replaceChildren(li);
    return;
  }
  ol.replaceChildren(...list.map((n) => {
    const li = document.createElement('li');
    const time = document.createElement('time');
    time.dateTime = n.created_at;
    time.textContent = new Date(n.created_at).toLocaleTimeString();
    const deliveries = document.createElement('div');
    deliveries.className = 'deliveries';
    deliveries.append(...n.deliveries.map((d) => chip(d.state, `${d.channel}: ${d.state}`)));
    li.append(time, document.createTextNode(n.message), deliveries);
    return li;
  }));
}

// --- Start ------------------------------------------------------------------

byId('refresh-accounts').addEventListener('click', loadAccounts);

(async function start() {
  try {
    const cfg = await (await fetch('/config.json')).json();
    notificationsURL = (cfg.notifications_url || '').replace(/\/$/, '');
  } catch {
    notificationsURL = '';
  }
  await loadAccounts();
  if (notificationsURL) {
    byId('feed').hidden = false;
    pollFeed();
  }
})();
