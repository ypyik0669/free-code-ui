const express = require('express');
const { WebSocketServer } = require('ws');
const { spawn, execSync } = require('child_process');
const http = require('http');
const path = require('path');
const os = require('os');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ── CONFIG STORAGE ──
const CONFIG_DIR = path.join(os.homedir(), '.free-code-ui');
const ACCOUNTS_FILE = path.join(CONFIG_DIR, 'accounts.json');

function ensureConfigDir() {
  if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });
}

function loadAccounts() {
  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) return [];
    return JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
  } catch { return []; }
}

function saveAccounts(accounts) {
  ensureConfigDir();
  fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2));
}

// ── ACCOUNT REST API ──

// GET all accounts
app.get('/api/accounts', (req, res) => {
  res.json(loadAccounts());
});

// POST create account
app.post('/api/accounts', (req, res) => {
  const accounts = loadAccounts();
  const account = {
    id: crypto.randomUUID(),
    name: req.body.name || 'New Account',
    provider: req.body.provider || 'custom',
    authType: req.body.authType || 'apikey',
    apiKey: req.body.apiKey || '',
    baseUrl: req.body.baseUrl || '',
    models: req.body.models || [],
    active: accounts.length === 0, // first account is active by default
    usage: req.body.usage || {},
    createdAt: Date.now()
  };
  accounts.push(account);
  saveAccounts(accounts);
  res.json(account);
});

// PUT update account
app.put('/api/accounts/:id', (req, res) => {
  const accounts = loadAccounts();
  const idx = accounts.findIndex(a => a.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'not found' });
  accounts[idx] = { ...accounts[idx], ...req.body, id: req.params.id };
  saveAccounts(accounts);
  res.json(accounts[idx]);
});

// DELETE account
app.delete('/api/accounts/:id', (req, res) => {
  let accounts = loadAccounts();
  accounts = accounts.filter(a => a.id !== req.params.id);
  saveAccounts(accounts);
  res.json({ ok: true });
});

// POST set active account
app.post('/api/accounts/:id/activate', (req, res) => {
  const accounts = loadAccounts();
  accounts.forEach(a => a.active = (a.id === req.params.id));
  saveAccounts(accounts);
  res.json({ ok: true });
});

// POST update usage for an account
app.post('/api/accounts/:id/usage', (req, res) => {
  const accounts = loadAccounts();
  const idx = accounts.findIndex(a => a.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'not found' });
  accounts[idx].usage = { ...accounts[idx].usage, ...req.body, updatedAt: Date.now() };
  saveAccounts(accounts);
  res.json(accounts[idx]);
});

// ── ENV BUILDER ──
function buildEnvForAccount(account) {
  if (!account) return { ...process.env };
  const env = { ...process.env };

  switch (account.provider) {
    case 'anthropic':
      if (account.apiKey) env.ANTHROPIC_API_KEY = account.apiKey;
      if (account.baseUrl) env.ANTHROPIC_BASE_URL = account.baseUrl;
      break;
    case 'codex':
      if (account.apiKey) env.OPENAI_API_KEY = account.apiKey;
      // Codex OAuth uses stored credentials - free-code handles it
      break;
    case 'antigravity':
      if (account.apiKey) env.ANTHROPIC_API_KEY = account.apiKey;
      if (account.baseUrl) env.ANTHROPIC_BASE_URL = account.baseUrl;
      break;
    case 'gemini':
      if (account.apiKey) env.GEMINI_API_KEY = account.apiKey;
      if (account.baseUrl) env.ANTHROPIC_BASE_URL = account.baseUrl;
      break;
    case 'bedrock':
      env.CLAUDE_CODE_USE_BEDROCK = '1';
      if (account.awsRegion) env.AWS_REGION = account.awsRegion;
      break;
    case 'custom':
      if (account.apiKey) env.ANTHROPIC_API_KEY = account.apiKey;
      if (account.baseUrl) env.ANTHROPIC_BASE_URL = account.baseUrl;
      break;
  }
  return env;
}

// ── QUOTA ERROR DETECTION ──
const QUOTA_PATTERNS = [
  /quota/i, /rate.?limit/i, /429/,
  /insufficient/i, /exceeded/i,
  /billing/i, /overloaded/i,
  /capacity/i, /limit.?reached/i,
  /api.?error.*400/i
];

function isQuotaError(text) {
  return QUOTA_PATTERNS.some(p => p.test(text));
}

// ── AUTO ROTATION ──
function findFallbackAccount(currentId, model) {
  const accounts = loadAccounts();
  // Find accounts that support the same model, excluding current
  const candidates = accounts.filter(a =>
    a.id !== currentId &&
    (a.models.length === 0 || a.models.some(m => m.toLowerCase().includes(model?.toLowerCase() || '')))
  );
  return candidates[0] || null;
}

// ── WEBSOCKET HANDLER ──
const clientState = new Map();

wss.on('connection', (ws) => {
  const state = {
    activeProcess: null,
    sessionId: null,
    activeAccountId: null,
    currentModel: null
  };
  clientState.set(ws, state);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    // ── CHAT ──
    if (msg.type === 'chat') {
      if (state.activeProcess) {
        state.activeProcess.kill('SIGINT');
        state.activeProcess = null;
      }
      runChat(ws, state, msg, false);
    }

    // ── COMMAND ──
    if (msg.type === 'command') {
      runCommand(ws, state, msg);
    }

    // ── OAUTH LOGIN ──
    if (msg.type === 'oauth_login') {
      runOAuthLogin(ws, msg.provider, msg.accountId);
    }

    // ── INTERRUPT ──
    if (msg.type === 'interrupt') {
      if (state.activeProcess) {
        state.activeProcess.kill('SIGINT');
        ws.send(JSON.stringify({ type: 'interrupted' }));
      }
    }

    // ── CLEAR SESSION ──
    if (msg.type === 'clear_session') {
      state.sessionId = null;
      ws.send(JSON.stringify({ type: 'session_cleared' }));
    }

    // ── SET ACTIVE ACCOUNT ──
    if (msg.type === 'set_account') {
      state.activeAccountId = msg.accountId;
      ws.send(JSON.stringify({ type: 'account_set', accountId: msg.accountId }));
    }
  });

  ws.on('close', () => {
    if (state.activeProcess) state.activeProcess.kill();
    clientState.delete(ws);
  });
});

function runChat(ws, state, msg, isRetry) {
  const accounts = loadAccounts();
  const accountId = state.activeAccountId || msg.accountId;
  const account = accountId
    ? accounts.find(a => a.id === accountId)
    : accounts.find(a => a.active) || accounts[0];

  state.currentModel = msg.model;

  const args = [];
  if (msg.model) args.push('--model', msg.model);
  if (msg.sessionId || state.sessionId) args.push('--resume', msg.sessionId || state.sessionId);
  args.push('-p', msg.prompt);

  const workdir = msg.workdir || os.homedir();
  const env = buildEnvForAccount(account);

  if (!isRetry) {
    ws.send(JSON.stringify({
      type: 'start',
      accountName: account?.name,
      accountId: account?.id,
      provider: account?.provider
    }));
  } else {
    ws.send(JSON.stringify({
      type: 'rotating',
      accountName: account?.name,
      accountId: account?.id
    }));
  }

  const proc = spawn('free-code', args, {
    env,
    cwd: workdir,
    shell: false,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  state.activeProcess = proc;
  let outputBuffer = '';
  let quotaHit = false;

  proc.stdout.on('data', (chunk) => {
    const text = chunk.toString();
    outputBuffer += text;
    if (isQuotaError(text) && !quotaHit) {
      quotaHit = true;
      // Mark usage as exhausted
      if (account) {
        const accounts2 = loadAccounts();
        const idx = accounts2.findIndex(a => a.id === account.id);
        if (idx !== -1) {
          accounts2[idx].usage = { ...accounts2[idx].usage, exhausted: true, exhaustedAt: Date.now() };
          saveAccounts(accounts2);
        }
      }
    }
    ws.send(JSON.stringify({ type: 'chunk', text }));
  });

  proc.stderr.on('data', (chunk) => {
    const text = chunk.toString();
    const match = text.match(/--resume\s+([a-f0-9-]{36})/);
    if (match) {
      state.sessionId = match[1];
      ws.send(JSON.stringify({ type: 'session_id', id: match[1] }));
    }
    const skip = ['ExperimentalWarning', 'DeprecationWarning'];
    if (!skip.some(s => text.includes(s))) {
      if (isQuotaError(text)) quotaHit = true;
      ws.send(JSON.stringify({ type: 'chunk', text }));
    }
  });

  proc.on('close', (code) => {
    const sessionMatch = outputBuffer.match(/--resume\s+([a-f0-9-]{36})/);
    if (sessionMatch && !state.sessionId) state.sessionId = sessionMatch[1];

    // Auto-rotate if quota hit
    if (quotaHit && account) {
      const fallback = findFallbackAccount(account.id, msg.model);
      if (fallback) {
        ws.send(JSON.stringify({
          type: 'auto_rotate',
          from: account.name,
          to: fallback.name,
          reason: 'quota_exceeded'
        }));
        state.activeAccountId = fallback.id;
        runChat(ws, state, msg, true);
        return;
      }
    }

    ws.send(JSON.stringify({
      type: 'done',
      exitCode: code,
      sessionId: state.sessionId,
      accountId: account?.id
    }));
    state.activeProcess = null;
  });

  proc.on('error', (err) => {
    ws.send(JSON.stringify({ type: 'error', text: `Failed to start free-code: ${err.message}` }));
    state.activeProcess = null;
  });
}

function runCommand(ws, state, msg) {
  const accounts = loadAccounts();
  const account = state.activeAccountId
    ? accounts.find(a => a.id === state.activeAccountId)
    : accounts.find(a => a.active) || accounts[0];

  const args = ['-p', msg.command];
  if (state.sessionId) args.push('--resume', state.sessionId);
  if (msg.model) args.push('--model', msg.model);

  const workdir = msg.workdir || os.homedir();
  const env = buildEnvForAccount(account);

  ws.send(JSON.stringify({ type: 'start', isCommand: true, command: msg.command }));

  const proc = spawn('free-code', args, {
    env, cwd: workdir,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  proc.stdout.on('data', c => ws.send(JSON.stringify({ type: 'chunk', text: c.toString() })));
  proc.stderr.on('data', (chunk) => {
    const text = chunk.toString();
    const match = text.match(/--resume\s+([a-f0-9-]{36})/);
    if (match) { state.sessionId = match[1]; ws.send(JSON.stringify({ type: 'session_id', id: match[1] })); }
  });
  proc.on('close', code => ws.send(JSON.stringify({ type: 'done', exitCode: code, sessionId: state.sessionId })));
  proc.on('error', err => ws.send(JSON.stringify({ type: 'error', text: err.message })));
}

function runOAuthLogin(ws, provider, accountId) {
  ws.send(JSON.stringify({ type: 'oauth_start', provider }));

  // Spawn free-code /login — it will output a URL or open browser
  const proc = spawn('free-code', ['/login'], {
    env: { ...process.env },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  proc.stdout.on('data', (chunk) => {
    const text = chunk.toString();
    // Look for auth URLs
    const urlMatch = text.match(/https?:\/\/[^\s]+/g);
    ws.send(JSON.stringify({
      type: 'oauth_output',
      text,
      urls: urlMatch || []
    }));
  });

  proc.stderr.on('data', (chunk) => {
    const text = chunk.toString();
    const urlMatch = text.match(/https?:\/\/[^\s]+/g);
    ws.send(JSON.stringify({
      type: 'oauth_output',
      text,
      urls: urlMatch || []
    }));
  });

  proc.on('close', (code) => {
    ws.send(JSON.stringify({
      type: 'oauth_done',
      exitCode: code,
      provider,
      accountId,
      success: code === 0
    }));
  });

  proc.on('error', err => {
    ws.send(JSON.stringify({ type: 'oauth_error', text: err.message }));
  });
}

// ── SESSIONS API ──
app.get('/api/sessions', (req, res) => {
  const claudeDir = path.join(os.homedir(), '.claude', 'projects');
  try {
    if (!fs.existsSync(claudeDir)) return res.json([]);
    const dirs = fs.readdirSync(claudeDir).slice(-10).reverse();
    res.json(dirs.map(d => ({ id: d, name: d.slice(0, 8) + '...' })));
  } catch { res.json([]); }
});

const PORT = process.env.PORT || 3333;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n╔════════════════════════════════════════╗`);
  console.log(`║  free-code UI v2  → http://localhost:${PORT}  ║`);
  console.log(`╚════════════════════════════════════════╝\n`);
});
