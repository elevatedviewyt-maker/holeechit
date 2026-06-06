const http = require('http');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI;
const ADMIN_PASS = process.env.ADMIN_PASS || 'changeme';

if (!MONGO_URI) { console.error('ERROR: MONGO_URI env var not set!'); process.exit(1); }

// ── In-memory cache (loaded from MongoDB at startup) ──────
let strokes = [];
let users = {};   // { name_lower: { name, btc, createdAt }, ... }
let chatAll = []; // full history
const CHAT_BROADCAST_LIMIT = 100;

let db, colStrokes, colUsers, colChat;

// ── MongoDB helpers ───────────────────────────────────────
async function saveStrokes() {
  await colStrokes.replaceOne({ _id: 'main' }, { _id: 'main', data: strokes }, { upsert: true });
}
async function saveUsers() {
  await colUsers.replaceOne({ _id: 'main' }, { _id: 'main', data: users }, { upsert: true });
}
async function saveChat() {
  await colChat.replaceOne({ _id: 'main' }, { _id: 'main', data: chatAll }, { upsert: true });
}

// ── Admin auth helper ─────────────────────────────────────
function checkAdmin(req, res) {
  const url = new URL(req.url, 'http://localhost');
  if (url.searchParams.get('pass') !== ADMIN_PASS) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Forbidden: wrong ?pass=' }));
    return false;
  }
  return true;
}

// ── HTTP server ───────────────────────────────────────────
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  // GET /users?pass=xxx
  if (req.method === 'GET' && pathname === '/users') {
    if (!checkAdmin(req, res)) return;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(users, null, 2));
  }

  // GET /chat?pass=xxx
  if (req.method === 'GET' && pathname === '/chat') {
    if (!checkAdmin(req, res)) return;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(chatAll, null, 2));
  }

  // DELETE /chat?pass=xxx — wipe all chat
  if (req.method === 'DELETE' && pathname === '/chat') {
    if (!checkAdmin(req, res)) return;
    chatAll = [];
    saveChat().catch(console.error);
    broadcast({ type: 'chat_init', messages: [] });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, msg: 'Chat cleared' }));
  }

  // DELETE /chat/:index?pass=xxx — delete single message
  if (req.method === 'DELETE' && pathname.startsWith('/chat/')) {
    if (!checkAdmin(req, res)) return;
    const idx = parseInt(pathname.split('/')[2], 10);
    if (isNaN(idx) || idx < 0 || idx >= chatAll.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Invalid index' }));
    }
    const removed = chatAll.splice(idx, 1);
    saveChat().catch(console.error);
    broadcast({ type: 'chat_init', messages: chatAll.slice(-CHAT_BROADCAST_LIMIT) });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, removed }));
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Ho Lee Chit WS Server running!');
});

// ── WebSocket ─────────────────────────────────────────────
const wss = new WebSocket.Server({ server });
let clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`Client connected. Total: ${clients.size}`);

  ws.send(JSON.stringify({ type: 'init', strokes }));
  ws.send(JSON.stringify({ type: 'chat_init', messages: chatAll.slice(-CHAT_BROADCAST_LIMIT) }));
  broadcast({ type: 'presence', count: clients.size });

  ws.on('message', (msg) => {
    let data;
    try { data = JSON.parse(msg); } catch { return; }

    // ── Drawing ──
    if (data.type === 'stroke') {
      strokes.push(data.stroke);
      if (strokes.length > 5000) strokes = strokes.slice(-5000);
      saveStrokes().catch(console.error);
      for (const client of clients) {
        if (client !== ws && client.readyState === WebSocket.OPEN)
          client.send(JSON.stringify({ type: 'stroke', stroke: data.stroke }));
      }
    }

    if (data.type === 'clear') {
      strokes = [];
      saveStrokes().catch(console.error);
      broadcast({ type: 'clear' });
    }

    // ── Register ──
    if (data.type === 'register') {
      const name = (data.name || '').trim();
      const btc  = (data.btc  || '').trim();
      if (!name) return ws.send(JSON.stringify({ type: 'register_err', msg: 'Name required' }));

      const nameKey = name.toLowerCase();
      const btcKey  = btc.toLowerCase();

      if (users[nameKey])
        return ws.send(JSON.stringify({ type: 'register_err', field: 'name', msg: 'Name already taken' }));

      if (btc) {
        const btcTaken = Object.values(users).some(u => u.btc && u.btc.toLowerCase() === btcKey);
        if (btcTaken)
          return ws.send(JSON.stringify({ type: 'register_err', field: 'btc', msg: 'Wallet already registered' }));
      }

      users[nameKey] = { name, btc, createdAt: new Date().toISOString() };
      saveUsers().catch(console.error);
      ws.send(JSON.stringify({ type: 'register_ok', name, btc }));
    }

    // ── Update user ──
    if (data.type === 'update_user') {
      const oldName = (data.oldName || '').trim();
      const newName = (data.newName || '').trim();
      const newBtc  = (data.newBtc  || '').trim();
      if (!oldName || !newName)
        return ws.send(JSON.stringify({ type: 'update_err', msg: 'Name required' }));

      const oldKey = oldName.toLowerCase();
      const newKey = newName.toLowerCase();

      if (newKey !== oldKey && users[newKey])
        return ws.send(JSON.stringify({ type: 'update_err', field: 'name', msg: 'Name already taken' }));

      if (newBtc) {
        const newBtcLow = newBtc.toLowerCase();
        const btcTaken = Object.values(users).some(u =>
          u.btc && u.btc.toLowerCase() === newBtcLow && u.name.toLowerCase() !== oldKey
        );
        if (btcTaken)
          return ws.send(JSON.stringify({ type: 'update_err', field: 'btc', msg: 'Wallet already registered' }));
      }

      const oldCreatedAt = users[oldKey]?.createdAt;
      delete users[oldKey];
      users[newKey] = { name: newName, btc: newBtc, createdAt: oldCreatedAt || new Date().toISOString(), updatedAt: new Date().toISOString() };
      saveUsers().catch(console.error);
      ws.send(JSON.stringify({ type: 'update_ok', name: newName, btc: newBtc }));
    }

    // ── Chat ──
    if (data.type === 'chat') {
      const name = (data.name || '').trim().slice(0, 32);
      const text = (data.text || '').trim().slice(0, 300);
      if (!name || !text) return;

      const chatMsg = { name, text, ts: Date.now() };
      chatAll.push(chatMsg);
      saveChat().catch(console.error);
      broadcast({ type: 'chat', msg: chatMsg });
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`Client left. Total: ${clients.size}`);
    broadcast({ type: 'presence', count: clients.size });
  });

  ws.on('error', () => clients.delete(ws));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  }
}

// ── Connect to MongoDB then start server ──────────────────
async function start() {
  console.log('Connecting to MongoDB...');
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db('holeechit');
  colStrokes = db.collection('strokes');
  colUsers   = db.collection('users');
  colChat    = db.collection('chat');
  console.log('MongoDB connected!');

  // Load data into memory
  const sDoc = await colStrokes.findOne({ _id: 'main' });
  const uDoc = await colUsers.findOne({ _id: 'main' });
  const cDoc = await colChat.findOne({ _id: 'main' });
  strokes = sDoc?.data || [];
  users   = uDoc?.data || {};
  chatAll = cDoc?.data || [];
  console.log(`Loaded: ${strokes.length} strokes, ${Object.keys(users).length} users, ${chatAll.length} chat msgs`);

  server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}

start().catch(err => { console.error('Startup error:', err); process.exit(1); });
