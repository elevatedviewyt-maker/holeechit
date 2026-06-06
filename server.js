const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const STROKES_FILE = path.join(__dirname, 'strokes.json');
const USERS_FILE = path.join(__dirname, 'users.json');
const CHAT_FILE = path.join(__dirname, 'chat.json');

// ── Load persisted data ───────────────────────────────────
let strokes = [];
let users = {};   // { name_lower: { name, btc, createdAt }, ... }
let chatAll = []; // full history, never deleted
const CHAT_BROADCAST_LIMIT = 100;

function loadJSON(file, fallback) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch(e) { console.error('Load error', file, e.message); }
  return fallback;
}
function saveJSON(file, data) {
  try { fs.writeFileSync(file, JSON.stringify(data)); } catch(e) { console.error('Save error', file, e.message); }
}

strokes = loadJSON(STROKES_FILE, []);
users   = loadJSON(USERS_FILE, {});
chatAll = loadJSON(CHAT_FILE, []);
console.log(`Loaded: ${strokes.length} strokes, ${Object.keys(users).length} users, ${chatAll.length} chat msgs`);

// ── HTTP server ───────────────────────────────────────────
const server = http.createServer((req, res) => {
  // Simple REST: GET /users and GET /chat for admin viewing
  if (req.url === '/users') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(users, null, 2));
  }
  if (req.url === '/chat') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(chatAll, null, 2));
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

  // Send initial state
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
      saveJSON(STROKES_FILE, strokes);
      for (const client of clients) {
        if (client !== ws && client.readyState === WebSocket.OPEN)
          client.send(JSON.stringify({ type: 'stroke', stroke: data.stroke }));
      }
    }

    if (data.type === 'clear') {
      strokes = [];
      saveJSON(STROKES_FILE, strokes);
      broadcast({ type: 'clear' });
    }

    // ── User registration ──
    if (data.type === 'register') {
      const name = (data.name || '').trim();
      const btc  = (data.btc  || '').trim();
      if (!name) return ws.send(JSON.stringify({ type: 'register_err', msg: 'Name required' }));

      const nameKey = name.toLowerCase();
      const btcKey  = btc.toLowerCase();

      // Check name uniqueness
      if (users[nameKey]) {
        return ws.send(JSON.stringify({ type: 'register_err', field: 'name', msg: 'Name already taken' }));
      }
      // Check BTC uniqueness (if provided)
      if (btc) {
        const btcTaken = Object.values(users).some(u => u.btc && u.btc.toLowerCase() === btcKey);
        if (btcTaken) {
          return ws.send(JSON.stringify({ type: 'register_err', field: 'btc', msg: 'Wallet already registered' }));
        }
      }

      users[nameKey] = { name, btc, createdAt: new Date().toISOString() };
      saveJSON(USERS_FILE, users);
      ws.send(JSON.stringify({ type: 'register_ok', name, btc }));
    }

    // ── User update (rename / change wallet) ──
    if (data.type === 'update_user') {
      const oldName = (data.oldName || '').trim();
      const newName = (data.newName || '').trim();
      const newBtc  = (data.newBtc  || '').trim();
      if (!oldName || !newName) return ws.send(JSON.stringify({ type: 'update_err', msg: 'Name required' }));

      const oldKey = oldName.toLowerCase();
      const newKey = newName.toLowerCase();

      // Name changed and new name taken by someone else
      if (newKey !== oldKey && users[newKey]) {
        return ws.send(JSON.stringify({ type: 'update_err', field: 'name', msg: 'Name already taken' }));
      }
      // BTC uniqueness check (ignore own current btc)
      if (newBtc) {
        const newBtcLow = newBtc.toLowerCase();
        const btcTaken = Object.values(users).some(u =>
          u.btc && u.btc.toLowerCase() === newBtcLow && u.name.toLowerCase() !== oldKey
        );
        if (btcTaken) {
          return ws.send(JSON.stringify({ type: 'update_err', field: 'btc', msg: 'Wallet already registered' }));
        }
      }

      // Delete old key, save new
      delete users[oldKey];
      users[newKey] = { name: newName, btc: newBtc, createdAt: users[oldKey]?.createdAt || new Date().toISOString(), updatedAt: new Date().toISOString() };
      saveJSON(USERS_FILE, users);
      ws.send(JSON.stringify({ type: 'update_ok', name: newName, btc: newBtc }));
    }

    // ── Chat ──
    if (data.type === 'chat') {
      const name = (data.name || '').trim().slice(0, 32);
      const text = (data.text || '').trim().slice(0, 300);
      if (!name || !text) return;

      const chatMsg = { name, text, ts: Date.now() };
      chatAll.push(chatMsg);
      saveJSON(CHAT_FILE, chatAll);

      // Broadcast last 100 to everyone
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

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
