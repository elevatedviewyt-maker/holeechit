const http = require('http');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');
const { AccessToken } = require('livekit-server-sdk');

const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI;
const ADMIN_PASS = process.env.ADMIN_PASS || 'changeme';
const LK_API_KEY    = process.env.LK_API_KEY    || 'APIqdhW7kJA7gLm';
const LK_API_SECRET = process.env.LK_API_SECRET || 'bEjEiD3reUG4Dkm7eswkcAPnUMnqfJpThDQo0AhhxClA';
const LK_ROOM       = 'holeechit-main';
let RADIO_YT_ID   = process.env.RADIO_YT_ID   || 'X4VbdwhkE10';
let RADIO_NETE_ID = process.env.RADIO_NETE_ID || '';

if (!MONGO_URI) { console.error('ERROR: MONGO_URI env var not set!'); process.exit(1); }
if (!LK_API_SECRET) { console.warn('WARNING: LK_API_SECRET not set — voice chat will not work'); }

// ── In-memory cache (loaded from MongoDB at startup) ──────
let strokes = [];
let users = {};   // { name_lower: { name, btc, createdAt, blocked? }, ... }
let chatAll = []; // full history
let blockedUsers = new Set(); // set of name_lower strings
let activeClients = new Map(); // ws -> { name }
const CHAT_BROADCAST_LIMIT = 100;
const CHAT_MAX_STORED = 500;  // trim chat to last 500 messages in DB

let db, colStrokes, colUsers, colChat, colQueue;

// ── Rate-limit: clear board ───────────────────────────────
// Max 1 clear every 30 seconds globally
let lastClearAt = 0;
const CLEAR_COOLDOWN_MS = 30_000;

// ── Music queues (YouTube + NetEase) ─────────────────────
let ytQueue   = []; // [{ videoId, title, addedBy }]
let ytCurrent = null; // null = radio
let neteQueue   = []; // [{ songId, title, addedBy }]
let neteCurrent = null; // null = radio

// ── MongoDB helpers ───────────────────────────────────────
async function saveStrokes() {
  await colStrokes.replaceOne({ _id: 'main' }, { _id: 'main', data: strokes }, { upsert: true });
}
async function saveUsers() {
  await colUsers.replaceOne({ _id: 'main' }, { _id: 'main', data: users }, { upsert: true });
}
async function saveChat() {
  if (chatAll.length > CHAT_MAX_STORED) chatAll = chatAll.slice(-CHAT_MAX_STORED);
  await colChat.replaceOne({ _id: 'main' }, { _id: 'main', data: chatAll }, { upsert: true });
}
async function saveQueue() {
  await colQueue.replaceOne({ _id: "main" }, { _id: "main", ytQueue, ytCurrent, neteQueue, neteCurrent }, { upsert: true });
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
const server = http.createServer(async (req, res) => {
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


  // ── Admin queue endpoints ─────────────────────────────────

  // GET /admin/queue?pass=xxx
  if (req.method === 'GET' && pathname === '/admin/queue') {
    if (!checkAdmin(req, res)) return;
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ytQueue, ytCurrent, neteQueue, neteCurrent }));
  }

  // DELETE /admin/queue/yt/:index?pass=xxx
  if (req.method === 'DELETE' && pathname.startsWith('/admin/queue/yt/')) {
    if (!checkAdmin(req, res)) return;
    const idx = parseInt(pathname.split('/')[4], 10);
    if (isNaN(idx) || idx < 0 || idx >= ytQueue.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Invalid index' }));
    }
    ytQueue.splice(idx, 1);
    if (idx === 0) ytCurrent = ytQueue.length > 0 ? ytQueue[0].videoId : null;
    saveQueue().catch(console.error);
    broadcastQueueUpdate();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true }));
  }

  // DELETE /admin/queue/nete/:index?pass=xxx
  if (req.method === 'DELETE' && pathname.startsWith('/admin/queue/nete/')) {
    if (!checkAdmin(req, res)) return;
    const idx = parseInt(pathname.split('/')[4], 10);
    if (isNaN(idx) || idx < 0 || idx >= neteQueue.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Invalid index' }));
    }
    neteQueue.splice(idx, 1);
    if (idx === 0) neteCurrent = neteQueue.length > 0 ? neteQueue[0].songId : null;
    saveQueue().catch(console.error);
    broadcastQueueUpdate();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true }));
  }

  // POST /admin/queue/yt/skip?pass=xxx
  if (req.method === 'POST' && pathname === '/admin/queue/yt/skip') {
    if (!checkAdmin(req, res)) return;
    if (ytQueue.length > 0) ytQueue.shift();
    ytCurrent = ytQueue.length > 0 ? ytQueue[0].videoId : null;
    saveQueue().catch(console.error);
    broadcastQueueUpdate();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, ytCurrent }));
  }

  // POST /admin/queue/nete/skip?pass=xxx
  if (req.method === 'POST' && pathname === '/admin/queue/nete/skip') {
    if (!checkAdmin(req, res)) return;
    if (neteQueue.length > 0) neteQueue.shift();
    neteCurrent = neteQueue.length > 0 ? neteQueue[0].songId : null;
    saveQueue().catch(console.error);
    broadcastQueueUpdate();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, neteCurrent }));
  }

  // DELETE /admin/queue/yt?pass=xxx — clear entire YT queue
  if (req.method === 'DELETE' && pathname === '/admin/queue/yt') {
    if (!checkAdmin(req, res)) return;
    ytQueue = []; ytCurrent = null;
    saveQueue().catch(console.error);
    broadcastQueueUpdate();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, msg: 'YT queue cleared' }));
  }

  // DELETE /admin/queue/nete?pass=xxx — clear entire NetEase queue
  if (req.method === 'DELETE' && pathname === '/admin/queue/nete') {
    if (!checkAdmin(req, res)) return;
    neteQueue = []; neteCurrent = null;
    saveQueue().catch(console.error);
    broadcastQueueUpdate();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, msg: 'NetEase queue cleared' }));
  }

  // POST /admin/queue/yt/add?pass=xxx  body:{videoId,title}
  if (req.method === 'POST' && pathname === '/admin/queue/yt/add') {
    if (!checkAdmin(req, res)) return;
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { videoId, title } = JSON.parse(body);
        if (!videoId) { res.writeHead(400); return res.end(JSON.stringify({ error: 'videoId required' })); }
        ytQueue.push({ videoId, title: title || videoId, addedBy: 'Admin' });
        if (!ytCurrent) ytCurrent = videoId;
        saveQueue().catch(console.error);
        broadcastQueueUpdate();
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid JSON' })); }
    });
    return;
  }

  // POST /admin/queue/nete/add?pass=xxx  body:{songId,title}
  if (req.method === 'POST' && pathname === '/admin/queue/nete/add') {
    if (!checkAdmin(req, res)) return;
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { songId, title } = JSON.parse(body);
        if (!songId) { res.writeHead(400); return res.end(JSON.stringify({ error: 'songId required' })); }
        neteQueue.push({ songId, title: title || songId, addedBy: 'Admin' });
        if (!neteCurrent) neteCurrent = songId;
        saveQueue().catch(console.error);
        broadcastQueueUpdate();
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid JSON' })); }
    });
    return;
  }

  // GET /admin/data?pass=xxx — all users + chat
  if (req.method === 'GET' && pathname === '/admin/data') {
    if (!checkAdmin(req, res)) return;
    const activeNames = new Set([...activeClients.values()].map(c => c.name?.toLowerCase()).filter(Boolean));
    const usersWithStatus = Object.values(users).map(u => ({
      ...u,
      active: activeNames.has(u.name.toLowerCase()),
      blocked: blockedUsers.has(u.name.toLowerCase())
    }));
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ users: usersWithStatus, chat: chatAll, chatCount: chatAll.length }));
  }

  // DELETE /admin/chat/:index?pass=xxx
  if (req.method === 'DELETE' && pathname.startsWith('/admin/chat/')) {
    if (!checkAdmin(req, res)) return;
    const idx = parseInt(pathname.split('/')[3], 10);
    if (isNaN(idx) || idx < 0 || idx >= chatAll.length) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Invalid index' }));
    }
    chatAll.splice(idx, 1);
    saveChat().catch(console.error);
    broadcast({ type: 'chat_init', messages: chatAll.slice(-CHAT_BROADCAST_LIMIT) });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true }));
  }

  // DELETE /admin/chat?pass=xxx — wipe all chat
  if (req.method === 'DELETE' && pathname === '/admin/chat') {
    if (!checkAdmin(req, res)) return;
    chatAll = [];
    saveChat().catch(console.error);
    broadcast({ type: 'chat_init', messages: [] });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, msg: 'Chat cleared' }));
  }

  // POST /admin/block?pass=xxx  body:{name, blocked:true/false}
  if (req.method === 'POST' && pathname === '/admin/block') {
    if (!checkAdmin(req, res)) return;
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { name, blocked } = JSON.parse(body);
        const key = (name || '').toLowerCase();
        if (!key) { res.writeHead(400); return res.end(JSON.stringify({ error: 'name required' })); }
        if (blocked) {
          blockedUsers.add(key);
          if (users[key]) users[key].blocked = true;
          // Kick blocked user from WS
          for (const [client, info] of activeClients) {
            if (info.name?.toLowerCase() === key) {
              client.send(JSON.stringify({ type: 'kicked', reason: 'You have been blocked by admin.' }));
              client.close();
            }
          }
        } else {
          blockedUsers.delete(key);
          if (users[key]) users[key].blocked = false;
        }
        saveUsers().catch(console.error);
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true, name, blocked }));
      } catch(e) { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid JSON' })); }
    });
    return;
  }

  // POST /admin/voice-kick?pass=xxx  body:{name}
  if (req.method === 'POST' && pathname === '/admin/voice-kick') {
    if (!checkAdmin(req, res)) return;
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { name } = JSON.parse(body);
        // Notify the client to leave voice
        for (const [client, info] of activeClients) {
          if (info.name?.toLowerCase() === (name||'').toLowerCase()) {
            client.send(JSON.stringify({ type: 'voice_kick', reason: 'Removed from voice by admin.' }));
          }
        }
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid JSON' })); }
    });
    return;
  }

  // POST /admin/radio?pass=xxx  body:{type:'yt'|'nete', id}
  if (req.method === 'POST' && pathname === '/admin/radio') {
    if (!checkAdmin(req, res)) return;
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { type, id } = JSON.parse(body);
        if (type === 'yt') RADIO_YT_ID = id;
        else if (type === 'nete') RADIO_NETE_ID = id;
        // Broadcast new radio to all clients
        broadcast({ type: 'radio_update', ytRadioId: RADIO_YT_ID, neteRadioId: RADIO_NETE_ID });
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true, RADIO_YT_ID, RADIO_NETE_ID }));
      } catch(e) { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid JSON' })); }
    });
    return;
  }

  // OPTIONS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,DELETE', 'Access-Control-Allow-Headers': 'Content-Type' });
    return res.end();
  }

  // GET /livekit-token?name=USERNAME — issue a Livekit JWT for the caller
  if (req.method === 'GET' && pathname === '/livekit-token') {
    const name = (url.searchParams.get('name') || '').trim().slice(0, 32);
    if (!name) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'name required' }));
    }
    if (!LK_API_SECRET) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Voice chat not configured on server' }));
    }
    try {
      const at = new AccessToken(LK_API_KEY, LK_API_SECRET, {
        identity: name,
        ttl: '4h',
      });
      at.addGrant({ roomJoin: true, room: LK_ROOM, canPublish: true, canSubscribe: true });
      const token = await at.toJwt();
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      });
      return res.end(JSON.stringify({ token, room: LK_ROOM }));
    } catch (err) {
      console.error('Livekit token error:', err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Token generation failed' }));
    }
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Ho Lee Chit WS Server running!');
});

// ── WebSocket ─────────────────────────────────────────────
const wss = new WebSocket.Server({ server });
let clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  activeClients.set(ws, {});
  console.log(`Client connected. Total: ${clients.size}`);

  ws.send(JSON.stringify({ type: 'init', strokes }));
  ws.send(JSON.stringify({ type: 'chat_init', messages: chatAll.slice(-CHAT_BROADCAST_LIMIT) }));
  ws.send(JSON.stringify({ type: 'queue_update', ytQueue, ytCurrent, neteQueue, neteCurrent }));
  ws.send(JSON.stringify({ type: 'radio_update', ytRadioId: RADIO_YT_ID, neteRadioId: RADIO_NETE_ID }));
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
      const now = Date.now();
      if (now - lastClearAt < CLEAR_COOLDOWN_MS) {
        return ws.send(JSON.stringify({ type: 'clear_err', msg: 'Clear on cooldown, try again in a moment.' }));
      }
      lastClearAt = now;
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

      // Check if blocked
      if (blockedUsers.has(nameKey)) {
        return ws.send(JSON.stringify({ type: 'register_err', field: 'name', msg: 'This name is blocked.' }));
      }
      users[nameKey] = { name, btc, createdAt: new Date().toISOString() };
      saveUsers().catch(console.error);
      activeClients.set(ws, { name });
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
      activeClients.set(ws, { name: newName });
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

    // -- YouTube queue add --
    if (data.type === 'queue_add') {
      const videoId = (data.videoId || '').trim().slice(0, 20);
      const title   = (data.title   || videoId).trim().slice(0, 100);
      const addedBy = (data.addedBy || '').trim().slice(0, 32);
      if (!videoId || !addedBy) return;
      const alreadyIn = ytQueue.some(q => q.addedBy.toLowerCase() === addedBy.toLowerCase());
      if (alreadyIn) return ws.send(JSON.stringify({ type: 'queue_err', source: 'yt', msg: 'You already have a song in the YouTube queue' }));
      ytQueue.push({ videoId, title, addedBy });
      if (!ytCurrent) ytCurrent = videoId;
      saveQueue().catch(console.error);
      broadcastQueueUpdate();
    }

    // -- NetEase queue add --
    if (data.type === 'nete_queue_add') {
      const songId  = (data.songId  || '').trim().slice(0, 20);
      const title   = (data.title   || songId).trim().slice(0, 100);
      const addedBy = (data.addedBy || '').trim().slice(0, 32);
      if (!songId || !addedBy) return;
      const alreadyIn = neteQueue.some(q => q.addedBy.toLowerCase() === addedBy.toLowerCase());
      if (alreadyIn) return ws.send(JSON.stringify({ type: 'queue_err', source: 'nete', msg: 'You already have a song in the NetEase queue' }));
      neteQueue.push({ songId, title, addedBy });
      if (!neteCurrent) neteCurrent = songId;
      saveQueue().catch(console.error);
      broadcastQueueUpdate();
    }

    // -- YouTube song ended --
    if (data.type === 'song_ended') {
      const endedId = (data.videoId || '').trim();
      if (endedId && endedId === ytCurrent) {
        if (ytQueue.length > 0 && ytQueue[0].videoId === endedId) ytQueue.shift();
        ytCurrent = ytQueue.length > 0 ? ytQueue[0].videoId : null;
        saveQueue().catch(console.error);
        broadcastQueueUpdate();
      }
    }

    // -- NetEase song ended --
    if (data.type === 'nete_ended') {
      const endedId = (data.songId || '').trim();
      if (endedId && endedId === neteCurrent) {
        if (neteQueue.length > 0 && neteQueue[0].songId === endedId) neteQueue.shift();
        neteCurrent = neteQueue.length > 0 ? neteQueue[0].songId : null;
        saveQueue().catch(console.error);
        broadcastQueueUpdate();
      }
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    activeClients.delete(ws);
    console.log(`Client left. Total: ${clients.size}`);
    broadcast({ type: 'presence', count: clients.size });
  });

  ws.on('error', () => { clients.delete(ws); activeClients.delete(ws); });
});

function broadcastQueueUpdate() {
  broadcast({ type: 'queue_update', ytQueue, ytCurrent, neteQueue, neteCurrent });
}

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
  colQueue   = db.collection('queue');
  console.log('MongoDB connected!');

  // Load data into memory
  const sDoc = await colStrokes.findOne({ _id: 'main' });
  const uDoc = await colUsers.findOne({ _id: 'main' });
  const cDoc = await colChat.findOne({ _id: 'main' });
  const qDoc = await colQueue.findOne({ _id: 'main' });
  strokes       = sDoc?.data || [];
  users         = uDoc?.data || {};
  // Rebuild blocked set from users
  blockedUsers = new Set(Object.entries(users).filter(([k,v]) => v.blocked).map(([k]) => k));
  chatAll       = cDoc?.data || [];
  ytQueue       = qDoc?.ytQueue   || [];
  ytCurrent     = qDoc?.ytCurrent || null;
  neteQueue     = qDoc?.neteQueue   || [];
  neteCurrent   = qDoc?.neteCurrent || null;
  console.log(`Loaded: ${strokes.length} strokes, ${Object.keys(users).length} users, ${chatAll.length} chat msgs, YT:${ytQueue.length} NetEase:${neteQueue.length} queue items`);

  server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}

start().catch(err => { console.error('Startup error:', err); process.exit(1); });
