const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'strokes.json');

// Load saved strokes from disk
let strokes = [];
try {
  if (fs.existsSync(DATA_FILE)) {
    strokes = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    console.log(`Loaded ${strokes.length} strokes from disk`);
  }
} catch (e) {
  console.log('No saved strokes found, starting fresh');
  strokes = [];
}

function saveToDisk() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(strokes));
  } catch (e) {
    console.error('Save error:', e);
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Ho Lee Chit WS Server running!');
});

const wss = new WebSocket.Server({ server });

let clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`Client connected. Total: ${clients.size}`);

  // Send all existing strokes to new client
  ws.send(JSON.stringify({ type: 'init', strokes }));

  // Broadcast online count to everyone
  broadcast({ type: 'presence', count: clients.size });

  ws.on('message', (msg) => {
    let data;
    try { data = JSON.parse(msg); } catch { return; }

    if (data.type === 'stroke') {
      strokes.push(data.stroke);
      // Keep max 5000 strokes to avoid memory bloat
      if (strokes.length > 5000) strokes = strokes.slice(-5000);
      saveToDisk();
      // Broadcast to all OTHER clients
      for (const client of clients) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify({ type: 'stroke', stroke: data.stroke }));
        }
      }
    }

    if (data.type === 'clear') {
      strokes = [];
      saveToDisk();
      broadcast({ type: 'clear' });
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
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
