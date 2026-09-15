const WebSocket = require('ws');
const http = require('http');
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('BrainRot WS Server');
});
const wss = new WebSocket.Server({ server });
const rooms = {};

wss.on('connection', (ws) => {
  let playerName, roomCode;

  // ===== HEARTBEAT =====
  // Without this, a socket that dies uncleanly (phone locks, wifi drops, tab
  // crashes — anything that doesn't send a proper close frame) can sit in
  // `clients` looking perfectly alive for minutes. That's what made host
  // migration / "player left" detection feel slow: the server didn't actually
  // know they were gone yet. `ws.ping()` every 30s + terminating anyone who
  // didn't pong back means dead connections get reaped quickly, so the
  // `close` handler (and its 'players' broadcast) fires promptly instead.
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if(msg.type === 'join'){
        playerName = msg.name;
        roomCode = msg.code;
        if(!rooms[roomCode]) rooms[roomCode] = { players: [], clients: new Set() };
        rooms[roomCode].clients.add(ws);
        if(!rooms[roomCode].players.includes(playerName)){
          rooms[roomCode].players.push(playerName);
        }
        broadcast(roomCode, { type: 'players', players: rooms[roomCode].players });
        return;
      }
      if(msg.type === 'chat'){
        broadcast(roomCode, { type: 'chat', username: msg.username, message: msg.message });
        return;
      }
      if(msg.type === 'quiz_start'){
        broadcast(roomCode, { type: 'quiz_start', quiz: msg.quiz });
        return;
      }
      if(msg.type === 'player_done'){
        broadcast(roomCode, { type: 'player_done', name: msg.name });
        return;
      }
      // ===== CO-OP BOSS BATTLE (and any future message types) =====
      // These are all just room-scoped broadcasts — the host client is
      // authoritative on game logic, this server just relays verbatim.
      // coop_boss_starting | coop_boss_start | coop_boss_state | coop_boss_answer | coop_boss_end
      broadcast(roomCode, msg);
    } catch(e) {
      console.error('Parse error:', e);
    }
  });
  ws.on('close', () => {
    if(!roomCode || !rooms[roomCode]) return;
    rooms[roomCode].clients.delete(ws);
    rooms[roomCode].players = rooms[roomCode].players.filter(p => p !== playerName);
    broadcast(roomCode, { type: 'players', players: rooms[roomCode].players });
    if(rooms[roomCode].clients.size === 0) delete rooms[roomCode];
  });
  ws.on('error', () => ws.close());
});

// Ping every client every 30s; anyone who missed the previous pong (i.e.
// never got marked alive again) gets terminated, which fires their 'close'
// handler above and lets the room move on without them.
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);
wss.on('close', () => clearInterval(heartbeatInterval));

function broadcast(code, msg){
  if(!rooms[code]) return;
  const data = JSON.stringify(msg);
  rooms[code].clients.forEach(client => {
    if(client.readyState === WebSocket.OPEN) client.send(data);
  });
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`WS running on port ${PORT}`));
