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
      // coop_boss_start | coop_boss_state | coop_boss_answer | coop_boss_end
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

function broadcast(code, msg){
  if(!rooms[code]) return;
  const data = JSON.stringify(msg);
  rooms[code].clients.forEach(client => {
    if(client.readyState === WebSocket.OPEN) client.send(data);
  });
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`WS running on port ${PORT}`));
