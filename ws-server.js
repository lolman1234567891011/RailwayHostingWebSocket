const WebSocket = require('ws');
const http = require('http');
const server = http.createServer((req, res) => { res.writeHead(200); res.end('BrainRot WS Server'); });
const wss = new WebSocket.Server({ server });
const rooms = {};

// ===== GLOBAL STUDY FEED: anonymous aggregates only (no names, no ids). Subjects are whitelisted. =====
const SUBJECTS = ['Chemistry', 'Physics', 'Biology', 'Mathematics', 'History', 'Literature', 'Computer Science', 'Economics', 'Geography', 'Other'];
const feedSubs = new Set();
let answered = 0, day = new Date().toISOString().slice(0, 10);
function snapshot() {
  const bySub = {}, byCC = {};
  for (const c of wss.clients) if (c.study) {
    bySub[c.study.subject] = (bySub[c.study.subject] || 0) + 1;
    const k = c.study.cc + '|' + c.study.subject; byCC[k] = (byCC[k] || 0) + 1;
  }
  const rows = Object.entries(byCC).map(([k, count]) => { const [cc, subject] = k.split('|'); return { cc, subject, count }; }).sort((a, b) => b.count - a.count).slice(0, 6);
  const top = Object.entries(bySub).sort((a, b) => b[1] - a[1])[0];
  return { type: 'feed', total: Object.values(bySub).reduce((a, b) => a + b, 0), subjects: bySub, rows, top: top ? top[0] : null, answered };
}
setInterval(() => {
  const d = new Date().toISOString().slice(0, 10); if (d !== day) { day = d; answered = 0; }
  if (!feedSubs.size) return;
  const data = JSON.stringify(snapshot());
  feedSubs.forEach(c => c.readyState === WebSocket.OPEN && c.send(data));
}, 3000);

wss.on('connection', (ws) => {
  let playerName, roomCode;
  // Heartbeat: reap sockets that died without a close frame so rooms/feed counts stay accurate.
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('close', () => feedSubs.delete(ws));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'feed_sub') { feedSubs.add(ws); ws.send(JSON.stringify(snapshot())); return; }
      if (msg.type === 'study_join') { ws.study = { subject: SUBJECTS.includes(msg.subject) ? msg.subject : 'Other', cc: /^[A-Z]{2}$/.test(msg.cc) ? msg.cc : '--' }; return; }
      if (msg.type === 'study_leave') { ws.study = null; return; }
      if (msg.type === 'study_answer') { const n = Date.now(); if (n - (ws.lastAns || 0) > 800) { ws.lastAns = n; answered++; } return; }
      if (msg.type === 'join') {
        playerName = msg.name;
        roomCode = msg.code;
        if (!rooms[roomCode]) rooms[roomCode] = { players: [], clients: new Set() };
        rooms[roomCode].clients.add(ws);
        if (!rooms[roomCode].players.includes(playerName)) rooms[roomCode].players.push(playerName);
        broadcast(roomCode, { type: 'players', players: rooms[roomCode].players });
        const R = rooms[roomCode]; // late joiners / reconnects catch up instead of hanging on the waiting room
        if (R.host) ws.send(JSON.stringify({ type: 'host_announce', name: R.host }));
        if (R.quiz && Date.now() - R.quizAt < 10 * 60000) ws.send(JSON.stringify({ type: 'quiz_start', quiz: R.quiz }));
        return;
      }
      if (msg.type === 'chat') { broadcast(roomCode, { type: 'chat', username: msg.username, message: msg.message }); return; }
      if (msg.type === 'quiz_start') { if (rooms[roomCode]) { rooms[roomCode].quiz = msg.quiz; rooms[roomCode].quizAt = Date.now(); } broadcast(roomCode, { type: 'quiz_start', quiz: msg.quiz }); return; }
      if (msg.type === 'player_done') { broadcast(roomCode, { type: 'player_done', name: msg.name }); return; }
      // CO-OP BOSS BATTLE (and future types): room-scoped relay, host client is authoritative.
      if (msg.type === 'host_announce' && rooms[roomCode]) rooms[roomCode].host = msg.name;
      broadcast(roomCode, msg);
    } catch (e) {
      console.error('Parse error:', e);
    }
  });
  ws.on('close', () => {
    if (!roomCode || !rooms[roomCode]) return;
    rooms[roomCode].clients.delete(ws);
    rooms[roomCode].players = rooms[roomCode].players.filter(p => p !== playerName);
    broadcast(roomCode, { type: 'players', players: rooms[roomCode].players });
    if (rooms[roomCode].clients.size === 0) delete rooms[roomCode];
  });
  ws.on('error', () => ws.close());
});

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => { if (ws.isAlive === false) return ws.terminate(); ws.isAlive = false; ws.ping(); });
}, 30000);
wss.on('close', () => clearInterval(heartbeatInterval));

function broadcast(code, msg) {
  if (!rooms[code]) return;
  const data = JSON.stringify(msg);
  rooms[code].clients.forEach(client => { if (client.readyState === WebSocket.OPEN) client.send(data); });
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`WS running on port ${PORT}`));
