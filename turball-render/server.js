const express = require('express');
const http = require('http');
const path = require('path');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 3000);
const STATIC_ROOT = path.resolve(__dirname, '../turball_extracted');

const app = express();
app.use(express.json({ limit: '2kb' }));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = new Map();

// The public party code is deliberately one digit (0-9). It is an alias for
// the longer Colyseus room ID used by the actual game party client.
const partyCodes = new Map();
const partyCodesByRoom = new Map();
const PARTY_CODE_TTL_MS = 6 * 60 * 60 * 1000;

function prunePartyCodes() {
  const now = Date.now();
  for (const [code, entry] of partyCodes.entries()) {
    if (entry.expiresAt <= now) {
      partyCodes.delete(code);
      partyCodesByRoom.delete(entry.roomId);
    }
  }
}

function storePartyCode(roomId) {
  prunePartyCodes();
  const existingCode = partyCodesByRoom.get(roomId);
  if (existingCode && partyCodes.has(existingCode)) {
    partyCodes.get(existingCode).expiresAt = Date.now() + PARTY_CODE_TTL_MS;
    return existingCode;
  }

  for (let digit = 0; digit <= 9; digit += 1) {
    const code = String(digit);
    if (!partyCodes.has(code)) {
      partyCodes.set(code, {
        roomId,
        expiresAt: Date.now() + PARTY_CODE_TTL_MS,
      });
      partyCodesByRoom.set(roomId, code);
      return code;
    }
  }
  return null;
}

function createCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let result = '';
  for (let i = 0; i < 5; i += 1) {
    result += chars[Math.floor(Math.random() * chars.length)];
  }
  return result;
}

function getRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, {
      code,
      players: new Map(),
      state: {
        phase: 'lobby',
        players: [],
        lastUpdated: Date.now(),
      },
    });
  }
  return rooms.get(code);
}

function broadcastRoom(room, payload) {
  const message = JSON.stringify(payload);
  for (const client of room.players.values()) {
    if (client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(message);
    }
  }
}

function clearClosedSockets(room) {
  for (const [id, client] of room.players.entries()) {
    if (client.ws.readyState !== WebSocket.OPEN) {
      room.players.delete(id);
    }
  }
  room.state.players = Array.from(room.players.values()).map((player) => ({
    id: player.id,
    name: player.name,
    team: player.team,
    connected: true,
  }));
}

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    try {
      const payload = JSON.parse(String(message));

      if (!payload || typeof payload.type !== 'string') {
        return;
      }

      if (payload.type === 'join-room') {
        const code = String(payload.room || '').toUpperCase();
        const room = getRoom(code || createCode());
        const playerId = String(payload.playerId || `player-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`);
        const playerName = String(payload.name || 'Driver').slice(0, 24);
        const team = payload.team === 'orange' ? 'orange' : 'blue';

        ws.playerId = playerId;
        ws.roomCode = room.code;

        room.players.set(playerId, {
          id: playerId,
          name: playerName,
          team,
          ws,
        });

        clearClosedSockets(room);
        room.state.players = Array.from(room.players.values()).map((player) => ({
          id: player.id,
          name: player.name,
          team: player.team,
        }));

        const welcome = {
          type: 'room-joined',
          room: room.code,
          playerId,
          players: room.state.players,
          server: 'turball-render',
        };

        ws.send(JSON.stringify(welcome));
        broadcastRoom(room, {
          type: 'room-update',
          room: room.code,
          players: room.state.players,
        });
        return;
      }

      if (payload.type === 'match-state' && ws.roomCode) {
        const room = rooms.get(ws.roomCode);
        if (!room) {
          return;
        }
        room.state = {
          ...room.state,
          ...payload.state,
          phase: payload.state?.phase || room.state.phase,
          lastUpdated: Date.now(),
        };
        broadcastRoom(room, {
          type: 'match-state',
          room: room.code,
          state: room.state,
        });
      }

      if (payload.type === 'chat' && ws.roomCode) {
        const room = rooms.get(ws.roomCode);
        if (!room) {
          return;
        }
        broadcastRoom(room, {
          type: 'chat',
          room: room.code,
          playerId: ws.playerId,
          message: String(payload.message || '').slice(0, 200),
        });
      }
    } catch (error) {
      console.error('WS payload error', error);
    }
  });

  ws.on('close', () => {
    if (!ws.roomCode) {
      return;
    }
    const room = rooms.get(ws.roomCode);
    if (!room) {
      return;
    }
    if (ws.playerId) {
      room.players.delete(ws.playerId);
    }
    clearClosedSockets(room);
    if (room.players.size === 0) {
      rooms.delete(ws.roomCode);
      return;
    }

    broadcastRoom(room, {
      type: 'room-update',
      room: room.code,
      players: room.state.players,
    });
  });
});

app.post('/api/party-codes', (req, res) => {
  const roomId = String(req.body?.roomId || '').trim();
  if (!roomId || roomId.length > 128 || !/^[A-Za-z0-9_-]+$/.test(roomId)) {
    return res.status(400).json({ ok: false, error: 'A valid party room ID is required.' });
  }

  const code = storePartyCode(roomId);
  if (code === null) {
    return res.status(409).json({
      ok: false,
      error: 'All 10 one-digit party codes are currently in use. Leave an old party or try again later.',
    });
  }
  return res.json({ ok: true, code, roomId, expiresInSeconds: PARTY_CODE_TTL_MS / 1000 });
});

app.get('/api/party-codes/:code', (req, res) => {
  const code = String(req.params.code || '');
  if (!/^\d$/.test(code)) {
    return res.status(400).json({ ok: false, error: 'Party codes must be one digit from 0 to 9.' });
  }

  prunePartyCodes();
  const entry = partyCodes.get(code);
  if (!entry) {
    return res.status(404).json({ ok: false, error: 'That one-digit party code is not active.' });
  }
  entry.expiresAt = Date.now() + PARTY_CODE_TTL_MS;
  return res.json({ ok: true, code, roomId: entry.roomId });
});

app.delete('/api/party-codes/:code', (req, res) => {
  const code = String(req.params.code || '');
  const entry = partyCodes.get(code);
  const requestedRoomId = String(req.body?.roomId || '').trim();
  if (!entry) return res.json({ ok: true, released: false });
  if (!requestedRoomId || requestedRoomId !== entry.roomId) {
    return res.status(403).json({ ok: false, error: 'Party code does not match that room.' });
  }
  partyCodes.delete(code);
  partyCodesByRoom.delete(entry.roomId);
  return res.json({ ok: true, released: true });
});

app.use('/health', (req, res) => {
  res.json({ ok: true, rooms: rooms.size, uptime: process.uptime() });
});

app.get('/api/create-room', (req, res) => {
  let code = createCode();
  while (rooms.has(code)) {
    code = createCode();
  }
  const room = getRoom(code);
  res.json({
    ok: true,
    room: room.code,
    ws: `${req.protocol === 'https' ? 'wss' : 'ws'}://${req.get('host')}`,
  });
});

app.get('/api/room/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const room = rooms.get(code) || getRoom(code);
  res.json({
    ok: true,
    room: room.code,
    players: Array.from(room.players.values()).map((player) => ({
      id: player.id,
      name: player.name,
      team: player.team,
    })),
  });
});

app.get('/multiplayer', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'multiplayer.html'));
});

app.get('/multiplayer.js', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'multiplayer.js'));
});

app.use(express.static(STATIC_ROOT));

app.use((req, res) => {
  if (req.path.startsWith('/api/') || path.extname(req.path)) {
    return res.status(404).type('text/plain').send('Not found');
  }
  res.sendFile(path.join(STATIC_ROOT, 'index.html'));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Turball multiplayer server listening on http://0.0.0.0:${PORT}`);
});
