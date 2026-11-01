const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 3000);
const STATIC_ROOT = path.resolve(__dirname, '../turball_extracted');
const INDEX_FILE = path.join(STATIC_ROOT, 'index.html');
const PARTY_API_ORIGIN = new URL(process.env.TURBALL_API_ORIGIN || 'https://api.turball.io');
// Allow slow matchmaker and seat-join handshakes more time through the Render proxy.
const configuredPartyApiTimeout = Number(process.env.TURBALL_API_TIMEOUT_MS || 120000);
const PARTY_API_TIMEOUT_MS = Number.isFinite(configuredPartyApiTimeout)
  ? Math.min(300000, Math.max(20000, configuredPartyApiTimeout))
  : 120000;

// Colyseus may allocate rooms on a worker whose publicAddress differs from
// the general matchmaking API host. Keep that worker mapping so the browser's
// subsequent /<processId>/<roomId> WebSocket reaches the process that reserved
// the seat instead of being sent back to the API gateway.
const processRoutes = new Map();
const PROCESS_ROUTE_TTL_MS = 60 * 60 * 1000;

if (!['http:', 'https:'].includes(PARTY_API_ORIGIN.protocol)) {
  throw new Error('TURBALL_API_ORIGIN must use http:// or https://');
}

const app = express();
app.set('trust proxy', true);
const server = http.createServer(app);
// The small /multiplayer lobby continues to use the local root WebSocket.
// Colyseus party WebSockets use /<processId>/<roomId> and are proxied upstream.
const wss = new WebSocket.Server({ noServer: true });
const rooms = new Map();

// One-digit invite codes are aliases for the real Colyseus room IDs.
// Keep this mapping on the Render process so both clients use the same code.
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
      partyCodes.set(code, { roomId, expiresAt: Date.now() + PARTY_CODE_TTL_MS });
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

function getPublicOrigin(req) {
  const forwardedProto = String(req.get('x-forwarded-proto') || '').split(',')[0].trim();
  const protocol = forwardedProto || req.protocol || 'https';
  return `${protocol}://${req.get('host')}`;
}

function parsePublicAddress(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;

  try {
    let parsed;
    if (/^wss:\/\//i.test(raw)) parsed = new URL(raw.replace(/^wss:/i, 'https:'));
    else if (/^ws:\/\//i.test(raw)) parsed = new URL(raw.replace(/^ws:/i, 'http:'));
    else if (/^https?:\/\//i.test(raw)) parsed = new URL(raw);
    else parsed = new URL(`https://${raw}`);

    if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname) return null;
    return {
      protocol: parsed.protocol,
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      pathnamePrefix: parsed.pathname && parsed.pathname !== '/' ? parsed.pathname.replace(/\/$/, '') : '',
    };
  } catch {
    return null;
  }
}

function rememberProcessRoute(reservation, req) {
  const processId = String(reservation?.processId || '').trim();
  const target = parsePublicAddress(reservation?.publicAddress);
  if (!processId || !target || processId.length > 160 || !/^[A-Za-z0-9_-]+$/.test(processId)) {
    return false;
  }

  const now = Date.now();
  for (const [id, route] of processRoutes.entries()) {
    if (route.expiresAt <= now) processRoutes.delete(id);
  }
  processRoutes.set(processId, { ...target, expiresAt: now + PROCESS_ROUTE_TTL_MS });

  // Colyseus' browser SDK constructs its room socket from publicAddress.
  // Rewrite only that field to this Render host; the upgrade handler below
  // uses processId to forward the connection to the original worker address.
  reservation.publicAddress = new URL(getPublicOrigin(req)).host;
  return true;
}

async function sendGameIndex(req, res, next) {
  try {
    let html = await fs.promises.readFile(INDEX_FILE, 'utf8');
    const origin = new URL(getPublicOrigin(req)).origin.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
    // Route the game's HTTP API and Colyseus matchmaking through this Render
    // service. This avoids browser CORS restrictions on the hosted game.
    html = html.replace(
      /<meta\s+name="api-base"\s+content="[^"]*"\s*\/?\s*>/i,
      `<meta name="api-base" content="${origin}">`,
    );
    res.set('Cache-Control', 'no-store');
    res.type('html').send(html);
  } catch (error) {
    next(error);
  }
}

function proxyHttpToPartyApi(req, res) {
  const upstreamUrl = new URL(req.originalUrl, PARTY_API_ORIGIN);
  const transport = upstreamUrl.protocol === 'https:' ? https : http;
  const headers = { ...req.headers, host: upstreamUrl.host, 'accept-encoding': 'identity' };
  delete headers.connection;
  delete headers['proxy-connection'];
  delete headers['transfer-encoding'];
  delete headers['keep-alive'];
  delete headers.te;
  delete headers.trailer;
  delete headers.upgrade;

  const upstreamRequest = transport.request({
    protocol: upstreamUrl.protocol,
    hostname: upstreamUrl.hostname,
    port: upstreamUrl.port || (upstreamUrl.protocol === 'https:' ? 443 : 80),
    method: req.method,
    path: `${upstreamUrl.pathname}${upstreamUrl.search}`,
    headers,
  }, (upstreamResponse) => {
    const isMatchmakeResponse = upstreamUrl.pathname.startsWith('/matchmake/')
      && (upstreamResponse.statusCode || 500) >= 200
      && (upstreamResponse.statusCode || 500) < 300
      && String(upstreamResponse.headers['content-type'] || '').includes('application/json');

    if (!isMatchmakeResponse) {
      const responseHeaders = { ...upstreamResponse.headers };
      delete responseHeaders.connection;
      delete responseHeaders['keep-alive'];
      delete responseHeaders.upgrade;
      res.writeHead(upstreamResponse.statusCode || 502, responseHeaders);
      upstreamResponse.pipe(res);
      return;
    }

    const chunks = [];
    upstreamResponse.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
    upstreamResponse.on('end', () => {
      const rawBody = Buffer.concat(chunks).toString('utf8');
      let body;
      try {
        body = JSON.parse(rawBody);
      } catch {
        const responseHeaders = { ...upstreamResponse.headers };
        delete responseHeaders.connection;
        delete responseHeaders['keep-alive'];
        delete responseHeaders.upgrade;
        res.writeHead(upstreamResponse.statusCode || 502, responseHeaders);
        res.end(rawBody);
        return;
      }

      const mapped = rememberProcessRoute(body, req);
      const responseBody = Buffer.from(JSON.stringify(body));
      const responseHeaders = { ...upstreamResponse.headers };
      delete responseHeaders.connection;
      delete responseHeaders['keep-alive'];
      delete responseHeaders.upgrade;
      delete responseHeaders['content-encoding'];
      delete responseHeaders['transfer-encoding'];
      responseHeaders['content-length'] = String(responseBody.length);
      res.writeHead(upstreamResponse.statusCode || 502, responseHeaders);
      res.end(responseBody);

      if (body?.processId) {
        console.log(`[party-api] Matchmake ${body.processId}: ${mapped ? 'worker route saved through Render' : 'no public worker address; using API fallback'}`);
      }
    });
    upstreamResponse.on('error', (error) => {
      console.error('[party-api] Matchmake response read error:', error.message);
      if (!res.headersSent) res.status(502).end('Matchmaking response failed.');
      else res.destroy(error);
    });
  });

  upstreamRequest.setTimeout(PARTY_API_TIMEOUT_MS, () => {
    upstreamRequest.destroy(new Error('Party API request timed out'));
  });
  upstreamRequest.on('error', (error) => {
    console.error('[party-api] HTTP proxy error:', error.message);
    if (!res.headersSent) {
      res.status(502).json({
        ok: false,
        error: 'The party service could not be reached. Please try again.',
      });
    } else {
      res.destroy(error);
    }
  });
  req.pipe(upstreamRequest);
}

function proxyWebSocketToPartyApi(req, socket, head) {
  const requestUrl = new URL(req.url, 'http://render.local');
  const processId = requestUrl.pathname.split('/').filter(Boolean)[0] || '';
  const route = processRoutes.get(processId);
  if (route && route.expiresAt <= Date.now()) processRoutes.delete(processId);
  const activeRoute = route && route.expiresAt > Date.now() ? route : null;
  const upstreamBase = activeRoute
    ? `${activeRoute.protocol}//${activeRoute.hostname}:${activeRoute.port}`
    : PARTY_API_ORIGIN.origin;
  const routedPath = activeRoute?.pathnamePrefix
    ? `${activeRoute.pathnamePrefix}${req.url.startsWith('/') ? req.url : `/${req.url}`}`
    : req.url;
  const upstreamUrl = new URL(routedPath, upstreamBase);
  const transport = upstreamUrl.protocol === 'https:' ? https : http;
  if (processId) {
    console.log(`[party-api] Room WebSocket ${processId}: ${activeRoute ? `${upstreamUrl.host} (worker)` : `${upstreamUrl.host} (API fallback)`}`);
  }
  const headers = { ...req.headers, host: upstreamUrl.host };
  // Hop-by-hop headers are regenerated by the upstream client.
  headers.connection = 'Upgrade';
  headers.upgrade = 'websocket';

  const upstreamRequest = transport.request({
    protocol: upstreamUrl.protocol,
    hostname: upstreamUrl.hostname,
    port: upstreamUrl.port || (upstreamUrl.protocol === 'https:' ? 443 : 80),
    method: 'GET',
    path: `${upstreamUrl.pathname}${upstreamUrl.search}`,
    headers,
  });

  let settled = false;
  upstreamRequest.on('upgrade', (upstreamResponse, upstreamSocket, upstreamHead) => {
    settled = true;
    upstreamRequest.setTimeout(0);
    const statusLine = `HTTP/1.1 ${upstreamResponse.statusCode || 101} ${upstreamResponse.statusMessage || 'Switching Protocols'}\r\n`;
    const responseHeaders = Object.entries(upstreamResponse.headers)
      .map(([key, value]) => `${key}: ${Array.isArray(value) ? value.join(', ') : value}`)
      .join('\r\n');
    socket.write(`${statusLine}${responseHeaders}\r\n\r\n`);
    if (upstreamHead?.length) socket.write(upstreamHead);
    if (head?.length) upstreamSocket.write(head);
    upstreamSocket.pipe(socket);
    socket.pipe(upstreamSocket);
    upstreamSocket.on('error', () => socket.destroy());
    socket.on('error', () => upstreamSocket.destroy());
    socket.on('close', () => upstreamSocket.destroy());
  });

  upstreamRequest.on('response', (upstreamResponse) => {
    settled = true;
    const statusLine = `HTTP/1.1 ${upstreamResponse.statusCode || 502} ${upstreamResponse.statusMessage || 'Bad Gateway'}\r\n`;
    const responseHeaders = Object.entries(upstreamResponse.headers)
      .map(([key, value]) => `${key}: ${Array.isArray(value) ? value.join(', ') : value}`)
      .join('\r\n');
    socket.write(`${statusLine}${responseHeaders}\r\n\r\n`);
    upstreamResponse.pipe(socket);
  });

  upstreamRequest.on('error', (error) => {
    console.error('[party-api] WebSocket proxy error:', error.message);
    if (!socket.destroyed && !settled) {
      socket.end('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\nParty service unavailable');
    }
  });
  upstreamRequest.setTimeout(PARTY_API_TIMEOUT_MS, () => upstreamRequest.destroy(new Error('Party WebSocket timed out')));
  upstreamRequest.end();
}

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    try {
      const payload = JSON.parse(String(message));
      if (!payload || typeof payload.type !== 'string') return;

      if (payload.type === 'join-room') {
        const code = String(payload.room || '').toUpperCase();
        const room = getRoom(code || createCode());
        const playerId = String(payload.playerId || `player-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`);
        const playerName = String(payload.name || 'Driver').slice(0, 24);
        const team = payload.team === 'orange' ? 'orange' : 'blue';

        ws.playerId = playerId;
        ws.roomCode = room.code;
        room.players.set(playerId, { id: playerId, name: playerName, team, ws });
        clearClosedSockets(room);
        room.state.players = Array.from(room.players.values()).map((player) => ({
          id: player.id,
          name: player.name,
          team: player.team,
        }));

        ws.send(JSON.stringify({
          type: 'room-joined',
          room: room.code,
          playerId,
          players: room.state.players,
          server: 'turball-render',
        }));
        broadcastRoom(room, { type: 'room-update', room: room.code, players: room.state.players });
        return;
      }

      if (payload.type === 'match-state' && ws.roomCode) {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        room.state = {
          ...room.state,
          ...payload.state,
          phase: payload.state?.phase || room.state.phase,
          lastUpdated: Date.now(),
        };
        broadcastRoom(room, { type: 'match-state', room: room.code, state: room.state });
      }

      if (payload.type === 'chat' && ws.roomCode) {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
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
    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    if (ws.playerId) room.players.delete(ws.playerId);
    clearClosedSockets(room);
    if (room.players.size === 0) {
      rooms.delete(ws.roomCode);
      return;
    }
    broadcastRoom(room, { type: 'room-update', room: room.code, players: room.state.players });
  });
});

// Party-code endpoints are local to this Render instance. Use route-specific
// JSON parsing so proxied Colyseus API request bodies remain untouched.
app.post('/api/party-codes', express.json({ limit: '2kb' }), (req, res) => {
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

app.delete('/api/party-codes/:code', express.json({ limit: '2kb' }), (req, res) => {
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

// Same-origin proxy for the game's existing API and Colyseus party service.
app.use((req, res, next) => {
  if (req.path === '/api/v1' || req.path.startsWith('/api/v1/') || req.path === '/matchmake' || req.path.startsWith('/matchmake/') || req.path === '/auth' || req.path.startsWith('/auth/')) {
    proxyHttpToPartyApi(req, res);
    return;
  }
  next();
});

app.use('/health', (req, res) => {
  res.json({ ok: true, rooms: rooms.size, partyCodes: partyCodes.size, workerRoutes: processRoutes.size, uptime: process.uptime(), partyApi: PARTY_API_ORIGIN.origin });
});

// The standalone lobby endpoints are retained for the separate /multiplayer page.
app.get('/api/create-room', (req, res) => {
  let code = createCode();
  while (rooms.has(code)) code = createCode();
  const room = getRoom(code);
  res.json({ ok: true, room: room.code, ws: `${req.protocol === 'https' ? 'wss' : 'ws'}://${req.get('host')}` });
});

app.get('/api/room/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const room = rooms.get(code) || getRoom(code);
  res.json({
    ok: true,
    room: room.code,
    players: Array.from(room.players.values()).map((player) => ({ id: player.id, name: player.name, team: player.team })),
  });
});

app.get('/multiplayer', (req, res) => res.sendFile(path.join(__dirname, 'public', 'multiplayer.html')));
app.get('/multiplayer.js', (req, res) => res.sendFile(path.join(__dirname, 'public', 'multiplayer.js')));

app.get('/', sendGameIndex);
app.get('/index.html', sendGameIndex);
app.use(express.static(STATIC_ROOT));

app.use((req, res) => {
  if (req.path.startsWith('/api/') || path.extname(req.path)) {
    return res.status(404).type('text/plain').send('Not found');
  }
  return sendGameIndex(req, res, (error) => {
    console.error('Unable to serve game index:', error);
    res.status(500).type('text/plain').send('Unable to load the game.');
  });
});

server.on('upgrade', (req, socket, head) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (pathname === '/' || pathname === '/multiplayer') {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
    return;
  }
  proxyWebSocketToPartyApi(req, socket, head);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Turball multiplayer server listening on 0.0.0.0:${PORT}`);
  console.log(`Party API proxy target: ${PARTY_API_ORIGIN.origin}`);
});
