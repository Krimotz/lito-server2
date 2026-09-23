export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Match /broadcast/<roomId> or /listen/<roomId>
    const match = url.pathname.match(/^\/(broadcast|listen)\/([a-zA-Z0-9-]+)$/);
    if (!match) {
      return new Response('Invalid path. Use /broadcast/<roomId> or /listen/<roomId>', { status: 400 });
    }

    const [, role, roomId] = match;

    // Verify WebSocket upgrade request
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 426 });
    }

    // Route to a Durable Object instance keyed by roomId
    const id = env.RELAY.idFromName(roomId);
    const stub = env.RELAY.get(id);

    // Pass the request (with role info) to the Durable Object
    const doUrl = new URL(request.url);
    doUrl.searchParams.set('role', role);
    return stub.fetch(new Request(doUrl.toString(), request));
  },
};

export class RelayRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.sessions = new Map();       // ws -> { role }
    this.broadcaster = null;         // ws | null, the active broadcaster
    this.graceTimer = null;          // pending room-cleanup timer, if any

    for (const ws of this.state.getWebSockets()) {
      const attachment = ws.deserializeAttachment();
      if (attachment) {
        this.sessions.set(ws, attachment);
        if (attachment.role === 'broadcast') {
          this.broadcaster = ws;
        }
      }
    }

    this.state.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair('ping', 'pong')
    );

    this.heartbeatInterval = setInterval(() => {
      for (const [ws] of this.sessions) {
        if (ws.readyState === WebSocket.OPEN) {
          try { ws.send(JSON.stringify({ type: 'ping' })); } catch (_) {}
        }
      }
    }, 30000);
  }

  async fetch(request) {
    const url = new URL(request.url);
    const role = url.searchParams.get('role');

    if (role !== 'broadcast' && role !== 'listen') {
      return new Response('Invalid role', { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    const attachment = { role };
    server.serializeAttachment(attachment);
    this.sessions.set(server, attachment);

    // A new broadcaster takes over the slot. Evict any existing one.
    if (role === 'broadcast') {
      if (this.broadcaster && this.broadcaster !== server) {
        try {
          this.broadcaster.close(4001, 'replaced by new broadcaster');
        } catch (_) {}
        this.sessions.delete(this.broadcaster);
      }
      this.broadcaster = server;
    }

    // Cancel any pending room GC.
    if (this.graceTimer) {
      clearTimeout(this.graceTimer);
      this.graceTimer = null;
    }

    server.send(JSON.stringify({ type: 'joined', role }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const session = this.sessions.get(ws);
    if (!session) return;

    // Only the active broadcaster may originate audio or control frames.
    if (ws !== this.broadcaster) return;

    if (typeof message === 'string') {
      // Control frames: relay to listeners only, not other broadcasters.
      for (const [peer, peerSession] of this.sessions) {
        if (peer !== ws
            && peerSession.role === 'listen'
            && peer.readyState === WebSocket.OPEN) {
          peer.send(message);
        }
      }
    } else {
      // Binary audio: relay to listeners only.
      for (const [peer, peerSession] of this.sessions) {
        if (peer !== ws
            && peerSession.role === 'listen'
            && peer.readyState === WebSocket.OPEN) {
          peer.send(message);
        }
      }
    }
  }

  async webSocketClose(ws) {
    this.sessions.delete(ws);
    if (ws === this.broadcaster) {
      this.broadcaster = null;
      this.scheduleRoomGc();
    }
  }

  async webSocketError(ws) {
    this.sessions.delete(ws);
    if (ws === this.broadcaster) {
      this.broadcaster = null;
      this.scheduleRoomGc();
    }
  }

  scheduleRoomGc() {
    // If no listeners remain AND no broadcaster, close the DO after grace.
    // If listeners remain, keep the room alive indefinitely so they can
    // hear the broadcaster when they return.
    if (this.graceTimer) clearTimeout(this.graceTimer);
    this.graceTimer = setTimeout(() => {
      if (!this.broadcaster && this.sessions.size === 0) {
        // Nothing left; let the DO hibernate/evict naturally.
        // No explicit close needed — DOs idle out.
      }
      this.graceTimer = null;
    }, 10 * 60 * 1000);
  }
}
