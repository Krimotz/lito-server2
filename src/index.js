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

    // Restore any hibernating sessions from attachments
    this.sessions = new Map();
    for (const ws of this.state.getWebSockets()) {
      const attachment = ws.deserializeAttachment();
      if (attachment) {
        this.sessions.set(ws, attachment);
      }
    }

    // Auto-reply to ping/pong without waking from hibernation
    this.state.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair('ping', 'pong')
    );

    // Heartbeat: send a message every 30 seconds to every connected client.
    // Cloudflare kills WebSockets that have been idle for 100 seconds on
    // the Free plan. This message resets that timer.
    this.heartbeatInterval = setInterval(() => {
      for (const [ws, session] of this.sessions) {
        if (ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({ type: 'ping' }));
          } catch (_) {
            // Client may have disconnected between loop start and now.
          }
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

    // Create WebSocket pair
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept the WebSocket with hibernation support
    this.state.acceptWebSocket(server);

    // Persist role so it survives hibernation
    const attachment = { role };
    server.serializeAttachment(attachment);
    this.sessions.set(server, attachment);

    // Send confirmation to client
    server.send(JSON.stringify({ type: 'joined', role }));

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(ws, message) {
    const session = this.sessions.get(ws);
    if (!session) return;

    // Only broadcasters originate audio. Listeners don't send.
    if (session.role !== 'broadcast') return;

    // Relay binary audio to all listeners
    if (typeof message === 'string') {
      // Text frames are control messages — relay to all as-is
      for (const [peer, peerSession] of this.sessions) {
        if (peer !== ws && peer.readyState === WebSocket.OPEN) {
          peer.send(message);
        }
      }
    } else {
      // Binary frames are audio — relay to listeners only
      for (const [peer, peerSession] of this.sessions) {
        if (peer !== ws && peerSession.role === 'listen' && peer.readyState === WebSocket.OPEN) {
          peer.send(message);
        }
      }
    }
  }

  async webSocketClose(ws, code, reason) {
    this.sessions.delete(ws);
  }

  async webSocketError(ws, error) {
    this.sessions.delete(ws);
  }
}
