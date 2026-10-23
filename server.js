
'use strict';

const http = require('http');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT) || 10000;
const HOST = '0.0.0.0';

// HTTP server: used by Render for port detection and health checks.
const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Cache-Control': 'no-store'
        });

        res.end(JSON.stringify({
            status: 'ok',
            service: 'WebRTC Signaling Server'
        }));
        return;
    }

    res.writeHead(404, {
        'Content-Type': 'application/json; charset=utf-8'
    });
    res.end(JSON.stringify({ error: 'Not found' }));
});

// Keep the WebSocket endpoint at the root path to preserve
// compatibility with clients using wss://your-service.onrender.com
const wss = new WebSocket.Server({
    server,
    maxPayload: 1024 * 1024
});

// roomId -> Map(clientId, WebSocket)
const rooms = new Map();

function send(ws, data) {
    if (ws.readyState !== WebSocket.OPEN) return false;

    try {
        ws.send(JSON.stringify(data));
        return true;
    } catch (error) {
        console.error('WebSocket send failed:', error.message);
        return false;
    }
}

function cleanup(ws) {
    const roomId = ws.roomId;
    const clientId = ws.clientId;

    if (!roomId || !clientId) return;

    const room = rooms.get(roomId);

    // Clear this socket's room assignment first.
    ws.roomId = null;
    ws.clientId = null;

    if (!room) return;

    // Do not remove a newer connection that replaced this socket.
    if (room.get(clientId) !== ws) return;

    room.delete(clientId);

    for (const peerWs of room.values()) {
        send(peerWs, {
            type: 'peer-left',
            peerId: clientId
        });
    }

    if (room.size === 0) {
        rooms.delete(roomId);
    }

    console.log(`Client left room: ${roomId}`);
}

wss.on('connection', (ws) => {
    ws.clientId = null;
    ws.roomId = null;
    ws.isAlive = true;

    console.log('WebSocket client connected');

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (rawMessage) => {
        let msg;

        try {
            msg = JSON.parse(rawMessage.toString());
        } catch {
            send(ws, {
                type: 'error',
                message: 'Invalid JSON message'
            });
            return;
        }

        if (!msg || typeof msg.type !== 'string') return;

        switch (msg.type) {
            case 'join': {
                const roomId = String(msg.roomId || '').trim();
                const clientId = String(msg.clientId || '').trim();

                if (
                    !roomId ||
                    !clientId ||
                    roomId.length > 200 ||
                    clientId.length > 200
                ) {
                    send(ws, {
                        type: 'error',
                        message: 'Invalid roomId or clientId'
                    });
                    return;
                }

                // Avoid stale room membership if this socket joins again.
                if (ws.roomId && ws.clientId) {
                    cleanup(ws);
                }

                if (!rooms.has(roomId)) {
                    rooms.set(roomId, new Map());
                }

                const room = rooms.get(roomId);
                const existingSocket = room.get(clientId);

                // A repeated join with the same socket is harmless.
                if (existingSocket === ws) {
                    send(ws, {
                        type: 'joined',
                        roomId,
                        clientId,
                        existingPeers: Array.from(room.keys())
                            .filter(id => id !== clientId)
                    });
                    return;
                }

                // Replace an old connection using the same client ID.
                if (existingSocket) {
                    existingSocket.close(4001, 'Replaced by new connection');
                }

                const existingPeerIds = Array.from(room.keys());

                ws.roomId = roomId;
                ws.clientId = clientId;

                room.set(clientId, ws);

                // Tell the joining client about peers already in the room.
                send(ws, {
                    type: 'joined',
                    roomId,
                    clientId,
                    existingPeers: existingPeerIds
                });

                // Tell existing peers about the joining client.
                for (const [peerId, peerWs] of room.entries()) {
                    if (peerId !== clientId) {
                        send(peerWs, {
                            type: 'peer-joined',
                            peerId: clientId
                        });
                    }
                }

                console.log(
                    `Client ${clientId} joined room ${roomId}`
                );
                break;
            }

            case 'offer':
            case 'answer':
            case 'ice-candidate': {
                if (!ws.roomId || !ws.clientId) {
                    send(ws, {
                        type: 'error',
                        message: 'Join a room before sending signaling messages'
                    });
                    return;
                }

                const targetId = String(msg.target || '');
                const room = rooms.get(ws.roomId);

                if (!room || !targetId || targetId === ws.clientId) {
                    return;
                }

                const targetWs = room.get(targetId);

                if (!targetWs || targetWs.readyState !== WebSocket.OPEN) {
                    send(ws, {
                        type: 'error',
                        message: 'Target peer is not connected',
                        target: targetId
                    });
                    return;
                }

                // Preserve the original message format and add sender ID.
                send(targetWs, {
                    ...msg,
                    sender: ws.clientId
                });
                break;
            }

            case 'leave':
                cleanup(ws);
                break;

            default:
                // Ignore unknown message types for compatibility.
                break;
        }
    });

    ws.on('close', () => {
        cleanup(ws);
        console.log('WebSocket client disconnected');
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error.message);
        cleanup(ws);
    });
});

// Detect broken connections and release stale room memberships.
const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
        if (ws.isAlive === false) {
            ws.terminate();
            continue;
        }

        ws.isAlive = false;
        ws.ping();
    }
}, 30000);

wss.on('close', () => {
    clearInterval(heartbeat);
});

server.listen(PORT, HOST, () => {
    console.log(`Signaling server listening on ${HOST}:${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
});

function shutdown() {
    console.log('Shutting down signaling server...');

    clearInterval(heartbeat);

    for (const ws of wss.clients) {
        ws.close(1001, 'Server shutting down');
    }

    wss.close(() => {
        server.close(() => {
            process.exit(0);
        });
    });

    // Avoid hanging indefinitely during shutdown.
    setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
