// server.js
const WebSocket = require('ws');

const PORT = 10000;
const wss = new WebSocket.Server({ port: PORT });

// roomId -> Map<clientId, WebSocket>
const rooms = new Map();

console.log(`Multi-User Signaling Server running on ws://localhost:${PORT}`);

wss.on('connection', (ws) => {
    ws.clientId = null;
    ws.roomId = null;

    ws.on('message', (message) => {
        let msg;
        try {
            msg = JSON.parse(message.toString());
        } catch (e) {
            return;
        }

        switch (msg.type) {
            case 'join': {
                const { roomId, clientId } = msg;
                if (!roomId || !clientId) return;

                ws.roomId = roomId;
                ws.clientId = clientId;

                if (!rooms.has(roomId)) {
                    rooms.set(roomId, new Map());
                }

                const room = rooms.get(roomId);

                // Collect list of existing peer IDs before adding new client
                const existingPeerIds = Array.from(room.keys());

                room.set(clientId, ws);

                // 1. Send joining client the list of all peers currently in the room
                ws.send(JSON.stringify({
                    type: 'joined',
                    roomId,
                    clientId,
                    existingPeers: existingPeerIds
                }));

                // 2. Notify all existing clients that a new peer joined
                for (const [peerId, peerWs] of room.entries()) {
                    if (peerId !== clientId && peerWs.readyState === WebSocket.OPEN) {
                        peerWs.send(JSON.stringify({
                            type: 'peer-joined',
                            peerId: clientId
                        }));
                    }
                }
                break;
            }

            case 'offer':
            case 'answer':
            case 'ice-candidate': {
                const room = rooms.get(ws.roomId);
                if (!room) return;

                const targetWs = room.get(msg.target);
                if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                    targetWs.send(JSON.stringify({
                        ...msg,
                        sender: ws.clientId
                    }));
                }
                break;
            }

            case 'leave': {
                cleanup(ws);
                break;
            }
        }
    });

    ws.on('close', () => cleanup(ws));
    ws.on('error', () => cleanup(ws));
});

function cleanup(ws) {
    if (!ws.roomId || !ws.clientId) return;
    const room = rooms.get(ws.roomId);
    if (room) {
        room.delete(ws.clientId);
        for (const [, peerWs] of room.entries()) {
            if (peerWs.readyState === WebSocket.OPEN) {
                peerWs.send(JSON.stringify({ type: 'peer-left', peerId: ws.clientId }));
            }
        }
        if (room.size === 0) {
            rooms.delete(ws.roomId);
        }
    }
    ws.roomId = null;
    ws.clientId = null;
}
