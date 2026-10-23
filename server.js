
"use strict";

const http = require("http");
const WebSocket = require("ws");
const { randomUUID } = require("crypto");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const MAX_ROOM_ID_LENGTH = 128;
const MAX_CLIENT_ID_LENGTH = 128;
const MAX_MESSAGE_BYTES = 256 * 1024;
const HEARTBEAT_INTERVAL_MS = 30_000;
const MAX_ROOMS = 10_000;

// roomId -> Set<WebSocket>
const rooms = new Map();

// clientId -> WebSocket
const clients = new Map();

const server = http.createServer((req, res) => {
    if (req.method === "GET" && req.url === "/health") {
        res.writeHead(200, {
            "content-type": "application/json",
            "cache-control": "no-store",
        });
        res.end(JSON.stringify({
            status: "ok",
            uptime: Math.floor(process.uptime()),
            rooms: rooms.size,
            connections: wss.clients.size,
        }));
        return;
    }

    res.writeHead(404, {
        "content-type": "text/plain",
    });
    res.end("Not found");
});

const wss = new WebSocket.Server({
    server,
    maxPayload: MAX_MESSAGE_BYTES,
    perMessageDeflate: false,
    clientTracking: true,
});

function send(ws, data) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        return false;
    }

    try {
        ws.send(JSON.stringify(data));
        return true;
    } catch {
        return false;
    }
}

function validId(value, maxLength) {
    return (
        typeof value === "string" &&
        value.length > 0 &&
        value.length <= maxLength &&
        value.trim() === value &&
        !/[\u0000-\u001f\u007f]/.test(value)
    );
}

function getRoom(roomId) {
    return rooms.get(roomId);
}

function notifyRoom(room, excludedSocket, message) {
    for (const peer of room) {
        if (peer !== excludedSocket) {
            send(peer, message);
        }
    }
}

function leaveRoom(ws, notify = true) {
    const roomId = ws.roomId;
    if (!roomId) return;

    const clientId = ws.clientId;
    const room = getRoom(roomId);

    // Clear membership first to make cleanup idempotent.
    ws.roomId = null;

    if (!room || !room.delete(ws)) return;

    if (notify) {
        notifyRoom(room, ws, {
            type: "peer-left",
            peerId: clientId,
        });
    }

    if (room.size === 0) {
        rooms.delete(roomId);
        return;
    }

    // A remaining peer may wait for a replacement.
    notifyRoom(room, ws, {
        type: "room-available",
        roomId,
    });
}

function cleanup(ws) {
    leaveRoom(ws);

    if (
        ws.clientId &&
        clients.get(ws.clientId) === ws
    ) {
        clients.delete(ws.clientId);
    }

    ws.clientId = null;
}

function joinRoom(ws, msg) {
    const roomId = msg.roomId;
    const clientId = msg.clientId;

    if (
        !validId(roomId, MAX_ROOM_ID_LENGTH) ||
        !validId(clientId, MAX_CLIENT_ID_LENGTH)
    ) {
        send(ws, {
            type: "error",
            code: "INVALID_JOIN",
            message: "Valid roomId and clientId are required.",
        });
        return;
    }

    if (ws.roomId) {
        send(ws, {
            type: "error",
            code: "ALREADY_JOINED",
            message: "Leave the current room before joining another.",
        });
        return;
    }

    // One active connection per client ID.
    const existing = clients.get(clientId);

    if (existing && existing !== ws) {
        send(ws, {
            type: "error",
            code: "CLIENT_ID_IN_USE",
            message: "This clientId is already connected.",
        });
        return;
    }

    let room = rooms.get(roomId);

    if (room && room.size >= 2) {
        send(ws, { type: "room-full" });
        return;
    }

    if (!room) {
        if (rooms.size >= MAX_ROOMS) {
            send(ws, {
                type: "error",
                code: "SERVER_BUSY",
                message: "Room capacity reached.",
            });
            return;
        }

        room = new Set();
        rooms.set(roomId, room);
    }

    const existingPeer = room.size === 1
        ? Array.from(room)[0]
        : null;

    ws.clientId = clientId;
    ws.roomId = roomId;
    ws.isAlive = true;

    room.add(ws);
    clients.set(clientId, ws);

    if (!existingPeer) {
        send(ws, {
            type: "joined",
            roomId,
            peerId: null,
            isPolite: false,
        });
        return;
    }

    // The first participant is impolite; the second is polite.
    send(existingPeer, {
        type: "peer-joined",
        peerId: clientId,
        isPolite: true,
    });

    send(ws, {
        type: "joined",
        roomId,
        peerId: existingPeer.clientId,
        isPolite: true,
    });

    // Notify the first peer of its role explicitly.
    // The frontend should apply these roles consistently.
    send(existingPeer, {
        type: "role",
        isPolite: false,
    });
}

function forwardSignal(ws, msg) {
    if (!ws.roomId) {
        send(ws, {
            type: "error",
            code: "NOT_IN_ROOM",
        });
        return;
    }

    const room = rooms.get(ws.roomId);
    if (!room || !room.has(ws)) return;

    let payload;

    switch (msg.type) {
        case "offer":
        case "answer": {
            if (
                !msg.description ||
                typeof msg.description !== "object" ||
                Array.isArray(msg.description) ||
                msg.description.type !== msg.type ||
                typeof msg.description.sdp !== "string" ||
                msg.description.sdp.length === 0 ||
                msg.description.sdp.length > 128 * 1024
            ) {
                send(ws, {
                    type: "error",
                    code: "INVALID_DESCRIPTION",
                });
                return;
            }

            payload = {
                type: msg.type,
                description: msg.description,
                from: ws.clientId,
            };
            break;
        }

        case "ice-candidate": {
            if (
                !msg.candidate ||
                typeof msg.candidate !== "object" ||
                Array.isArray(msg.candidate) ||
                (
                    msg.candidate.candidate !== null &&
                    typeof msg.candidate.candidate !== "string"
                )
            ) {
                send(ws, {
                    type: "error",
                    code: "INVALID_CANDIDATE",
                });
                return;
            }

            payload = {
                type: "ice-candidate",
                candidate: msg.candidate,
                from: ws.clientId,
            };
            break;
        }

        default:
            return;
    }

    notifyRoom(room, ws, payload);
}

wss.on("connection", (ws, req) => {
    ws.clientId = null;
    ws.roomId = null;
    ws.isAlive = true;

    // Optional: restrict Origin for browser clients.
    // Configure ALLOWED_ORIGINS as a comma-separated env variable.
    const allowedOrigins = (process.env.ALLOWED_ORIGINS || "")
        .split(",")
        .map(origin => origin.trim())
        .filter(Boolean);

    const origin = req.headers.origin;

    if (
        allowedOrigins.length > 0 &&
        origin &&
        !allowedOrigins.includes(origin)
    ) {
        ws.close(1008, "Origin not allowed");
        return;
    }

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    ws.on("message", (raw, isBinary) => {
        if (isBinary) {
            ws.close(1003, "Text messages only");
            return;
        }

        let msg;

        try {
            msg = JSON.parse(raw.toString());
        } catch {
            send(ws, {
                type: "error",
                code: "INVALID_JSON",
            });
            return;
        }

        if (
            !msg ||
            typeof msg !== "object" ||
            Array.isArray(msg) ||
            typeof msg.type !== "string"
        ) {
            send(ws, {
                type: "error",
                code: "INVALID_MESSAGE",
            });
            return;
        }

        switch (msg.type) {
            case "ping":
                send(ws, { type: "pong" });
                return;

            case "join":
                joinRoom(ws, msg);
                return;

            case "leave":
                leaveRoom(ws);
                send(ws, { type: "left" });
                return;

            case "offer":
            case "answer":
            case "ice-candidate":
                forwardSignal(ws, msg);
                return;

            default:
                send(ws, {
                    type: "error",
                    code: "UNKNOWN_MESSAGE_TYPE",
                });
        }
    });

    ws.on("close", () => cleanup(ws));

    ws.on("error", () => cleanup(ws));
});

// Detect broken connections even when no close event arrives.
const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
        if (!ws.isAlive) {
            ws.terminate();
            continue;
        }

        ws.isAlive = false;
        ws.ping();
    }
}, HEARTBEAT_INTERVAL_MS);

heartbeat.unref();

server.listen(PORT, HOST, () => {
    console.log(`Signaling server listening on ${PORT}`);
});

function shutdown() {
    clearInterval(heartbeat);

    for (const ws of wss.clients) {
        cleanup(ws);
        ws.close(1001, "Server shutting down");
    }

    wss.close(() => {
        server.close(() => process.exit(0));
    });

    // Do not wait indefinitely for clients.
    setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
