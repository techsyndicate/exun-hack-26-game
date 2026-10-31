const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { WebSocketServer } = require("ws");

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 3000);
const ROOT_DIR = __dirname;
const ASSETS_DIR = path.join(ROOT_DIR, "assets");
const MAX_MESSAGE_BYTES = 16 * 1024;
const CONTROLLER_PING_MS = 2000;

const NEUTRAL_INPUT = {
  joystick: { x: 0, y: 0, pressed: false },
  buttons: [false, false, false, false],
};

let latestInput = { ...NEUTRAL_INPUT, receivedAt: null, deviceId: null };

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ttf": "font/ttf",
};

function sendJson(response, statusCode, body) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(body));
}

function finiteNumber(value, fieldName) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    throw new Error(`${fieldName} must be a finite number.`);
  }
  return number;
}

function normalizeInput(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("The message must be a JSON object.");
  }

  const joystick = payload.joystick || {};
  const rawButtons = payload.buttons;
  if (!Array.isArray(rawButtons) || rawButtons.length !== 4) {
    throw new Error("buttons must be an array containing exactly 4 values.");
  }

  const x = finiteNumber(joystick.x, "joystick.x");
  const y = finiteNumber(joystick.y, "joystick.y");
  if (x < -1 || x > 1 || y < -1 || y > 1) {
    throw new Error(
      "joystick.x and joystick.y must be normalized between -1 and 1."
    );
  }
  if (joystick.pressed !== undefined && typeof joystick.pressed !== "boolean") {
    throw new Error("joystick.pressed must be a boolean.");
  }

  return {
    joystick: { x, y, pressed: joystick.pressed === true },
    buttons: rawButtons.map((button, index) => {
      if (typeof button !== "boolean") {
        throw new Error(`buttons[${index}] must be a boolean.`);
      }
      return button;
    }),
    receivedAt: new Date().toISOString(),
    deviceId:
      typeof payload.deviceId === "string"
        ? payload.deviceId.slice(0, 100)
        : null,
  };
}

// Only index.html and the assets folder are public, so server.js, .git etc. are never served.
function serveStatic(request, response) {
  const requestPath = request.url === "/" ? "/index.html" : request.url;
  const decodedPath = decodeURIComponent(requestPath.split("?")[0]);
  const filePath = path.resolve(ROOT_DIR, `.${decodedPath}`);

  const isIndex = filePath === path.join(ROOT_DIR, "index.html");
  const isAsset = filePath.startsWith(`${ASSETS_DIR}${path.sep}`);
  if (!isIndex && !isAsset) {
    sendJson(response, 404, { error: "Not found." });
    return;
  }

  fs.stat(filePath, (error, stats) => {
    if (error || !stats.isFile()) {
      sendJson(response, 404, { error: "Not found." });
      return;
    }
    response.writeHead(200, {
      "Content-Type":
        MIME_TYPES[path.extname(filePath).toLowerCase()] ||
        "application/octet-stream",
      "Cache-Control": "no-cache",
    });
    fs.createReadStream(filePath).pipe(response);
  });
}

const server = http.createServer((request, response) => {
  if (request.method === "GET" && request.url === "/api/state") {
    sendJson(response, 200, latestInput);
    return;
  }

  if (request.method === "GET") {
    serveStatic(request, response);
    return;
  }

  sendJson(response, 405, { error: "Method not allowed." });
});

// Browsers running the game connect to /ws and receive every controller reading.
const gameSockets = new WebSocketServer({ noServer: true });
// The ESP32 connects to /controller and streams readings over one open connection.
const controllerSockets = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_MESSAGE_BYTES,
});

function broadcast(message) {
  const serialized = JSON.stringify(message);
  for (const client of gameSockets.clients) {
    if (client.readyState === 1) {
      client.send(serialized);
    }
  }
}

gameSockets.on("connection", (socket) => {
  socket.send(JSON.stringify({ type: "input", data: latestInput }));
});

controllerSockets.on("connection", (socket, request) => {
  const address = request.socket.remoteAddress;
  console.log(`Controller connected from ${address}`);
  socket.isAlive = true;
  socket.on("pong", () => {
    socket.isAlive = true;
  });

  socket.on("message", (data, isBinary) => {
    if (isBinary) return;
    try {
      latestInput = normalizeInput(JSON.parse(data.toString()));
      broadcast({ type: "input", data: latestInput });
    } catch (error) {
      socket.send(JSON.stringify({ type: "error", error: error.message }));
    }
  });

  socket.on("close", () => {
    console.log(`Controller disconnected (${address})`);
    // release everything so the player doesn't keep running or holding a button
    latestInput = {
      ...NEUTRAL_INPUT,
      receivedAt: new Date().toISOString(),
      deviceId: latestInput.deviceId,
    };
    broadcast({ type: "input", data: latestInput });
  });
});

// Drop controllers that vanish without closing (e.g. ESP32 lost power or Wi-Fi).
const controllerPing = setInterval(() => {
  for (const socket of controllerSockets.clients) {
    if (!socket.isAlive) {
      socket.terminate();
      continue;
    }
    socket.isAlive = false;
    socket.ping();
  }
}, CONTROLLER_PING_MS);

server.on("upgrade", (request, socket, head) => {
  const { pathname } = new URL(request.url, "http://localhost");
  const target =
    pathname === "/ws"
      ? gameSockets
      : pathname === "/controller"
      ? controllerSockets
      : null;
  if (!target) {
    socket.destroy();
    return;
  }
  target.handleUpgrade(request, socket, head, (client) => {
    target.emit("connection", client, request);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`Game server listening on http://localhost:${PORT}`);
  console.log(
    `ESP32 should connect to ws://<this-computer-ip>:${PORT}/controller`
  );
});

function shutdown() {
  clearInterval(controllerPing);
  gameSockets.close();
  controllerSockets.close();
  server.close(() => process.exit(0));
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
