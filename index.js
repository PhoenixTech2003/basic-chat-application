const express = require("express");
const path = require("node:path");
const { Server } = require("socket.io");
const { createServer } = require("node:http");
const sqlite3 = require("sqlite3");
const { open } = require("sqlite");

async function main() {
  const db = await open({
    filename: "chat.db",
    driver: sqlite3.Database,
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT
    )
    `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
  });

  app.set("view engine", "ejs");
  app.set("views", path.join(__dirname, "views"));

  app.get("/", (req, res) => {
    res.render("index");
  });

  io.on("connection", async (socket) => {
    socket.on("chat message", async (msg, clientOffset, callback) => {
      let result;
      try {
        result = await db.run(`INSERT INTO MESSAGES (content, client_offset) VALUES (?, ?)`, msg, clientOffset);
      } catch (e) {
        if(e.errno == 19)
          callback()
        return;
      }
      io.emit("chat message", msg, result.lastID);
      callback()
    });

    if (!socket.recovered) {
      try {
        await db.each(
          "SELECT id, content FROM messages WHERE id > ?",
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            socket.emit("chat message", row.content, row.id);
          }
        );
      } catch (e) {}
    }
  });

  server.listen(3000, () => {
    console.log(`Server is running at http://localhost:3000`);
  });
}

main();
