// index.js
const { WebSocketServer, WebSocket } = require('ws');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const crypto = require('crypto');

const PORT = 8080;
const MAX_PAYLOAD_SIZE = 1024 * 1024;
const MAX_CARDS_PER_BOX = 1000;

const wss = new WebSocketServer({
    port: PORT,
    maxPayload: MAX_PAYLOAD_SIZE
});

const dbPath = path.join(__dirname, 'cards.db');
const db = new sqlite3.Database(dbPath);

db.serialize(() => {
    db.run(`
    CREATE TABLE IF NOT EXISTS cards (
      id TEXT PRIMARY KEY,
      box_id TEXT NOT NULL,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL
    )
  `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_box ON cards(box_id)`);

    cleanupExpiredCards();
});

function cleanupExpiredCards() {
    const now = Date.now();
    db.run(`DELETE FROM cards WHERE expires_at <= ?`, [now], function (err) {
        if (err) {
            console.error('クリーンアップエラー:', err);
        } else if (this.changes > 0) {
            console.log(`🧹 期限切れカード ${this.changes} 件を自動削除しました`);
        }
    });
}

setInterval(cleanupExpiredCards, 60 * 60 * 1000);

wss.on('connection', (ws) => {
    ws.box_id = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'POST_CARD': {
                    const { box_id, payload, expires_at } = data;
                    if (!box_id || !payload) return;

                    ws.box_id = box_id;

                    const payloadStr = JSON.stringify(payload);
                    if (payloadStr.length > MAX_PAYLOAD_SIZE) {
                        ws.send(JSON.stringify({ type: 'ERROR', message: '送信データサイズが大きすぎます。' }));
                        return;
                    }

                    db.get(`SELECT COUNT(*) AS count FROM cards WHERE box_id = ?`, [box_id], (err, row) => {
                        if (err) {
                            console.error('件数確認エラー:', err);
                            return;
                        }

                        if (row && row.count >= MAX_CARDS_PER_BOX) {
                            ws.send(JSON.stringify({
                                type: 'ERROR',
                                message: `このメッセージボックスは最大件数 (${MAX_CARDS_PER_BOX}件) に達しています。`
                            }));
                            return;
                        }

                        const cardId = crypto.randomUUID();
                        const createdAt = Date.now();
                        const expireTime = expires_at || (createdAt + 30 * 24 * 60 * 60 * 1000);

                        const stmt = db.prepare(`
              INSERT INTO cards (id, box_id, payload, created_at, expires_at)
              VALUES (?, ?, ?, ?, ?)
            `);

                        stmt.run(cardId, box_id, payloadStr, createdAt, expireTime, (err) => {
                            if (err) {
                                console.error('保存エラー:', err);
                                return;
                            }

                            const newCard = {
                                id: cardId,
                                payload: payload,
                                created_at: createdAt
                            };

                            ws.send(JSON.stringify({ type: 'POST_SUCCESS', card_id: cardId }));

                            wss.clients.forEach((client) => {
                                if (client.readyState === WebSocket.OPEN && client.box_id === box_id) {
                                    client.send(JSON.stringify({
                                        type: 'NEW_CARD',
                                        box_id: box_id,
                                        card: newCard
                                    }));
                                }
                            });

                            console.log(`✉️ カード保存 & ブロードキャスト完了 (box: ${box_id})`);
                        });
                        stmt.finalize();
                    });
                    break;
                }

                case 'FETCH_CARDS': {
                    const { box_id } = data;
                    if (!box_id) return;

                    ws.box_id = box_id;
                    const now = Date.now();

                    db.all(
                        `SELECT id, payload, created_at FROM cards WHERE box_id = ? AND expires_at > ? ORDER BY created_at DESC`,
                        [box_id, now],
                        (err, rows) => {
                            if (err) {
                                console.error('取得エラー:', err);
                                return;
                            }

                            const cards = rows.map(r => ({
                                id: r.id,
                                payload: JSON.parse(r.payload),
                                created_at: r.created_at
                            }));

                            ws.send(JSON.stringify({
                                type: 'CARDS_DATA',
                                box_id,
                                cards
                            }));
                        }
                    );
                    break;
                }
            }
        } catch (err) {
            console.error('メッセージ処理エラー:', err);
        }
    });
});

console.log(`🚀 リレー起動: ws://localhost:${PORT}`);