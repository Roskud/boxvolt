import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "users.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def init_db() -> None:
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            subscription_end TEXT,
            vless_uuid TEXT,
            trial_used INTEGER DEFAULT 0
        )
        """
    )

    if not _column_exists(conn, "users", "vless_uuid"):
        cursor.execute("ALTER TABLE users ADD COLUMN vless_uuid TEXT")
    if not _column_exists(conn, "users", "trial_used"):
        cursor.execute("ALTER TABLE users ADD COLUMN trial_used INTEGER DEFAULT 0")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT UNIQUE NOT NULL,
            telegram_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            amount_rub REAL NOT NULL,
            days INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            paid_at TEXT,
            raw_payload TEXT,
            FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments (telegram_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (status)")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialized")
