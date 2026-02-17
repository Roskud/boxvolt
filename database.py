import sqlite3

def init_db():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        username TEXT,
        subscription_end TEXT,
        trial_used INTEGER DEFAULT 0,
        plan_type TEXT DEFAULT 'none'
    )
    """)

    conn.commit()
    conn.close()
