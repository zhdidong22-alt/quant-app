# app/scripts/migrate_db.py
from app.storage.db import load_config, make_conn, migrate

def main():
    cfg = load_config()
    conn = make_conn(cfg)
    migrate(conn)
    cfg = load_config()
    conn = make_conn(cfg)
    migrate()
    conn.close()
    print("DB migrate OK")

if __name__ == "__main__":
    main()
