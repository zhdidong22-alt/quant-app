from app.storage.db import load_config, make_conn, migrate

def main():
    # 1. 加载配置
    cfg = load_config()

    # 2. 建立数据库连接（用于连通性验证）
    conn = make_conn(cfg)

    # 3. 执行数据库迁移（migrate 自己处理连接）
    migrate()

    # 4. 关闭连接
    conn.close()

    print("DB migrate OK")

if __name__ == "__main__":
    main()
