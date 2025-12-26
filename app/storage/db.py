import os
import yaml
import psycopg2

def _find_cfg_path() -> str:
    # Keep your existing behavior: prefer app/config/local.yaml first
    # (If you later add ./config/local.yaml, you can extend search here.)
    here = os.path.dirname(__file__)
    app_dir = os.path.abspath(os.path.join(here, ".."))
    cand = os.path.join(app_dir, "config", "local.yaml")
    if os.path.exists(cand):
        return cand
    # fallback to project-root/config/local.yaml if exists
    proj = os.path.abspath(os.path.join(app_dir, ".."))
    cand2 = os.path.join(proj, "config", "local.yaml")
    if os.path.exists(cand2):
        return cand2
    raise FileNotFoundError(f"local.yaml not found. tried: {cand} , {cand2}")

def load_config() -> dict:
    """Load full config yaml (database + data + future modules)."""
    cfg_path = _find_cfg_path()
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        raise TypeError(f"local.yaml must be a mapping/dict, got {type(cfg)}")
    return cfg

def get_db_cfg(cfg: dict) -> dict:
    """Extract db config; supports either cfg['database'] or cfg['db'] or already-db-dict."""
    if not isinstance(cfg, dict):
        raise TypeError(f"cfg must be dict, got {type(cfg)}")

    # if already looks like db cfg
    if set(("host", "port", "user", "password", "database")).issubset(set(cfg.keys())):
        return cfg

    if "database" in cfg and isinstance(cfg["database"], dict):
        return cfg["database"]
    if "db" in cfg and isinstance(cfg["db"], dict):
        return cfg["db"]

    raise KeyError(f"config missing 'database' or 'db'. keys={list(cfg.keys())}")

def make_conn(cfg: dict):
    db = get_db_cfg(cfg)
    return psycopg2.connect(
        host=db.get("host", "127.0.0.1"),
        port=int(db.get("port", 5432)),
        user=db.get("user", "postgres"),
        password=db.get("password", "postgres"),
        dbname=db.get("database", "quant"),
        application_name="quant-app",
    )

def migrate():
    cfg = load_config()
    conn = make_conn(cfg)
    try:
        here = os.path.dirname(__file__)
        schema_path = os.path.join(here, "schema.sql")
        with open(schema_path, "r", encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()