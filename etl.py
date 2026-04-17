"""
etl.py - Purchase dmart ETL: MSSQL -> PostgreSQL
Kasutus:
    python etl.py                  # mõlemad tabelid
    python etl.py --table orders
    python etl.py --table products
    python etl.py --dry-run
"""
import os, sys, argparse, logging
from pathlib import Path
from datetime import datetime

try:
    import pyodbc
    import psycopg2, psycopg2.extras
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print(f"Puuduv moodul: {e}\nJooksuta: pip install -r requirements.txt")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("etl")
SQL_DIR = Path(__file__).parent / "sql"

PYODBC_TO_PG = {
    int: "BIGINT", float: "DOUBLE PRECISION", bool: "BOOLEAN",
    str: "TEXT", bytes: "BYTEA", datetime: "TIMESTAMP",
}

def ms_connect():
    dsn = (
        f"DRIVER={{{os.environ.get('MSSQL_DRIVER','ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={os.environ['MSSQL_HOST']},{os.environ.get('MSSQL_PORT','1433')};"
        f"DATABASE={os.environ.get('MSSQL_DATABASE','woodpecker_dev')};"
        f"UID={os.environ['MSSQL_USER']};PWD={os.environ['MSSQL_PASSWORD']};"
        f"TrustServerCertificate=yes;"
    )
    try:
        return pyodbc.connect(dsn)
    except Exception as e:
        log.error("MSSQL ühendus ebaõnnestus: %s", e); raise SystemExit(1)

def pg_connect():
    try:
        return psycopg2.connect(
            host=os.environ["PG_HOST"], port=int(os.environ.get("PG_PORT",5432)),
            user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"],
            dbname=os.environ["PG_DATABASE"], sslmode="require", connect_timeout=30,
        )
    except Exception:
        log.error("PostgreSQL ühendus ebaõnnestus"); raise SystemExit(1)

def infer_pg_type(pyodbc_type_code, sample_value):
    import decimal
    if sample_value is None:
        return "TEXT"
    t = type(sample_value)
    if t == bool: return "BOOLEAN"
    if t == int: return "BIGINT"
    if t == float: return "DOUBLE PRECISION"
    if t == decimal.Decimal: return "NUMERIC"
    if t == datetime: return "TIMESTAMP"
    if t == bytes: return "BYTEA"
    return "TEXT"

def etl_table(ms, pg, sql_file, schema, table, pk_col, dry_run=False):
    log.info(f"[{table}] Loen MSSQL-ist ({sql_file})...")
    sql = (SQL_DIR / sql_file).read_text(encoding="utf-8")
    cur = ms.cursor()
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    log.info(f"[{table}] {len(rows)} rida, {len(cols)} veergu.")
    if dry_run:
        log.info(f"[{table}] dry-run, ei kirjuta."); return

    # Tuvasta tüübid esimese rea põhjal
    sample = rows[0] if rows else [None]*len(cols)
    pg_types = [infer_pg_type(None, v) for v in sample]

    pg_cur = pg.cursor()
    pg_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # DROP + CREATE (view'd taastatakse ETL lõpus views_and_indexes.sql-ist)
    col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(cols, pg_types))
    pg_cur.execute(f'DROP TABLE IF EXISTS {schema}.{table} CASCADE')
    pg_cur.execute(f'CREATE TABLE {schema}.{table} ({col_defs}, etl_loaded_at TIMESTAMP, PRIMARY KEY ("{pk_col}"))')

    log.info(f"[{table}] Kirjutan {len(rows)} rida...")
    cols_sql = ", ".join(f'"{c}"' for c in cols) + ', "etl_loaded_at"'
    now = datetime.now()
    data = [tuple(row) + (now,) for row in rows]
    insert_sql = f'INSERT INTO {schema}.{table} ({cols_sql}) VALUES %s'

    psycopg2.extras.execute_values(pg_cur, insert_sql, data, page_size=2000)
    pg_cur.execute(f"GRANT SELECT ON {schema}.{table} TO doadmin")
    pg.commit()
    pg_cur.close()
    log.info(f"[{table}] Valmis.")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", choices=["orders","products","all"], default="all")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    log.info("="*60); log.info(f"ETL | {args.table} | dry-run={args.dry_run}"); log.info("="*60)
    t0 = datetime.now()
    ms = ms_connect(); log.info("MSSQL OK")
    pg = None if args.dry_run else pg_connect()
    if pg: log.info("PostgreSQL OK")
    try:
        if args.table in ("orders","all"):
            etl_table(ms, pg, "extract_order_lines.sql", "purchase_dmart", "purchase_order_lines", "order_line_id", args.dry_run)
        if args.table in ("products","all"):
            etl_table(ms, pg, "extract_received_products.sql", "purchase_dmart", "purchase_material_products", "product_id", args.dry_run)
        if not args.dry_run and pg:
            log.info("Taastan views ja indeksid...")
            views_sql = (Path(__file__).parent / "views_and_indexes.sql").read_text(encoding="utf-8")
            pg_cur = pg.cursor()
            pg_cur.execute(views_sql)
            pg.commit()
            pg_cur.close()
            log.info("Views OK.")
    except Exception:
        log.exception("ETL ebaõnnestus")
        if pg: pg.rollback()
        raise SystemExit(1)
    finally:
        ms.close()
        if pg: pg.close()
    log.info(f"Kõik valmis. Aeg: {(datetime.now()-t0).total_seconds():.1f}s")

if __name__ == "__main__":
    main()