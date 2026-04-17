"""
etl.py - Ostetud materjalide dmart ETL
MSSQL woodpecker_dev -> PostgreSQL purchase_dmart

Kasutus:
    python etl.py                  # täislaadus (mõlemad tabelid)
    python etl.py --table orders   # ainult purchase_order_lines
    python etl.py --table products # ainult purchase_material_products
    python etl.py --dry-run        # ainult loeb, ei kirjuta

Käivitab:
    sql/extract_order_lines.sql      -> purchase_dmart.purchase_order_lines
    sql/extract_received_products.sql -> purchase_dmart.purchase_material_products
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

try:
    import pyodbc
    import psycopg2
    import psycopg2.extras
except ImportError as e:
    print(f"Puuduv moodul: {e}")
    print("Jooksuta: pip install pyodbc psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv pole kohustuslik, env peab olema juba seatud

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl")

SQL_DIR = Path(__file__).parent / "sql"


# ---------------------------------------------------------------------------
# Ühendused
# ---------------------------------------------------------------------------

def connect_mssql() -> pyodbc.Connection:
    dsn = (
        f"DRIVER={{{os.environ.get('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={os.environ.get('MSSQL_HOST', 'localhost')},{os.environ.get('MSSQL_PORT', '1433')};"
        f"DATABASE={os.environ.get('MSSQL_DATABASE', 'woodpecker_dev')};"
        f"UID={os.environ.get('MSSQL_USER', '')};"
        f"PWD={os.environ.get('MSSQL_PASSWORD', '')};"
        f"TrustServerCertificate={os.environ.get('MSSQL_TRUST_SERVER_CERTIFICATE', 'yes')};"
    )
    try:
        conn = pyodbc.connect(dsn)
        conn.timeout = 120
        return conn
    except pyodbc.Error:
        log.error("MSSQL ühendus ebaõnnestus. Kontrolli .env muutujaid (MSSQL_*).")
        raise SystemExit(1)


def connect_postgres() -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(
            host=os.environ.get('PG_HOST', 'localhost'),
            port=int(os.environ.get('PG_PORT', 5432)),
            user=os.environ.get('PG_USER', 'user'),
            password=os.environ.get('PG_PASSWORD', ''),
            dbname=os.environ.get('PG_DATABASE', 'defaultdb'),
            sslmode='require',
            connect_timeout=30,
        )
        return conn
    except psycopg2.OperationalError:
        log.error("PostgreSQL ühendus ebaõnnestus. Kontrolli .env muutujaid (PG_*).")
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# SQL lugemine
# ---------------------------------------------------------------------------

def read_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.exists():
        log.error(f"SQL fail ei leitud: {path}")
        raise SystemExit(1)
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Üldine ETL helper
# ---------------------------------------------------------------------------

def etl_table(
    ms_conn: pyodbc.Connection,
    pg_conn: psycopg2.extensions.connection,
    sql_file: str,
    pg_schema: str,
    pg_table: str,
    pk_column: str,
    dry_run: bool = False,
) -> int:
    """
    Loeb MSSQL-ist, kirjutab PostgreSQL-i (TRUNCATE + INSERT).
    Tagastab kirjutatud ridade arvu.
    """
    target = f"{pg_schema}.{pg_table}"
    log.info(f"[{pg_table}] Loen MSSQL-ist ({sql_file})...")

    sql = read_sql(sql_file)
    ms_cur = ms_conn.cursor()
    ms_cur.execute(sql)
    columns = [col[0].lower() for col in ms_cur.description]
    rows = ms_cur.fetchall()
    ms_cur.close()

    log.info(f"[{pg_table}] MSSQL andis {len(rows)} rida, {len(columns)} veergu.")

    if dry_run:
        log.info(f"[{pg_table}] --dry-run: PostgreSQL-i ei kirjuta.")
        return len(rows)

    if not rows:
        log.warning(f"[{pg_table}] Andmed puuduvad, jätan vahele.")
        return 0

    pg_cur = pg_conn.cursor()

    # TRUNCATE (asendame kogu tabeli)
    log.info(f"[{pg_table}] TRUNCATE {target}...")
    pg_cur.execute(f"TRUNCATE TABLE {target}")

    # Koosta INSERT lause
    cols_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {target} ({cols_sql}) VALUES ({placeholders})"

    # Batch insert (1000 rida korraga)
    batch_size = 1000
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = [tuple(row) for row in rows[i: i + batch_size]]
        pg_cur.executemany(insert_sql, batch)
        total_inserted += len(batch)
        if total_inserted % 10000 == 0:
            log.info(f"[{pg_table}]   {total_inserted}/{len(rows)} sisestatud...")

    # Uuenda etl_loaded_at
    pg_cur.execute(f"UPDATE {target} SET etl_loaded_at = NOW()")

    pg_conn.commit()
    pg_cur.close()

    log.info(f"[{pg_table}] Kõik {total_inserted} rida PostgreSQL-i kirjutatud.")
    return total_inserted


# ---------------------------------------------------------------------------
# Põhiprogramm
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Purchase dmart ETL: MSSQL -> PostgreSQL")
    parser.add_argument(
        "--table",
        choices=["orders", "products", "all"],
        default="all",
        help="Milline tabel laadida (vaikimisi: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Loe andmed aga ära kirjuta PostgreSQL-i",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Purchase dmart ETL käivitub")
    log.info(f"  Tabel(id): {args.table}  |  dry-run: {args.dry_run}")
    log.info("=" * 60)

    ms_conn = connect_mssql()
    log.info("MSSQL ühendus OK")

    pg_conn = None
    if not args.dry_run:
        pg_conn = connect_postgres()
        log.info("PostgreSQL ühendus OK")

    start = datetime.now()

    try:
        if args.table in ("orders", "all"):
            etl_table(
                ms_conn, pg_conn,
                sql_file="extract_order_lines.sql",
                pg_schema="purchase_dmart",
                pg_table="purchase_order_lines",
                pk_column="order_line_id",
                dry_run=args.dry_run,
            )

        if args.table in ("products", "all"):
            etl_table(
                ms_conn, pg_conn,
                sql_file="extract_received_products.sql",
                pg_schema="purchase_dmart",
                pg_table="purchase_material_products",
                pk_column="product_id",
                dry_run=args.dry_run,
            )

    except Exception:
        log.exception("ETL ebaõnnestus")
        if pg_conn:
            pg_conn.rollback()
        raise SystemExit(1)
    finally:
        ms_conn.close()
        if pg_conn:
            pg_conn.close()

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"ETL valmis. Kokku aeg: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
