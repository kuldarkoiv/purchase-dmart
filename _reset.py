from dotenv import load_dotenv; load_dotenv()
import os, psycopg2

conn = psycopg2.connect(
    host=os.environ.get('PG_HOST'),
    port=int(os.environ.get('PG_PORT', 5432)),
    user=os.environ.get('PG_USER'),
    password=os.environ.get('PG_PASSWORD'),
    dbname=os.environ.get('PG_DATABASE'),
    sslmode='require'
)
conn.autocommit = True
cur = conn.cursor()

# Katkesta kõik MITTE-superuser ühendused
cur.execute("""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
      AND usename NOT IN (SELECT usename FROM pg_user WHERE usesuper = true)
""")
print("Teised ühendused katkestatud")

# Kustuta skeem täielikult
cur.execute("DROP SCHEMA IF EXISTS purchase_dmart CASCADE")
print("Skeem kustutatud")

# Loo uuesti
cur.execute(open('ddl_postgres.sql', encoding='utf-8').read())
print("DDL OK")

cur.execute("SELECT current_database()")
print("DB:", cur.fetchone()[0])
conn.close()
