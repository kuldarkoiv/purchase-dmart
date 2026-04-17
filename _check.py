import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
try:
    conn = psycopg2.connect(
        host=os.environ['PG_HOST'], port=int(os.environ.get('PG_PORT', 5432)),
        user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
        dbname=os.environ['PG_DATABASE'], sslmode='require'
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='purchase_dmart' ORDER BY table_name")
    tables = [r[0] for r in cur.fetchall()]
    print("Tabelid:", tables)
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM purchase_dmart.{t}")
        print(f"  {t}: {cur.fetchone()[0]} rida")
    cur.close(); conn.close()
except Exception as e:
    print("Viga:", e)
