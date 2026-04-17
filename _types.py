from dotenv import load_dotenv; load_dotenv()
import os, psycopg2
conn = psycopg2.connect(host=os.environ['PG_HOST'], port=int(os.environ.get('PG_PORT',5432)),
    user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
    dbname=os.environ['PG_DATABASE'], sslmode='require')
cur = conn.cursor()
cur.execute("""
    SELECT table_name, column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema='purchase_dmart' 
    AND column_name IN ('is_overdue','is_on_time','is_reserved_bron','is_allocated','is_consumed','is_shipped','is_wrote_off')
    ORDER BY table_name, column_name
""")
for r in cur.fetchall(): print(r)
conn.close()
