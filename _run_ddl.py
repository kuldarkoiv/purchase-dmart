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
cur = conn.cursor()
cur.execute(open('ddl_postgres.sql', encoding='utf-8').read())
conn.commit()
cur.execute('SELECT current_database()')
print('DDL OK, DB:', cur.fetchone()[0])
conn.close()
