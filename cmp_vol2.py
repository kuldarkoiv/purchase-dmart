import os, pyodbc, psycopg2
from dotenv import load_dotenv
load_dotenv()

cn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['MSSQL_HOST']},{os.environ.get('MSSQL_PORT','1433')};"
    f"DATABASE={os.environ['MSSQL_DATABASE']};"
    f"UID={os.environ['MSSQL_USER']};PWD={os.environ['MSSQL_PASSWORD']};"
    f"TrustServerCertificate=yes;"
)
pg = psycopg2.connect(host=os.environ['PG_HOST'], port=int(os.environ.get('PG_PORT',5432)),
    user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
    dbname=os.environ['PG_DATABASE'], sslmode='require')

mscur = cn.cursor()

# Vaata mis volume väljad on dbo.product peal
mscur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='product' AND COLUMN_NAME LIKE '%vol%'")
print("dbo.product vol* veerud:", [r[0] for r in mscur.fetchall()])

mscur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='product_piece' AND COLUMN_NAME LIKE '%vol%'")
print("dbo.product_piece vol* veerud:", [r[0] for r in mscur.fetchall()])

# Konkreetne pakk 310099 - kõik vol väljad
mscur.execute("SELECT * FROM dbo.product WHERE id=310099")
row = mscur.fetchone()
cols = [d[0] for d in mscur.description]
print("\ndbo.product 310099 vol väljad:")
for c, v in zip(cols, row):
    if 'vol' in c.lower() and v is not None:
        print(f"  {c}: {v}")

mscur.execute("SELECT * FROM dbo.product_piece WHERE product_id=310099")
row = mscur.fetchone()
if row:
    cols = [d[0] for d in mscur.description]
    print("\ndbo.product_piece 310099 vol väljad:")
    for c, v in zip(cols, row):
        if 'vol' in c.lower() and v is not None:
            print(f"  {c}: {v}")

# PG väärtus
pgcur = pg.cursor()
pgcur.execute("SELECT actual_volume_m3 FROM purchase_dmart.purchase_material_products WHERE product_id=310099")
print(f"\nPG actual_volume_m3: {pgcur.fetchone()[0]}")

cn.close()
pg.close()
