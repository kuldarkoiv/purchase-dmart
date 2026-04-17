# purchase-dmart

ETL pipeline, mis laadib ostetud materjalide andmed MSSQL-ist PostgreSQL andmekohta (data mart).

Eesmärk: jälgida ostetud toorme **teekonda** — millal osteti, kas tuli õigeaegselt kohale, kus see praegu asub (vaba laos, reserveeritud, tarbitud tootmises, ekspedeeritud kliendile, maha kantud) — ning seostada see müügilepingutega.

---

## Andmevoog

```
MSSQL (woodpecker_dev)
  └── dbo.product                     # iga pakk / rull / tükk
  └── dbo.contract_delivery           # ostutellimuse read (due kuupäev)
  └── dbo.purchase_waybill            # saatelehed (arrival_date)
  └── dbo.contract_goods / contract   # lepingud ja spetsifikatsioonid
  └── views.*                         # täitmisandmed (meters_done, stock jms)
        │
        ▼ etl.py
PostgreSQL (purchase_dmart skeem)
  └── purchase_order_lines            # ostutellimuste read + täitmise seis
  └── purchase_material_products      # pakitaseme teekond (material_status)
  └── v_free_stock_by_article         # vaade: vaba laovaru
  └── v_overdue_orders                # vaade: ületähtaegsed tellimused
  └── v_supplier_delivery_performance # vaade: tarnijate tarneetäpsus
  └── v_material_journey              # vaade: täismaatriks
```

---

## Tabelid ja vaated

### `purchase_order_lines`
Üks rida = üks ostutellimuse tarneliin.

| Veerg | Kirjeldus |
|---|---|
| `order_line_id` | Primaarvõti (= MSSQL `contract_delivery.id`) |
| `order_due_date` | Lubatud tarnekuupäev |
| `ordered_meters / packs` | Tellitud kogus |
| `received_*` | Vastu võetud kogus |
| `stock_*` | Hetkel laos |
| `shipped_*` | Ekspedeeritud |
| `pending_*` | Veel ootel (pole kohale jõudnud) |
| `fulfillment_pct` | Täitmise % |
| `is_overdue` | `true` kui tähtaeg möödunud aga pole täidetud |
| `days_since_due` | Positiivne = hilinenud päevade arv |
| `days_first_arrival_vs_due` | Esimese pakki hilinemine vs due (positiivne = hilines) |
| `first_arrival_date` | Millal esimene pakk tegelikult saabus |
| `order_comment` | Planeerija kommentaar real |

### `purchase_material_products`
Üks rida = üks füüsiline pakk (toote number).

| Veerg | Kirjeldus |
|---|---|
| `product_id` | Primaarvõti (= MSSQL `product.id`) |
| `product_label` | Füüsiline märgis pakil (`number1`) |
| `material_status` | **Peamine väli:** `vaba` \| `reserveeritud` \| `tarbitud` \| `ekspedeeritud` \| `maha_kantud` |
| `order_due_date` | Ostutellimuse tähtaeg |
| `effective_arrival_date` | Tegelik saabumiskuupäev (`arrival_date` või `sentdate`) |
| `days_late` | Positiivne = hilines, negatiivne = tuli varakult |
| `is_on_time` | `true` kui saabus tähtajaks |
| `consumed_at` | Millal tarbiti tootmises (`actual_used`) |
| `is_allocated` | Seotud müügilepinguga |
| `sales_contract_number` | Mis müügilepingusse reserveeritud |
| `customer_name` | Klient |
| `product_comment` | Planeerija märkus pakil |

### Vaated

| Vaade | Eesmärk |
|---|---|
| `v_free_stock_by_article` | Vaba laovaru artiklite kaupa (koos planeerija kommentaaridega) |
| `v_overdue_orders` | Ületähtaegsed ostutellimused, sorteeritud hilinemise järgi |
| `v_supplier_delivery_performance` | Tarnijate on-time %, keskmised hilinemised |
| `v_material_journey` | Täielik materjali teekond ühes vaates |

---

## Seadistamine

### 1. Klooni ja installi

```bash
git clone https://github.com/kuldarkoiv/purchase-dmart.git
cd purchase-dmart
pip install -r requirements.txt
```

### 2. Keskkonna muutujad

```bash
cp .env.example .env
# täida .env oma väärtustega
```

Vajalikud muutujad (vt `.env.example`):

```
# MSSQL allikas
MSSQL_HOST=
MSSQL_PORT=1433
MSSQL_DATABASE=woodpecker_dev
MSSQL_USER=
MSSQL_PASSWORD=
MSSQL_DRIVER=ODBC Driver 18 for SQL Server
MSSQL_TRUST_SERVER_CERTIFICATE=yes

# PostgreSQL sihtkoht
PG_HOST=
PG_PORT=5432
PG_USER=
PG_PASSWORD=
PG_DATABASE=
```

### 3. Loo PostgreSQL skeem

```bash
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f ddl_postgres.sql
```

### 4. Käivita ETL

```bash
# Kõik tabelid
python etl.py

# Ainult ostutellimuste read
python etl.py --table orders

# Ainult materjalipakid
python etl.py --table products

# Testimiseks (ei kirjuta PostgreSQL-i)
python etl.py --dry-run
```

---

## DigitalOcean deploy

### Eeldused
- DO Managed PostgreSQL klaster (sslmode=require)
- DO Droplet või App Platform Python worker
- ODBC Driver 18 for SQL Server installitud (`apt install msodbcsql18`)

### Cron (igapäevane ETL)

```cron
0 4 * * * cd /opt/purchase-dmart && python etl.py >> /var/log/purchase-dmart-etl.log 2>&1
```

### Env muutujad DO-s
Sea kõik `.env.example` muutujad App Platform keskkonna muutujatena või Dropletil `/opt/purchase-dmart/.env` failina.

---

## Failide struktuur

```
purchase-dmart/
├── etl.py                              # ETL põhiskript
├── ddl_postgres.sql                    # PostgreSQL DDL (skeem, tabelid, vaated)
├── requirements.txt
├── .env.example
├── sql/
│   ├── extract_order_lines.sql         # MSSQL päring: ostutellimuste read
│   └── extract_received_products.sql   # MSSQL päring: materjalipakid
└── example sql/                        # Originaalpäringud (referents)
    ├── contract_delivery_list_filled_purchase.sql
    └── contract_goods_report_purchase.sql
```

---

## Materjali staatuse loogika

```
                    ┌─────────────────────────────────┐
                    │         OSTETUD MATERJAL         │
                    └──────────────┬──────────────────┘
                                   │ purchase_waybill.arrival_date
                                   ▼
                    ┌─────────────────────────────────┐
                    │    VASTU VÕETUD (laos olemas)    │
                    └──────────────┬──────────────────┘
               ┌───────────────────┼────────────────────────┐
               ▼                   ▼                         ▼
        ┌──────────┐        ┌────────────┐          ┌──────────────┐
        │  VABA    │        │RESERVEERITUD│          │ MAHA KANTUD  │
        │  (laos)  │        │(müügilepingus)│        │(wrote_off=1) │
        └──────────┘        └──────┬─────┘          └──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
             ┌──────────┐                ┌─────────────┐
             │ TARBITUD  │               │EKSPEDEERITUD│
             │(tootmises)│               │ (kliendile) │
             └──────────┘                └─────────────┘
```
