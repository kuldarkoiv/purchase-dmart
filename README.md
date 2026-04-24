# purchase-dmart

ETL pipeline, mis laadib ostetud materjalide andmed MSSQL-ist PostgreSQL andmekohta (data mart).

**Eesmärk:** jälgida ostetud toorme teekonda — millal osteti, kas tuli õigeaegselt kohale, kus see praegu asub — ning seostada see müügilepingutega.

---

## Andmevoog

```
MSSQL (woodpecker_dev)
  dbo.product                     # iga pakk / rull / tükk
  dbo.product_piece               # pakis olevate tükkide tegelikud pikkused
  dbo.contract_delivery           # ostutellimuse read (due kuupäev)
  dbo.purchase_waybill            # saatelehed (arrival_date)
  dbo.contract_goods / contract   # lepingud ja spetsifikatsioonid
  dbo.account                     # tarnijad, kliendid, laokohad
  dbo.processing_work             # tootmistööd
  views.*                         # täitmisandmed (meters_done, stock jms)
        |
        v etl.py  (~36s, iga tund)
PostgreSQL (purchase_dmart skeem)
  purchase_order_lines            # ostutellimuste read + täitmise seis
  purchase_material_products      # pakitaseme teekond (material_status)
  v_free_stock_by_article         # vaade: vaba laovaru
  v_overdue_orders                # vaade: ületähtaegsed tellimused
  v_supplier_delivery_performance # vaade: tarnijate tarneetäpsus
  v_material_journey              # vaade: täismaatriks
  v_material_availability         # vaade: müügi ühtne saadavuse vaade
```

---

## Failide struktuur

```
purchase-dmart/
├── etl.py                              # ETL põhiskript
├── views_and_indexes.sql               # PostgreSQL vaated + indeksid (rakendatakse ETL lõpus automaatselt)
├── Dockerfile                          # DO App Platform build (ODBC Driver 18 nõuab Dockerfile'i)
├── requirements.txt
├── .env.example
├── sql/
│   ├── extract_order_lines.sql         # MSSQL päring: ostutellimuste read (~7700 rida)
│   └── extract_received_products.sql   # MSSQL päring: materjalipakid (~110k rida)
└── example sql/                        # Originaalpäringud referentsiks
```

---

## Tabelid — andmesõnastik

### `purchase_order_lines`
Üks rida = üks ostutellimuse tarneliin. PK: `order_line_id`.

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `order_line_id` | INT | PK — MSSQL `contract_delivery.id` |
| `purchase_contract_id` | INT | Ostuleping |
| `purchase_contract_number` | TEXT | Ostulepingu number (nt `K-2024-001`) |
| `purchase_order_number` | TEXT | Tellimuse number |
| `purchase_contract_date` | DATE | Lepingu kuupäev |
| `purchase_contract_archived` | BOOL | Kas ostuleping on arhiveeritud |
| `supplier_id` | INT | Tarnija ID |
| `supplier_name` | TEXT | Tarnija nimi |
| `order_line_number` | INT | Tarnerea number lepingus |
| `order_due_date` | DATE | Lubatud tarnekuupäev |
| `order_meters` | FLOAT | Tellitud jooksvad meetrid |
| `order_finished` | BOOL | Kas real märgitud lõpetatuks |
| `article_group_id` | INT | Artikligrupp ID |
| `article_group` | TEXT | Artikligrupp (nt `RWT 32x125`) |
| `article_name` | TEXT | Artikkel |
| `species_name` | TEXT | Liik (nt `RWT`, `WW`) |
| `height` / `width` / `length` | FLOAT | Nominaalmõõdud mm |
| `length_min` | FLOAT | Min pikkus (range-tellimusel) |
| `ordered_packs` | INT | Tellitud pakkide arv |
| `ordered_volume` | FLOAT | Tellitud m3 |
| `received_packs` | INT | Vastu võetud pakke |
| `received_volume` | FLOAT | Vastu võetud m3 |
| `stock_packs` | INT | Hetkel laos (meie enda ladu) |
| `stock_volume` | FLOAT | Laos m3 |
| `shipped_packs` | INT | Ekspedeeritud pakke |
| `pending_packs` | INT | Veel ootel (tellitud − vastu võetud) |
| `pending_meters` | FLOAT | Ootel meetrid |
| `pending_volume` | FLOAT | Ootel m3 |
| `fulfillment_pct` | FLOAT | Täitmise % (received/ordered) |
| `is_overdue` | INT | 1 kui tähtaeg möödunud ja pole täidetud |
| `is_finished` | BOOL | Kas real lõpetatud |
| `is_active` | INT | 1 kui aktiivne (pole arhiveeritud ega lõpetatud) |
| `days_since_due` | INT | Hilinemise päevad (positiivne = hilines) |
| `days_first_arrival_vs_due` | INT | Esimese saatuse hilinemine vs due |
| `order_comment` | TEXT | Planeerija kommentaar real |
| `marking_comments` | TEXT | Broneeringu märkus (kellele materjal mõeldud) |
| `price_m3` | FLOAT | Hind €/m3 ostutellimuse realt |

---

### `purchase_material_products`
Üks rida = üks füüsiline pakk × üks tegelik pikkus.
PK: `(product_id, actual_length_mm)` — mixed pakil (mitu pikkust) tekib mitu rida.

#### Identifikaatorid

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `product_id` | INT | PK osa — MSSQL `product.id` |
| `product_label` | TEXT | Füüsiline märgis pakil (number1) |
| `product_number2` | TEXT | Teine märgis |

#### Ostutellimuse seos

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `order_line_id` | INT | FK → `purchase_order_lines` |
| `order_line_number` | INT | Tarnerea number |
| `order_due_date` | DATE | Ostutellimuse tähtaeg |
| `order_meters` | FLOAT | Tellitud meetrid real |
| `order_finished` | BOOL | Kas real lõpetatud |
| `price_m3` | FLOAT | Hind €/m3 |
| `purchase_contract_id` | INT | Ostuleping ID |
| `purchase_contract_number` | TEXT | Ostulepingu number |
| `purchase_order_number` | TEXT | Tellimuse number |
| `purchase_contract_date` | DATE | Lepingu kuupäev |
| `purchase_contract_archived` | BOOL | Kas ostuleping on arhiveeritud |

#### Tarnija

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `supplier_id` | INT | Tarnija ID |
| `supplier_name` | TEXT | Tarnija nimi |

#### Spetsifikatsioon (nominaalmõõdud lepingust)

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `spec_height` | FLOAT | Kõrgus mm (lepingust) |
| `spec_width` | FLOAT | Laius mm (lepingust) |
| `spec_length_min` | FLOAT | Min pikkus mm (range-lepingutel) |
| `spec_length` | FLOAT | Pikkus mm (lepingust) |
| `actual_height` | FLOAT | Tegelik kõrgus mm (mõõdetud pakil) |
| `actual_width` | FLOAT | Tegelik laius mm (mõõdetud pakil) |
| `actual_length_mm` | INT | Tegelik pikkus mm (product_piece-st) |
| `actual_volume_m3` | FLOAT | Selle pikkuse m3 (product_piece-st) |
| `is_mixed_length` | INT | 1 kui pakis on mitu eri pikkust |
| `species_name` | TEXT | Puuliik (nt `RWT`, `WW`, `RW`) |
| `grade_name` | TEXT | Kvaliteediklass |
| `treatment_name` | TEXT | Töötlus |
| `cert_name` | TEXT | Sertifikaat (PEFC jms) |

#### Saateleht

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `waybill_id` | INT | Saatelehe ID |
| `waybill_number` | TEXT | Saatelehe number |
| `waybill_seller_number` | TEXT | Tarnija saatelehe number |
| `waybill_sent_date` | DATE | Saatelehe kuupäev |
| `waybill_arrival_date` | DATE | Tegelik saabumiskuupäev (põhiline) |
| `effective_arrival_date` | DATE | `COALESCE(arrival_date, sent_date)` |

#### Tähtaegsus

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `days_late` | INT | Hilinemine päevades (positiivne = hilines) |
| `is_on_time` | INT | 1 kui saabus tähtajaks, 0 kui hilines, NULL kui pole saabunud |

#### Staatus

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `material_status` | TEXT | **Peamine staatus** — vt alltoodud loetelu |
| `is_reserved_bron` | BOOL | Kasutaja poolt kinni pandud (bron) |
| `is_wrote_off` | BOOL | Maha kantud |
| `is_consumed` | INT | 1 kui tarbitud tootmises |
| `consumed_at` | TIMESTAMP | Millal tarbiti |
| `is_shipped` | INT | 1 kui `shipment_id` on täidetud |
| `is_allocated` | INT | 1 kui seotud müügilepinguga või bron=1 |
| `is_done` | BOOL | Lõpetatud |
| `is_finished_good` | BOOL | Valmistoode (ei peaks ostetud materjalil esinema) |

#### `material_status` väärtused (prioriteedi järjekorras)

| Väärtus | Tingimus |
|---|---|
| `maha_kantud` | `wrote_off = 1` |
| `tarbitud` | `actual_used IS NOT NULL` |
| `ekspedeeritud` | `shipment_id IS NOT NULL` VÕI pakk pole meie enda laos (`stock_account_id != 1`) VÕI orphan pakk (saatelehega aga ilma ostulepinguta) |
| `reserveeritud` | `contract_delivery_id IS NOT NULL` VÕI `bron = 1` VÕI `contract_id IS NOT NULL` (müügileping ilma delivery line'ita) |
| `tootmises` | `processing_work_id_in IS NOT NULL` |
| `vaba` | kõik muu |

#### Tootmistöö

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `processing_work_received` | INT | Tootmistöö ID, kuhu pakk sisendina lisati |
| `processing_step_used` | INT | Töösamba ID |
| `work_in_number` | INT | Tootmistöö kuvanumber (nt 549) |
| `work_in_status` | TEXT | Tootmistöö staatus (`Planned`, `Started`, `Finished`, `Standby`, `Ready`) |
| `work_in_archived` | BOOL | Kas tootmistöö on arhiveeritud |

#### Müügilepingu seos

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `sales_contract_id` | INT | Müügileping ID |
| `sales_contract_number` | TEXT | Müügilepingu number |
| `sales_order_number` | TEXT | Müügitellimuse number |
| `customer_id` | INT | Klient ID |
| `customer_name` | TEXT | Kliendi nimi |
| `sales_delivery_id` | INT | Müügi tarneliin ID |
| `sales_delivery_due` | DATE | Millal kliendile vaja |
| `sales_delivery_number` | INT | Müügi tarnerea number |

#### Lasukoht

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `stock_account_id` | INT | Laokonto ID (`1` = meie enda ladu Natural) |
| `stock_name` | TEXT | Laokonto nimi (nt `OÜ Thermoarena`, `BLRT sadam`) |

#### Kommentaarid ja audit

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `product_comment` | TEXT | Planeerija märkus pakil |
| `product_created_at` | TIMESTAMP | Loodud |
| `product_modified_at` | TIMESTAMP | Viimati muudetud |
| `purchase_invoice_id` | INT | Ostuarve ID (NULL = arveldamata) |
| `purchase_invoice_row_id` | INT | Ostuarve rea ID |

---

### Vaated

| Vaade | Kirjeldus |
|---|---|
| `v_free_stock_by_article` | Vaba laovaru (`material_status='vaba'`) liigi/kvaliteedi/mõõtude kaupa. Sisaldab `marking_comments` ja `planner_comments`. |
| `v_overdue_orders` | Ületähtaegsed ostutellimused (`is_overdue=1`), sorteeritud hilinemise järgi. |
| `v_supplier_delivery_performance` | Tarnijate tarneetäpsus: on-time %, keskmine hilinemine, max hilinemine. |
| `v_material_journey` | Täismaatriks — iga pakk koos ostutellimuse, saatelehe, müügilepingu ja tootmistöö infoga. |
| `v_material_availability` | **Müügi ühtne saadavuse vaade.** Segmendid: `vaba_ladu` (laos, `material_status='vaba'`, `work_in_status IS NULL`) ja `tulemas` (tellitud, pole saabunud). Mõõdud: `COALESCE(spec, actual)` — toimib ka orphan pakkidel. m3 arvutus: `SUM(actual_volume_m3)`. Sorteeritud liik/kvaliteet/mõõt/pikkus järgi. |

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

`.env.example` sisu:

```
MSSQL_HOST=
MSSQL_PORT=1433
MSSQL_DATABASE=woodpecker_dev
MSSQL_USER=
MSSQL_PASSWORD=
MSSQL_DRIVER=ODBC Driver 18 for SQL Server

PG_HOST=
PG_PORT=5432
PG_USER=
PG_PASSWORD=
PG_DATABASE=
```

### 3. Käivita ETL

```bash
# Kõik tabelid (+ vaated ja indeksid automaatselt)
python etl.py

# Ainult ostutellimused
python etl.py --table orders

# Ainult materjalipakid
python etl.py --table products

# Dry-run (ei kirjuta PostgreSQL-i)
python etl.py --dry-run
```

ETL kestus: ~36 sekundit (MSSQL lugemine ~8s + PG kirjutamine ~28s, 110k rida).

---

## DigitalOcean deploy

### Eeldused
- DO Managed PostgreSQL klaster (sslmode=require)
- DO App Platform Job (Dockerfile)
- **Build Settings → vali Dockerfile** (mitte buildpack — ODBC Driver vajab Dockerfile'i)

### Cron

```
0 * * * *   # iga tund
```

Env muutujad seata DO UI-s App → Settings → Environment Variables.