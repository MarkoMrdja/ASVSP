# Analiza kvaliteta vazduha u SAD

- Projektni zadatak iz predmeta Arhitekture sistema velikih skupova podataka
- Cilj projekta je da se za skup podataka o satnim mjerenjima šest zagađivača na stanicama EPA-e (US Environmental Protection Agency), koji datira od 2017. do 2025. godine, pruže adekvatni odgovori na pitanja o trendovima zagađenja vazduha u Sjedinjenim Američkim Državama, kao i da se uz analizu podataka prikupljenih u realnom vremenu sa Google Air Quality i Open-Meteo API-ja, omogući praćenje trenutnog stanja kvaliteta vazduha u ~80 američkih gradova

---

## Paketna obrada

- Svrha paketne obrade istorijskih podataka je da se stekne uvid u višegodišnje trendove zagađenja vazduha po saveznim državama, sa fokusom na PM2.5, uz analizu sezonskih obrazaca, COVID uticaja i dugoročnog napretka u smanjenju zagađenja
- Skup podataka je preuzet sa portala EPA AQS (Air Quality System) i sadrži satna mjerenja za šest polutanata: O3, PM2.5, PM10, NO2, SO2 i CO
- Podaci pokrivaju period od 2017. do 2025. godine i mogu se preuzeti sa sljedećeg linka: [EPA AQS Hourly Data](https://aqs.epa.gov/aqsweb/airdata/)

### Obrada podataka

Na osnovu prethodno opisanog skupa podataka, paketna obrada teži da odgovori na sljedeća pitanja:

1. Top 10 najzagađenijih država po godišnjem prosjeku PM2.5, sa promjenom ranga u odnosu na prethodnu godinu
2. Najzagađeniji mjesec po državi i godini, i koji se mjesec najčešće javlja kao najzagađeniji u devetogodišnjem periodu
3. Top 10 država s najviše uzastopnih prekoračenja NAAQS standarda PM2.5 (35 µg/m³)
4. Procentualna promjena prosječne koncentracije PM2.5 u odnosu na prethodni mjesec, po državi
5. Top 10 država s najvećim poboljšanjem i top 10 s najvećim pogoršanjem PM2.5 u poređenju s istim mjesecom prethodne godine
6. Datum s najvišim 30-dnevnim pokretnim prosjekom PM2.5 po državi i godini
7. Klasifikacija dnevnih mjerenja PM2.5 u kvartile po državi i godini
8. Top 15 država prema procentualnom smanjenju PM2.5 tokom marta–maja 2020. u odnosu na isti period 2019. (COVID efekat)
9. Top 10 država s najvećim i top 10 s najmanjim vikend efektom PM2.5 (omjer vikend/radni dan)
10. Top 15 država s najdužim nizom uzastopnih mjeseci opadanja PM2.5

---

## Obrada u realnom vremenu

- Obrada podataka u realnom vremenu omogućava kontinuirano praćenje kvaliteta vazduha u ~80 američkih gradova uz poređenje s istorijskim podacima
- Podaci se prikupljaju s dva javna API-ja: Google Air Quality (AQI i koncentracije polutanata) i Open-Meteo (temperatura, vlažnost, vjetar, pritisak, padavine)
- Simulira se tok iz prikupljenih podataka uz pomoć Apache Kafke i Spark Streaming-a

### Obrada podataka

Obrada u realnom vremenu teži da odgovori na sljedeća pitanja:

1. Poređenje trenutne koncentracije PM2.5 s istorijskom koncentracijom po gradu (spoj stream–batch)
2. Praćenje dominantnog polutanta i promjena trenda u kliznom prozoru od 30 minuta
3. Top 5 gradova s najvišim prosječnim PM2.5 u prozoru od 1 sat
4. Ventilacioni koeficijent — korelacija brzine vjetra i temperature po gradu u prozoru od 1 sat
5. Detekcija anomalija u rangiranju saveznih država po AQI-u u prozoru od 1 sat

---

## Dijagram arhitekture rješenja

<!-- TODO: Dijagram -->

---

## Tehnički stek

| Komponenta | Tehnologija |
|---|---|
| Procesor podataka | Apache Spark 3.0 |
| Skladište podataka | Apache Hive 2.3 + Hadoop 3.2 (HDFS) |
| Poruke | Apache Kafka (2 brokera) |
| Orkestracija | Apache Airflow |
| Baza vizualizacije | PostgreSQL |
| SQL UI | Hue |
| Vizualizacija | Metabase |
| Kontejnerizacija | Docker Compose |
| Jezik | Python 3 |

---

## Pokretanje

### Preduslovi

- Docker i Docker Compose
- Python 3.8+
- ~50 GB slobodnog prostora na disku (za pune podatke)

### 1. Kloniranje repozitorijuma i postavljanje okruženja

```bash
git clone <url-repozitorijuma>
cd ASVSP
cp .env.example .env
# Popuniti GOOGLE_AIR_QUALITY_API_KEY u .env fajlu
```

### 2. Pokretanje servisa

```bash
docker-compose up -d
```

| Servis | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| HDFS NameNode UI | http://localhost:9870 |
| Hue (SQL editor) | http://localhost:8888 |
| YARN Resource Manager | http://localhost:8088 |
| Metabase (dashboardi) | http://localhost:3000 |
| Airflow | http://localhost:8081 |
| PostgreSQL | localhost:5432 |
| Kafka broker 1 | localhost:9092 |
| Kafka broker 2 | localhost:9093 |

### 3. Preuzimanje EPA podataka

```bash
# Preuzimanje zip arhiva s EPA portala
python src/scripts/download_epa_data.py --start-year 2017 --end-year 2025

# Raspakivanje CSV fajlova iz arhiva
python src/scripts/extract_csv.py
```

### 4. Učitavanje podataka u HDFS

```bash
bash src/scripts/load_to_hdfs.sh
```

### 5. Pokretanje paketne obrade

Pokrenuti DAG `asvsp_batch_pipeline` iz Airflow UI-ja (http://localhost:8081), ili ručno:

```bash
docker exec spark-master bash /batch/process.sh
```

Izvršava se 5 Spark zadataka po redoslijedu:
`load_hourly` → `daily_aggregation` → `monthly_aggregation` → `annual_aggregation` → `baselines`

### 6. Export rezultata u PostgreSQL

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 2g \
  --jars /stream/consumer/postgresql-42.5.1.jar \
  /batch/export_to_postgres.py
```

Ili pokrenuti puni DAG koji uključuje i export korak.

### 7. Pregled rezultata u Hue

Otvoriti http://localhost:8888, navigirati do Hive editora i pokrenuti upite iz direktorijuma `src/queries/`.

### 8. Pokretanje streaming pipeline-a

Kolektor se pokreće kao Docker servis. Producent se automatski pokreće s `docker-compose up`.

Preporučeni način pokretanja je putem Airflow DAG-a `asvsp_streaming_pipeline` (http://localhost:8081), koji automatski:
1. Briše i ponovo kreira Kafka topike (`air_quality_stream`, `weather_stream`) i čisti checkpoint direktorijume
2. Pokreće producenta
3. Sekvencijalno pokreće svih 5 streaming consumera (zbog ograničenih resursa)

Alternativno, consumeri se mogu pokrenuti ručno:

```bash
docker exec spark-master bash /stream/consumer/process.sh
```

Rezultati se upisuju u PostgreSQL.

---

## Korišćenje sistema

- **Hue** (http://localhost:8888) — SQL editor za pokretanje Hive upita nad HDFS podacima
- **Metabase** (http://localhost:3000) — vizualizacija rezultata paketne i streaming obrade iz PostgreSQL baze
- **Spark Master UI** (http://localhost:8080) — praćenje Spark zadataka i resursa klastera
- **Airflow** (http://localhost:8081) — upravljanje DAG-ovima i praćenje pipeline-a
