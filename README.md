# La Seine à Paris, a daily max water levels data collector

**Fluctuat nec mergitur.**

![banner image](img/nivose.gif "A pixel-art concept about an unfinished adventure game set during the Paris centennial flood.")
<br>[Paris est né](https://en.wikipedia.org/wiki/Fluctuat_nec_mergitur), comme on sait, dans cette vieille île de la Cité qui a la forme d'un berceau. La grève de cette île fut sa première enceinte, la Seine son premier fossé. *(Victor Hugo, Notre-Dame de Paris, 1831).*

## Genesis

> This is a follow-up project inspired by the Hackaviz 2025 [dataset](https://github.com/Toulouse-Dataviz/hackaviz-2025). This repository contains a Python script that builds the Paris flood dataset available on [Kaggle](https://www.kaggle.com/datasets/grimespoint/paris-flood-dataset). The Joy Division inspired data visualization is available in the following [notebook]() and this was my original submission for the Hackathon: [1](https://github.com/hyperphantasia/miniature-fortnight) and [2](https://hyperphantasia.github.io/miniature-fortnight).

The resulting dataset is useful for studying flood risk indicators and their related seasonal pattern.

## Table of contents

<details>
<summary>Contents - click to expand</summary>

- [La Seine à Paris, a daily max water levels data collector](#la-seine-à-paris-a-daily-max-water-levels-data-collector)
  - [Genesis](#genesis)
  - [Table of contents](#table-of-contents)
  - [Project overview](#project-overview)
  - [Features](#features)
  - [Functional overview](#functional-overview)
  - [Architecture](#architecture)
  - [Installation](#installation)
    - [Clone the repository](#clone-the-repository)
    - [Create a virtual environment (optional but recommended)](#create-a-virtual-environment-optional-but-recommended)
    - [Install dependencies](#install-dependencies)
  - [Configuration](#configuration)
    - [Available metrics](#available-metrics)
  - [Usage](#usage)
  - [Data structure](#data-structure)
  - [Sample output](#sample-output)
  - [Data processing pipeline](#data-processing-pipeline)
    - [Step 1: fetching](#step-1-fetching)
      - [Fetching is concurrent](#fetching-is-concurrent)
      - [About the date-based pagination strategy](#about-the-date-based-pagination-strategy)
      - [Error handling strategy](#error-handling-strategy)
  - [Step 2: merging](#step-2-merging)
  - [Step 3: duplicate detection](#step-3-duplicate-detection)
  - [Step 4: gap detection](#step-4-gap-detection)
    - [1. Build full date range](#1-build-full-date-range)
    - [2. Detect missing days](#2-detect-missing-days)
    - [3. Group consecutive missing days](#3-group-consecutive-missing-days)
      - [Gap interval construction algorithm](#gap-interval-construction-algorithm)
  - [Limitations](#limitations)
  - [Possible enhancements](#possible-enhancements)
  - [Contributing](#contributing)
  - [License](#license)

</details>

## Project overview

> This project demonstrates production-style data ingestion patterns and concurrent API handling using Python.

The algorithm fetches historical daily maximum water height observations for multiple hydrometric stations from this API endpoint:

```markdown
https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab
```

It then:

- Merges all station's data
- Detects duplicate dates
- Detects missing dates across the global time series
- Builds continuous missing intervals
- Exports a chronologically sorted CSV file

Output file:

```text
paris_flood_dataset.csv
```

## Features

- **Modular** architecture
- **Connection reuse** via `requests.Session`
- **Concurrent** multi-station data fetching
- Automatic **date-based pagination**
- **Global duplicate** detection
- Continuous missing-date **interval detection**
- **Linear-time gap detection** algorithm
- **Chronological** CSV export

## Functional overview

The pipeline runs in three major steps:

```markdown
+-----------------------------+
|        Pipeline Flow        |
+-----------------------------+
| 1) **Concurrent Collection**|
|    - `ThreadPoolExecutor`   |
|    - Fetch stations in parallel
+-----------------------------+
| 2)  **Data Consolidation**  |
|    - Merge into one DataFrame
+-----------------------------+
| 3) **Integrity Analysis**   |
|    - Reconstruct global time range
|    - *Detect:* Missing days,  |
|      Continuous missing ranges,
|      Duplicate days         |
+-----------------------------+
```

## Architecture

A Separate ingestion logic from the validation logic.

```text
Main
 ├── concurrent_fetch()
 │     ├── ThreadPoolExecutor
 │     └── fetch_station()
 │            ├── API calls
 │            ├── Pagination loop
 │            └── DataFrame assembly
 │
 ├── detect_global_gaps()
 |       ├── Duplicate detection
 |       ├── Full date range reconstruction
 |       ├── Missing date detection
 |       └── Interval grouping
 └── CSV sort & export
```

## Installation

### Clone the repository

```bash
git clone https://github.com/hyperphantasia/paris-flood-dataset.git
cd paris-flood-dataset
```

### Create a virtual environment (optional but recommended)

```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows
```

### Install dependencies

- Python 3.10+

```bash
pip install -r requirements.txt
```

*requirements.txt*

```text
requests
pandas
```

## Configuration

- Default parameters:

```python
API_BASE = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"
METRIC = "HIXnJ"
OUTPUT = "paris_flood_dataset.csv"
MAX_PER_PAGE = 20000
MAX_WORKERS = 5
```

| Parameter | Type | Example | Description |
|---|---:|---|---|
| `API_BASE` | string | `"https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"` | Base URL of the API endpoint to query hydrometry observations. Max URL size: *2083 characters*.|
| `METRIC` | string | `"HIXnJ"` | Identifier of the specific metric to request from the API. **HIXnJ**= observed daily max in mm. Other available metrics: [see Metrics](#available-metrics). |
| `OUTPUT` | string (filename) | `"paris_flood_dataset.csv"` | Local filename where the downloaded results will be saved (CSV format). |
| `MAX_PER_PAGE` | integer | `20000` | Maximum number of records to request per API page (page size / limit parameter). For the *Hub'Eau API*: default is *5000*, max is *20000*. |
| `MAX_WORKERS` | integer | `5` | Maximum number of concurrent worker threads/processes to use for parallel requests. |

> [!TIP]
> For I/O-bound (API requests):
> 
> optimal MAX_WORKERS ≈ min(4 * cores, floor(R * avg_req_time_sec), memory_limit)

- Hydrometric stations:

```python
STATIONS = [
    "F700000109",
    "F700000110",
    ...
]
```

Various hydrometric stations references can be found on the french hydroportail. Below is the reference for **la Seine à Paris** stations:

```text
https://www.hydro.eaufrance.fr/sitehydro/F7000001/fiche
```

### Available metrics

| *obs_elab* Code | French | English |
|---|---|---|
| **QmnJ** | débit moyen journalier | daily mean flow |
| **QmM** | débit moyen mensuel | monthly mean flow |
| **HIXnJ** | **hauteur instantanée maximale journalière en mm** | **daily maximum instantaneous water level in mm** |
| **HIXM** | hauteur instantanée maximale mensuelle | monthly maximum instantaneous water level |
| **QIXnJ** | débit instantané maximal journalier | daily maximum instantaneous flow |
| **QIXM** | débit instantané maximal mensuel | monthly maximum instantaneous flow |
| **QINnJ** | débit instantané minimal journalier | daily minimum instantaneous flow |
| **QINM** | débit instantané minimal mensuel | monthly minimum instantaneous flow |

More [details](https://hubeau.eaufrance.fr/page/api-hydrometrie).

## Usage

Run:

```bash
python fluctuat_nec_mergitur.py
```

- Sample console output:

```text
Processing station F700000109
Processing station F700000110
Processing station F700000111
Processing station F700000102
Processing station F700000103
   From F700000103 → 7316 rows
   From F700000111 → 5842 rows
   From F700000110 → 2920 rows
   From F700000109 → 20000 rows
   From F700000109 → 4104 rows
   From F700000102 → 13147 rows

Sorted and saved data to paris_flood_dataset.csv
start_date: 1900-01-02 00:00:00
end_date: 2026-02-27 00:00:00
expected_days: 46078
observed_days: 46059
missing_days_count: 19
duplicate_records_count: 0
missing_ranges: [(Timestamp('1965-12-31 00:00:00'), Timestamp('1966-01-01 00:00:00')), (Timestamp('1973-12-31 00:00:00'), Timestamp('1974-01-01 00:00:00')), (Timestamp('1989-12-31 00:00:00'), Timestamp('1990-01-01 00:00:00')), (Timestamp('1992-06-29 00:00:00'), Timestamp('1992-07-04 00:00:00')), (Timestamp('1994-09-06 00:00:00'), Timestamp('1994-09-06 00:00:00')), (Timestamp('1994-09-23 00:00:00'), Timestamp('1994-09-23 00:00:00')), (Timestamp('1994-09-30 00:00:00'), Timestamp('1994-09-30 00:00:00')), (Timestamp('1995-10-20 00:00:00'), Timestamp('1995-10-20 00:00:00')), (Timestamp('1998-12-31 00:00:00'), Timestamp('1998-12-31 00:00:00')), (Timestamp('1999-05-20 00:00:00'), Timestamp('1999-05-20 00:00:00')), (Timestamp('2000-04-03 00:00:00'), Timestamp('2000-04-03 00:00:00'))]
```

## Data structure

Final CSV columns include:

| Column Name           | Description |
|----------------------|-------------|
| **code_site**          | Location ID |
| **code_station**          | Station ID |
| **date_obs_elab**        | Observation date |
| **resultat_obs_elab**    | Observed value: daily max water level (in mm) |
| **grandeur_hydro_elab**  | Metric code (*HIXnJ*) |
| **date_prod**  | Data production date (processing date)|
| **code_statut**       | Vamlidation status code |
| **libelle_statut**         | Validation status label |
| **code_methode**      | Production method code |
| **libelle_methode**        | Production method label |
| **code_qualification**         | Data quality assesment code |
| **libelle_qualification**         | Data quality assesment label |
| **longitude**         | Station longitude|
| **latitude**         |  Station latitude |
| **grandeur_hydro_elab**         | Observation metric |

Code values are explained in this [document](/doc/codes%20observations%20hydro.pdf) (*french*).

## Sample output

```csv
code_site,code_station,date_obs_elab,resultat_obs_elab,date_prod,code_statut,libelle_statut,code_methode,libelle_methode,code_qualification,libelle_qualification,longitude,latitude,grandeur_hydro_elab
F7000001,F700000109,1900-01-02,1300.0,2025-06-17T09:27:10Z,16,Donnée validée,0,Mesurée,20,Bonne,2.365515502,48.845409133,HIXnJ
```

## Data processing pipeline

### Step 1: fetching

The `fetch_station()` function:

1. Starts at a fixed historical date (`1900-01-01`)
2. Fetches up to `MAX_PER_PAGE` rows
3. Extracts the last observation date
4. Resumes from `last_date` + 1 day
5. Stops when fewer than `MAX_PER_PAGE` rows are returned

#### Fetching is concurrent

Uses:

```python
ThreadPoolExecutor(max_workers=5)
```

Why threads?

- API calls are I/O-bound
- Threads improve latency
- Controlled worker count prevents overload

> Time complexity: O(N / workers) for network wait.

#### About the date-based pagination strategy

This avoids inefficient offset-based pagination (using page numbers), and allows full coverage.

Strategy:

- Fetch maximum allowed rows
- Use last returned date
- Continue from `last_date` + 1 day

Why?

- Avoids offset inefficiency
- Prevents skipped records
- Robust to changing API data

> Time complexity: O(N)

#### Error handling strategy

- `raise_for_status()` validates HTTP responses
- Exceptions handled per station
- Failed station does not crash the pipeline

## Step 2: merging

All station DataFrames are concatenated:

```python
pd.concat(all_data, ignore_index=True)
```

## Step 3: duplicate detection

Duplicates across stations are detected using:

```python
df.duplicated(date_col)
```

Duplicates are counted before being dropped.

## Step 4: gap detection

### 1. Build full date range

```python
pd.date_range(start, end, freq="D")
```

This reconstructs all expected calendar days.

### 2. Detect missing days

```python
missing_days = full_range.difference(observed)
```

### 3. Group consecutive missing days

Example:

Input missing days:

```text
Jan 1, Jan 2, Jan 3, Jan 10, Jan 11
```

Output intervals:

```text
(Jan 1 - Jan 3)
(Jan 10 - Jan 11)
```

This is done via linear scanning.

#### Gap interval construction algorithm

Pseudo-code:

```python
gap_start = first_missing_day
for each current_day in missing_days:
    if current_day - previous_day > 1:
        close previous interval
        start new interval
```

This is:

- Single-pass
- Linear time
- Memory efficient

> Time Complexity: O(M)
> M = number of missing days

## Limitations

>[!WARNING]
> Memory usage grows linearly with dataset size.

- Consider chunked writing for >10M rows
- Consider parquet for larger datasets

## Possible enhancements

- [ ] Validate API schema before processing
- [ ] Implement a retry/backoff mechanism
- [ ] Timeout configuration per request
- [ ] Asynchronous I/O (aiohttp)
- [ ] Control concurrency with a Semaphore or a rate-limiter (token bucket)
- [ ] Multiprocessing vs threading
- [x] Write CSV once at end

## Contributing

1) Fork the repository.
2) Create a feature branch (`git checkout -b feature/your‑feature`).
3) Commit your changes (`git commit -m "Add …"`).
4) Push and open a Pull Request.

## License

This project is released into the public domain under the [Unlicense](https://unlicense.org/). See the [LICENSE](/LICENSE) file for details. Regarding the original data, be aware of the [Hub'Eau](https://assistance.brgm.fr/hubeau/quels-sont-droits-dusage-donnees-proposees-apis) platform  usage rights.

>[!NOTE]
> L’ensemble des données proposées dans le cadre des API sont des données publiques environnementales, déjà diffusées par ailleurs : elles sont donc librement utilisables et réutilisables, dans le cadre de la [licence ouverte interministérielle](https://www.data.gouv.fr/pages/legal/licences/etalab-2.0).

*English translation:*

> All data provided through the APIs are public environmental data, already published elsewhere: they are therefore freely usable and reusable under the Interministerial Open License ([ETALAB](https://www.data.gouv.fr/pages/legal/licences/etalab-2.0)).

♟
