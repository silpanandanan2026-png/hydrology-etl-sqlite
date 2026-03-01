# Hydrology Data Explorer API → SQLite Data Engineering Pipeline

This project is a **simple ETL pipeline**

It does the following:

1. Connects to the **Hydrological Data Explorer API**
2. Finds the station **HIPPER_PARK ROAD BRIDGE_E_202312**
3. Downloads the **10 most recent readings** for **two parameters**:
   - Conductivity (µS/cm)
   - Dissolved Oxygen (mg/L)
4. Transforms the data into a **small star schema**
5. Loads it into a **SQLite file-based database**
6. Includes **tests** and a **single command** to run the pipeline

---

## 1) Project structure

```text
hydrology_takehome/
├─ src/
│  ├─ __init__.py
│  ├─ settings.py
│  ├─ hydrology_api.py
│  ├─ transform.py
│  ├─ database.py
│  └─ pipeline.py
├─ tests/
│  ├─ test_transform.py
│  ├─ test_database.py
│  └─ test_pipeline_unit.py
├─ requirements.txt
```

---

## 2) What software you need

### Required
- **Python 3.10+**
- **VS Code**
- **DB Browser for SQLite** 

---

## 3) How to open and run (Windows / PowerShell)

> You can run commands in **VS Code Terminal** or normal **PowerShell**.  

### Step A — Open the project folder
- Open VS Code
- **File → Open Folder**
- Select the folder that contains `src`, `tests`, `requirements.txt`

### Step B — Open terminal
In VS Code:
- **Terminal → New Terminal**

## 4) Create virtual environment

### PowerShell command
```powershell
python -m venv .venv
```

### Activate it
```powershell
.\.venv\Scripts\Activate.ps1
```

If you get an execution policy error, use this (temporary, current terminal only):
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

You should then see `(.venv)` at the start of the terminal line.

---

## 5) Install dependencies

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 6) Run the pipeline 

python -m src.pipeline

### Optional: choose custom DB path

python -m src.pipeline --db-path output\my_hydrology.db


### Optional: change number of recent readings per parameter

python -m src.pipeline --latest-n 10


## 7) What output to expect

After running, you should see JSON output in the terminal showing:
- database path
- station info
- selected measures
- number of fetched rows
- number of inserted rows
- row counts in each table

If you run it again, `inserted_fact_rows` may be `0` because duplicates are prevented (**idempotent load**).

---

## 8) Star schema design 

### Dimension tables
- `dim_station` → station details (name, river, coordinates)
- `dim_parameter` → measure metadata (parameter name, unit, measure ID, period, etc.)

### Fact table
- `fact_measurement` → timestamped readings and values

analytical queries such as:
- latest reading by parameter
- reading trends by date
- row counts / data completeness checks

---

## 9) Run tests

```powershell
pytest -q
```

---

## 10) How to inspect the SQLite database

The database file is created at:
```text
output/hydrology_hipper.db
```

### Option A : DB Browser for SQLite
- Install “DB Browser for SQLite”
- Open file `output/hydrology_hipper.db`
- Browse tables: `dim_station`, `dim_parameter`, `fact_measurement`

### Option B (VS Code extension)
- Install SQLite Viewer
- Open the `.db` file
- Browse tables and run simple queries

---

