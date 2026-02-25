# Piraeus AIS — Scalable Trajectory Modeling (MongoDB)

## Overview

End-to-end data engineering project that transforms raw AIS vessel position streams into enriched, bucketed trajectory documents stored in MongoDB.

Data Source: https://zenodo.org/records/6323416
- Files:
    - ais_static.zip
    - geodata.zip
    - noaa_weather.zip
    - unipi_ais_dynamic_2019.zip
    - unipi_ais_dynamic_synopses.zip

The system performs:

- Data cleaning & validation (Python)
- Weather enrichment (spatial + temporal matching)
- Trip segmentation (>120 min gap rule)
- Geospatial indexing (2dsphere)
- Aggregation-based spatio-temporal queries
- Performance evaluation & benchmarking

---

## What This Project Demonstrates

- NoSQL schema design (embedding + bucketing pattern)
- Geospatial data modeling
- Spatio-temporal analytics
- Query optimization & index strategy
- Performance experimentation under varying workloads

---

## Data Pipeline

Raw AIS CSV  
→ Cleaning & type correction  
→ Weather enrichment (nearest station matching)  
→ Trip reconstruction  
→ MongoDB document generation  
→ Index creation  
→ Aggregation queries & performance evaluation  

All steps are implemented in Python.

---

## Example Document (Trips Collection)

See: `sample_document.json`

Each trip document contains:

- Vessel metadata
- Start & end time
- Embedded trajectory array
- Per-point weather data
- Derived semantic features

This design minimizes join operations and improves read locality.

---

## Indexing Strategy

- 2dsphere index on trajectory locations
- Compound index (vessel_id, start_time)
- Nested index for selective weather filtering

Indexes were evaluated experimentally for performance impact.

---

## Implemented Queries

### 1. Spatio-temporal Port Activity
Count vessels inside port geometry within a time window.

### 2. Crosswind Risk Detection
Detect large vessels:
- Low speed
- Inside port
- Strong wind
- Crosswind angle relative to course

Crosswind condition based on angular difference between vessel course and wind direction.

---

## Performance Evaluation

Experiments conducted on:

- Index vs no index (See `images/exp1_detailed_indexes.png`)
- Increasing time window size (See `images/exp2_scalabilty.png`)
- Selectivity variation (See `images/exp3_selectivity.png`)
- Increasing trajectory complexity (See `images/exp4_complexity.png`)

Results show:
- Significant latency reduction with indexing
- Linear scaling within memory limits (almost linear, x axis is not linear)
- Performance degradation beyond cache threshold
- Impact of embedded array size on query cost


---

## How to Run (Optional)

### Requirements

- Python 3.10+
- Docker
- Docker Compose

---

### 1️⃣ Start MongoDB with Docker

From the root directory of the project:

```bash
docker compose up -d
```


### 2️⃣ Download the Required Datasets

Download the AIS and weather datasets mentioned at the beginning of this README.

Place them in the correct paths as defined at the top of each script inside /python_scripts

3️⃣ Run the ETL Pipeline
python python_scripts/main.py

This will:

- Clean and validate AIS data
- Enrich trajectories with weather data
- Segment trips
- Generate MongoDB documents
- Create indexes
- Load the database
