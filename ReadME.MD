# Digital Footprint Analysis

This project explores digital footprints using browsing history exported from **Google Takeout (Google Chrome data)**. It provides scripts and visualization examples for analyzing its google usage. The workflow integrates :
- **GCS** – Hosting raw and processed data
- **Snowflake** – Data warehouse for storage and querying
- **dbt** - Data trasnformation 
- **Gemini** - Topic modeling using LLMs,
- **Metabase** – Visualization and exploration of results

Additional parsers are available for other Google Takeout data sources (e.g., YouTube history, phone activity).

---

## ⚙️ Prerequisites

- Export Google Chrome History using [Google Takeout](https://takeout.google.com/).
- Docker installed on your system  
- `.env` file containing:  
  - Snowflake account parameters  
  - Gemini API Key  

> A template `.env.example` is provided; it must be updated with your own credentials.

---

## 📁 Project Structure
```google_takeout_analyser/
├── data/                     # Your Takeout data
├── src/
│   ├── parsers/              # Scripts to parse Takeout data into CSV
│   ├── topic_modeling/       # Topic modeling pipeline (URL classification)
│   ├── db/                   # Snowflake ORM and table schemas
├── takeout_dbt/              # DBT project for transforming ingested Takeout data
├── requirements.txt          # Python dependencies
├── Dockerfile                # Docker image configuration
├── .env.example              # Template environment file
├── .gitignore
└── README.md
```

---

## 🔹 Workflow Overview

1. Parse Takeout data using scripts in `src/parsers/` to generate CSVs.
2. Load CSVs into Snowflake for structured storage.
3. Run topic modeling pipeline (`src/topic_modeling/`) to link URLs to topics (optional).
4. Transform and test data with DBT (`takeout_dbt/`) for staging and mart models.
5. Visualize results in Metabase, exploring browsing patterns and trends.

---

## 💡 Notes

- The .env file must be correctly configured before running any scripts.
- Parsers support YouTube, Chrome, and phone history exports from Google Takeout.

- Topic modeling output can serve as input to DBT mart models for downstream analysis.

---

## 🚀 Example usage

<img src="img/output_medium.gif" width="100%">


