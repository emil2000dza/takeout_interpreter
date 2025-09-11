# Digital Footprint Analysis

![Browsing History Overview](img/takeout.gif)

This project explores my **digital footprint**, focusing on Google Chrome browsing activity on mobile. Since most of my online activity is concentrated on Chrome, we use Google Takeout data to analyze, transform, and classify browsing behavior into broader topics using **LLMs**.

The goal of this project is to provide visualization examples and scripts for exploring an individual's digital footprint using their Google Takeout data. It combines Snowflake, dbt, and Metabase with a topic modeling pipeline that maps each visited URL to broader browsing activity topics.
- **Snowflake** – Data warehouse for storage and querying  
- **GCS** – Hosting raw and processed data  
- **Metabase** – Visualization and exploration of results  

Parsers are also available for other Takeout data (YouTube, phone history, etc.).

---

## ⚙️ Prerequisites

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