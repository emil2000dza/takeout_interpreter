# Topic Modeling Pipeline

This directory contains a **Topic Modeling Pipeline** for discovering, refining, and classifying topics in your Google Takeout browsing history dataset. It is designed for efficiency, memory-aware batching, and flexible database access.

---

## Overview

The pipeline centralizes configuration, session management, and I/O operations to simplify topic modeling tasks. It can serve as a drop-in replacement for multiple scripts while reusing existing utilities and LLM calls.

**Pipeline Flow:**
1. **Discover Topics** – Extract initial topics.
2. **Refine Topics** – Consolidate and improve topic quality.
3. **Classify** – Assign data points to refined topics.

---

## File Structure

- **main.py** – Entry point to run the full pipeline.  
- **config.py** – `AppConfig` for environment variables and defaults.  
- **naming.py** – Utilities for domain and table naming (`normalize_domain()`, `table_name()`).  
- **models.py** – Data models (`HistoryEntry`) for clear I/O contracts.  
- **db.py** – `SnowflakeRepository` wrapping `SnowflakeORM` for flexible DB access.  
- **pipeline.py** – `TopicModelingPipeline` with `discover_topics()`, `refine_topics()`, and `classify()`.

---

## Usage

1. **Update `.env` file**  
   Define the domain and any other environment variables required by `AppConfig`.  

2. **Execute the pipeline**  
   ```bash
   python src/topic_modeling/main.py
