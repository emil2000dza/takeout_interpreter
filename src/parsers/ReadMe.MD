# Parsers

This directory contains scripts to process Google Takeout data and generate `.csv` files that can be used for further analysis or loaded into Snowflake.

---

## Overview

The parsers extract data from Google Takeout exports for:

- **YouTube history**  
- **Google Analytics / Phone history**  
- **Chrome browsing history**  

The output is `.csv` files ready for ingestion into Snowflake. These datasets can then be used to replicate or extend analyses performed in other parts of the repository.

---

## Usage

1. Place your Google Takeout data in the appropriate folder.  
2. Run the relevant parser script:  
   ```bash
   python parsers/youtube_parser.py
   python parsers/ga_parser.py
   python parsers/chrome_parser.py
