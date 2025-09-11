# Database

This directory contains database utilities and table schemas used for exploration and the topic modeling pipeline.

---

## Overview

- **Snowflake ORM** – Provides a wrapper for interacting with Snowflake.  
- **Table Schemas** – Definitions of tables used for data exploration or by the topic modeling pipeline.

These components allow consistent and flexible access to Snowflake data, supporting both raw SQL queries and ORM-style interactions.

---

## Usage

1. Import the Snowflake repository or table schemas in your scripts:  
   ```python
   from db.db import SnowflakeRepository
   from db.models import YourTableSchema
