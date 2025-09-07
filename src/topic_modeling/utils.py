"""
utils.py

Utility functions shared between topic_modeling scripts.
"""

import json
import logging
import re
from typing import Dict

import json5
from sqlalchemy import text

from src.db.snowflake_client import SnowflakeORM

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

# ===================
# String utils
# ===================

def format_topics(topics) -> str:
    """
    Format a list of (title, description) into a structured text block
    for LLM prompts.
    """
    lines = []
    for i, (title, desc) in enumerate(topics, start=1):
        lines.append(
            f"Topic {i}:\n"
            f"- Name: {title}\n"
            f"- Description: {desc}\n"
        )
    return "\n".join(lines)

def extract_json(text: str) -> dict:
    """Extract first JSON-like object from LLM output using json5 for leniency."""
    match = re.search(r'\{.*\}', text, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON object found in LLM output")
    
    json_str = match.group()
    
    # Parse using json5, which handles:
    # - trailing commas
    # - single quotes
    # - multi-line strings
    try:
        return json5.loads(json_str)
    except Exception as e:
        logger.warning(f'ERROR : {e}')

# ===================
# Snowflake client utils
# ===================
def has_existing_topics(client_snowflake : SnowflakeORM,
                        table_name: str, 
                        min_count: int = 20) -> bool:
    """
    Check if the topics table for a given domain already contains at least `min_count` rows.

    Parameters
    ----------
    domain : str
        Domain name (e.g., "nytimes.com").
    min_count : int, optional
        Minimum number of topics required to consider the table "complete".

    Returns
    -------
    bool
        True if the table already exists with at least `min_count` rows.
    """
    with client_snowflake.session_scope() as session:
        try:
            result = session.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).fetchone()
        except Exception:
            return False  # Table does not exist yet

        return bool(result and result[0] >= min_count)


def fetch_topics(client_snowflake: SnowflakeORM,
                 table_name : str):
    """Fetch discovered topics from Snowflake."""
    with client_snowflake.session_scope() as session:
        topics = session.execute(
            text(f"SELECT topic_name, description FROM {table_name}")
        ).fetchall()
        return topics

def write_topics_to_snowflake(client_snowflake : SnowflakeORM, 
                              topics: Dict, 
                              topics_table :str) -> None:
    """Write topics into Snowflake table."""
    with client_snowflake.session_scope() as session:
        session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {topics_table} (
                topic_name STRING,
                description STRING,
                example_domains ARRAY,
                example_titles ARRAY
            )
        """))

        for topic, details in topics.items():
            session.execute(
                text(f"""
                    INSERT INTO {topics_table} (topic_name, description, example_domains, example_titles)
                    SELECT $1, $2, PARSE_JSON($3), PARSE_JSON($4)
                    FROM VALUES (:topic_name, :description, :example_domains, :example_titles);
                """),
                {
                    "topic_name": topic,
                    "description": details.get("description"),
                    "example_domains": json.dumps(details.get("example_domains", [])),
                    "example_titles": json.dumps(details.get("example_titles", []))
                }
            )

# ===================
# Naming utils
# ===================

def normalize_domain(domain: str) -> str:
    """Normalize domain to a stable, UPPER_CASE token for table names.
    Rule: drop the last TLD segment, replace remaining dots by underscores, uppercase.
    Example: "news.example.co.uk" → "NEWS_EXAMPLE_CO".
    """
    parts = domain.split(".")
    if len(parts) > 1:
        parts = parts[:-1]
    token = "_".join(parts).upper()
    return token or domain.upper()


def table_name(domain: str, suffix: str) -> str:
    return f"{normalize_domain(domain)}_{suffix}"
