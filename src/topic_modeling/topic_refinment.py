"""
topic_refinement.py

Runs topic refinement on discovered topics using TOPIC_REFINEMENT_PROMPT,
parses the resulting JSON, and writes refined topics into Snowflake.
"""

from src.db.snowflake_client import SnowflakeORM
from src.db.tables import DiscoveredTopics
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import TOPIC_REFINMENT_PROMPT
from src.topic_modeling.utils import extract_json, format_topics, write_topics_to_snowflake

# ---------- CONFIG ----------
REFINED_TOPICS_TABLE = "REFINED_TOPICS"
client_snowflake = SnowflakeORM()


# ---------- FUNCTIONS ----------
def fetch_discovered_topics(limit: int = None):
    """Fetch discovered topics from Snowflake."""
    with client_snowflake.session_scope() as session:
        query = session.query(
            DiscoveredTopics.topic_name,
            DiscoveredTopics.description,
        )
        if limit:
            query = query.limit(limit)
        return query.all()

def run_topic_refinement(topics):
    """Run LLM refinement on discovered topics and return structured output."""
    prompt = TOPIC_REFINMENT_PROMPT.format(
        all_topics=format_topics(topics)
    )
    response = call_llm(prompt)

    try:
        return extract_json(response)
    except Exception:
        print("⚠️ Invalid JSON received from LLM.")
        print(response)
        return {}


# ---------- MAIN ----------
if __name__ == "__main__":
    topics = fetch_discovered_topics()
    refined_topics = run_topic_refinement(topics)

    if refined_topics:
        write_topics_to_snowflake(client_snowflake, refined_topics, REFINED_TOPICS_TABLE)
        print(f"✅ {len(refined_topics)} refined topics written to Snowflake.")
    else:
        print("ℹ️ No refined topics produced.")
