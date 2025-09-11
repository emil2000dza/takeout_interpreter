from dotenv import load_dotenv
import os
from sqlalchemy import Column, Integer, String, TIMESTAMP, JSON
from src.db.snowflake_client import Base

load_dotenv()

class ChromeHistory(Base):
    """chrome_history mapping for Snowflake."""
    __tablename__ = os.getenv("SNOWFLAKE_TAKEOUT_EXPORT_TABLE_NAME", "")

    id = Column(Integer, primary_key=True)
    datetime = Column(TIMESTAMP)
    title = Column(String)
    url = Column(String)
    page_transition_qualifier = Column(String)
    favicon_url = Column(String)
    client_id = Column(String)
    domain = Column(String)
    time_since_last_visit = Column(String)

class DiscoveredTopics(Base):
    """Topics output by gemini2.5."""
    __tablename__ = "DISCOVERED_TOPICS_IN_HISTORY"

    description = Column(String, primary_key=True)
    example_domains = Column(JSON)
    example_titles = Column(JSON)
    topic_name = Column(String, primary_key=True)
