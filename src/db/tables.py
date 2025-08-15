from sqlalchemy import Column, Integer, String, TIMESTAMP
from src.db.snowflake_client import Base

class ChromeHistory(Base):
    """chrome_history mapping for Snowflake."""
    __tablename__ = "raw_history"

    id = Column(Integer, primary_key=True)
    datetime = Column(TIMESTAMP)
    title = Column(String)
    url = Column(String)
    page_transition_qualifier = Column(String)
    favicon_url = Column(String)
    client_id = Column(String)
    domain = Column(String)
    time_since_last_visit = Column(String)
