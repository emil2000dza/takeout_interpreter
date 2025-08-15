from src.db.snowflake_client import SnowflakeORM
from src.db.tables import ChromeHistory

client = SnowflakeORM()

with client.session_scope() as session:
    results = session.query(ChromeHistory.id, ChromeHistory.title).limit(5).all()
    for row in results:
        print(row)
