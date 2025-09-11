"""
snowflake_client.py
Reusable Snowflake ORM client.
"""

import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class SnowflakeORM:
    """
    Snowflake ORM client using SQLAlchemy.
    Provides an ACID-compliant interface to interact with Snowflake tables.
    """

    def __init__(self, user=None, password=None, account=None,
                 database=None, schema=None, warehouse=None):
        """
        Initialize the Snowflake ORM connection.
        Falls back to environment variables if arguments are not provided.
        """
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")

        if not all([self.user, self.password, self.account,
                    self.database, self.schema, self.warehouse]):
            raise ValueError("Missing required Snowflake connection parameters.")

        connection_string = (
            f"snowflake://{self.user}:{self.password}"
            f"@{self.account}/{self.database}/{self.schema}"
            f"?warehouse={self.warehouse}"
        )

        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(bind=self.engine)

    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope for database operations.
        Ensures ACID compliance by committing on success
        and rolling back on error.
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self):
        """Create all tables defined in ORM models."""
        Base.metadata.create_all(self.engine)

    def drop_tables(self):
        """Drop all tables defined in ORM models."""
        Base.metadata.drop_all(self.engine)
