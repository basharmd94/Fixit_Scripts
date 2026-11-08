from sqlalchemy import create_engine

# Database configuration
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "da"

# Database URL
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """
    Create and return a SQLAlchemy engine for the database.
    
    Returns:
        SQLAlchemy Engine instance
    """
    return create_engine(DATABASE_URL, future=True)
