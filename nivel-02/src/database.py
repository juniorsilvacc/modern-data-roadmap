from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username="postgres",
        password="root",
        host="localhost",
        port="5432",
        database="modern-data-etl-nv1"
    )
    return create_engine(url)
