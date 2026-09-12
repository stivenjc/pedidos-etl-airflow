import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def obtener_conexion():
    """
    Crea y devuelve un engine de SQLAlchemy conectado a Postgres,
    usando credenciales leídas desde el archivo .env.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    url_conexion = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    engine = create_engine(url_conexion)
    return engine

