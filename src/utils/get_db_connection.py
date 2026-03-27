from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    db_url = os.getenv('DATABASE_URL')
    engine = create_engine(db_url)
    return engine.connect()