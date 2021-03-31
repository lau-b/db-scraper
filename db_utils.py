import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv('.env')

def create_db_engine():
    host = os.getenv('host')
    user = os.getenv('user')
    port = os.getenv('port')
    db = os.getenv('db')
    password = os.getenv('password')

    connection_string = f'postgres://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(connection_string)
    return engine


if __name__ == '__main__':
    pew = db_engine()
