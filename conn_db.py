from sqlalchemy import create_engine

def connect_db():
    """
    Função de conexão e configuração com o banco de dados pgsql.
    """
    user = 'user'
    password = 'password'
    host = 'localhost'
    port = '5432'
    database = 'original_db'
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
