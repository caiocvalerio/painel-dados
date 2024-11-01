import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import MetaData, String, Table, Column, Integer, select, text

"""
Funções de estrutura do postgres
"""
def create_new_schema(engine, name):
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {name};"))
                print(f"[INFO] Esquema {name} criado com sucesso.")
    
    except Exception as e:
        print(f"[ERROR] Falha ao criar esquema {name}.\n {e}")

"""
Funções utilitárias
"""
def verificar_tabelas_publicas(engine, schema):
    '''
    Função para verificar se as tabelas do esquema publico já estão criadas e populadas.
    Retorna True se já está criado e populado e False caso não.
    '''

    tabelas = ['deputados', 'proposicoes', 'situacao_deputados']
    
    with engine.connect() as conn:
        try:
            for tabela in tabelas:
                # Verifica se as tabelas existem
                resultado = conn.execute(
                    text(f"""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_schema = '{schema}' 
                        AND table_name = '{tabela}'
                    );
                    """)
                ).scalar()

                if not resultado:
                    print(f"[ERROR] A tabela '{tabela}' não existe no esquema '{schema}'.")
                    return False

                # Verifica se a tabela tem registros
                quantidade_registros = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{tabela};")
                ).scalar()

                if quantidade_registros == 0:
                    print(f"[ERROR] A tabela '{tabela}' no esquema '{schema}' está vazia.")
                    return False

            # Se todas as tabelas foram verificadas e possuem registros, retorna True
            return True

        except Exception as e:
            print(f"[ERROR] Falha ao verificar as tabelas do esquema '{schema}'.\n{e}")
            return False

"""
Carregamento dos dados brutos (csv, xml) para o esquema público
"""
def load_deputado_to_db(xml_file, table, engine):
    """
    Função que carrega deputado csv para o banco de dados pgsql.
    """

    tree = ET.parse(xml_file)
    root = tree.getroot()
    data = []

    for deputado in root.findall('deputado'):
        record = {
            #'ideCadastro': deputado.find('ideCadastro').text,
            #'codOrcamento': deputado.find('codOrcamento').text,
            #'condicao': deputado.find('condicao').text,
            'matricula': deputado.find('matricula').text,
            'idParlamentar': deputado.find('idParlamentar').text,
            #'nome': deputado.find('nome').text,
            #'nomeParlamentar': deputado.find('nomeParlamentar').text,
            #'urlFoto': deputado.find('urlFoto').text,
            #'sexo': deputado.find('sexo').text,
            'uf': deputado.find('uf').text,
            'partido': deputado.find('partido').text,
            'gabinete': deputado.find('gabinete').text,
            #'anexo': deputado.find('anexo').text,
            #'fone': deputado.find('fone').text,
            #'email': deputado.find('email').text
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql(table, engine, if_exists='replace', index=False)
    print(f"[INFO] Dados dos deputados carregados na tabela {table} com sucesso!")

def load_proposicoes_to_db(csv_file, table, engine):
    """
    Função que carrega preposicoes para o banco de dados pgsql.
    """
    
    df = pd.read_csv(csv_file, delimiter=';', on_bad_lines='skip', low_memory=False)

    # Colunas a remover
    colunas_remover = []
    df = df.drop(columns=colunas_remover)

    try:
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"[INFO] Dados das preposicoes carregados na tabela {table} com sucesso!")
    
    except pd.errors.ParserError as e:
        print(f"[ERROR] Falha ao carregar as proposicoes. \n {e}")

def load_situacao_deputados_to_db(csv_file, table, engine):

    df = pd.read_csv(csv_file, delimiter=";", on_bad_lines='skip', encoding='ISO-8859-1')

    try:
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"[INFO] Dados das situações dos deputados carregados na tabela {table} com sucesso!")

    except pd.errors.ParserError as e:
        print(f"[ERROR] Falha ao carregar dados das situações dos deputados.\n {e}")

"""
Criação e população das tabelas do OLAP
"""
def create_and_populate_dim_time(engine, schema):
    metadata = MetaData(schema=schema)
    tempo = Table(
            'dim_tempo', metadata,
            Column('ano', Integer, primary_key=True)
        )
    
    try:
        metadata.create_all(engine)
        print("[INFO] Tabela dimensão dim_tempo criada com sucesso.")

        # Inserir os anos de 2002 a 2016 -- dados que estão presentes no proposicoes.csv
        with engine.connect() as conn:
            with conn.begin():
                anos = [{'ano': ano} for ano in range(2002, 2017)]
                conn.execute(tempo.insert(), anos)
        print("[INFO] Tabela dimensão dim_tempo populada com sucesso.")

    except Exception as e:
        print(f"[ERROR] Falha ao criar e popular a tabela dim_tempo.\n {e}")

def create_and_populate_dim_deputados(engine, source_schema, schema):
    metadata = MetaData(schema=schema)

    # Tabela a ser inserida no esquema 'cubo'
    dim_deputados = Table(
        'dim_deputado', metadata,
        Column('id_parlamentar', Integer, primary_key=True),
        Column('partido', String(50), nullable=False),
        Column('uf', String(2), nullable=False),
        Column('gabinete', String(20), nullable=True),
        Column('matricula', String(20), nullable=False)
    )

    # Tabela de origem (no esquema 'public')
    deputados_public = Table(
        'deputados', MetaData(),
        autoload_with=engine,
        schema=source_schema
    )

    try:
        # Cria a tabela dim_deputado no esquema 'cubo'
        metadata.create_all(engine)
        print("[INFO] Tabela dimensão dim_deputado criada com sucesso.")

        with engine.connect() as conn:
            # Inicia uma transação com o banco de dados
            trans = conn.begin() 
            
            # Monta o select dos dados da tabela de origem (esquema 'public')
            select_stmt = select(
                deputados_public.c.idParlamentar, # 'c' é um atalho que permite acessar as colunas da tabela
                deputados_public.c.partido,
                deputados_public.c.uf,
                deputados_public.c.gabinete,
                deputados_public.c.matricula
            )

            # Exectua o select no banco
            result = conn.execute(select_stmt)

            # Converter o resultado do select em uma lista de dicionários
            # As chaves são os nomes das colunas e os valores são os dados retornados.
            result_mappings = result.mappings().all()

            # Verifica se a lista de dicionários possui dados
            if not result_mappings:
                print("[WARNING] Nenhum dado foi encontrado na tabela de origem.")
            else:
                # Preparar os dados para inserção
                insert_data = [
                    {   #coluna no cubo: row[coluna no public]
                        'id_parlamentar': row['idParlamentar'], 
                        'partido': row['partido'],
                        'uf': row['uf'],
                        'gabinete': row['gabinete'],
                        'matricula': row['matricula']
                    }
                    for row in result_mappings
                ]

                # Inserir os dados na tabela dimensão
                conn.execute(dim_deputados.insert(), insert_data)
                
                # Salva as inserções realizadas no banco
                trans.commit() 
                print("[INFO] Tabela dimensão dim_deputado populada com sucesso.")

    except Exception as e:
        print(f"[ERROR] Falha ao criar e popular a tabela dim_deputado.\n {e}")
        trans.rollback() # Reverte a transação em caso de erro

def create_dim_membro(engine, source_schema, schema):
    pass

def create_dim_situacao(engine, source_schema, schema):
    pass

def create_fato_proposicao(engine, source_schema, schemaa):
    pass