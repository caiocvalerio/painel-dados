import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer

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
    print(f"Dados dos deputados carregados na tabela {table} com sucesso!")

def load_proposicoes_to_db(csv_file, table, engine):
    """
    Função que carrega preposicoes para o banco de dados pgsql.
    """
    
    df = pd.read_csv(csv_file, delimiter=';', on_bad_lines='skip')

    # Colunas a remover
    colunas_remover = []
    df = df.drop(columns=colunas_remover)

    try:
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"Dados das preposicoes carregados na tabela {table} com sucesso!")
    
    except pd.errors.ParserError as e:
        print(f"Erro ao processar o CSV: {e}")

def load_situacao_deputados_to_db(csv_file, table, engine):

    df = pd.read_csv(csv_file, delimiter=";", on_bad_lines='skip', encoding='ISO-8859-1')

    try:
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"Dados das situações dos deputados carregados na tabela {table} com sucesso!")

    except pd.errors.ParserError as e:
        print(f"Erro ao processar o CSV: {e}")

def create_time_table(engine):
    metadata = MetaData()
    
    try:
        tempo = Table(
            'tempo', metadata,
            Column('ano', Integer, primary_key=True)
        )

        metadata.create_all(engine)

        # Inserir os anos de 2002 a 2016 -- dados que estão presentes no proposicoes.csv
        with engine.connect() as conn:
            
            # Iniciar uma transação explícita
            with conn.begin():
                anos = [{'ano': ano} for ano in range(2002, 2017)]
                conn.execute(tempo.insert(), anos)

        print("Tabela tempo criada com sucesso.")

    except Exception as e:
        print("Erro ao criar a tabela tempo: {e}")
