import xml.etree.ElementTree as ET
import psycopg2
import pandas as pd
import sqlalchemy
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
            'ideCadastro': deputado.find('ideCadastro').text,
            #'codOrcamento': deputado.find('codOrcamento').text,
            #'condicao': deputado.find('condicao').text,
            #'matricula': deputado.find('matricula').text,
            'idParlamentar': deputado.find('idParlamentar').text,
            'nome': deputado.find('nome').text,
            'nomeParlamentar': deputado.find('nomeParlamentar').text,
            #'urlFoto': deputado.find('urlFoto').text,
            'sexo': deputado.find('sexo').text,
            'uf': deputado.find('uf').text,
            'partido': deputado.find('partido').text,
            'gabinete': deputado.find('gabinete').text,
            'anexo': deputado.find('anexo').text,
            'fone': deputado.find('fone').text,
            'email': deputado.find('email').text
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql(table, engine, if_exists='replace', index=False)
    print(f"Dados dos deputados carregados na tabela {table} com sucesso!")

def load_orgaos_to_db(xml_file, table, engine):
    """
    Função que os orgaos para o banco de dados pgsql.
    descontinuado?
    """

    tree = ET.parse(xml_file)
    root = tree.getroot()

    data = []

    for orgao in root.findall('orgao'):
        record = {
            'id': orgao.get('id'),
            'idTipodeOrgao': orgao.get('idTipodeOrgao'),
            'sigla': orgao.get('sigla'),
            'descricao': orgao.get('descricao')
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Dados de órgãos carregados na tabela {table_name} com sucesso!")

def load_preposicoes_to_db(csv_file, table, engine):
    """
    Função que carrega preposicoes para o banco de dados pgsql.
    """


    df = pd.read_csv(csv_file, delimiter=';', on_bad_lines='skip')

    try:
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"Dados das preposicoes carregados na tabela {table} com sucesso!")
    
    except pd.errors.ParserError as e:
        print(f"Erro ao processar o CSV: {e}")

def create_time_table(engine):
    metadata = MetaData()

    tempo = Table(
        'tempo', metadata,
        Column('id', Integer, primary_key=True),
        Column('ano', Integer, nullable=False),
    )

    metadata.create_all(engine)
