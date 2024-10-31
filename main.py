from etl_script import *
from conn_db import *

if __name__ == '__main__':
    
    # Fonte de dados
    xml_file = "dados\deputados.xml"
    proposicao_csv = "dados\proposicoes.csv"

    # Tabelas
    tabela_proposicoes = "proposicoes"
    tabela_deputados = "deputados"

    # Conecta com o banco
    engine = connect_db()

    # Carrega dados para o banco
    load_deputado_to_db(xml_file, tabela_deputados, engine)
    load_preposicoes_to_db(proposicao_csv, tabela_proposicoes, engine)