from etl_script import *
from conn_db import *

if __name__ == '__main__':
    
    # Fonte de dados
    xml_file = "dados\deputados.xml"
    proposicao_csv = "dados\proposicoes.csv"
    sitacao_deputados_csv = "dados\SituacaoDeputados.csv"

    # Tabelas
    tabela_proposicoes = "proposicoes"
    tabela_deputados = "deputados"
    tabela_situacao_deputados = "situacao_deputados"

    # Conecta com o banco
    engine = connect_db()

    # Carrega dados para o banco
    load_deputado_to_db(xml_file, tabela_deputados, engine)
    load_proposicoes_to_db(proposicao_csv, tabela_proposicoes, engine)
    load_situacao_deputados_to_db(sitacao_deputados_csv, tabela_situacao_deputados, engine)
    create_time_table(engine)