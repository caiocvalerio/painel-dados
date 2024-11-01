from etl_script.schema_util import *
from etl_script.data_loader import *
from etl_script.olap_creation import *
from db.conn_db import *

if __name__ == '__main__':
    
    # Fonte de dados
    xml_file = "dados/deputados.xml"
    proposicao_csv = "dados/proposicoes.csv"
    sitacao_deputados_csv = "dados/SituacaoDeputados.csv"

    # Nome das tabelas da base de dados original
    tabela_proposicoes = "proposicoes"
    tabela_deputados = "deputados"
    tabela_situacao_deputados = "situacao_deputados"
    
    # Nome dos esquemas no postgres
    esquema_olap = "cubo"
    esquema_dados_brutos = "public"

    # Conecta com o banco
    engine = connect_db()

    # Carrega dados brutos para o banco no esquema público
    load_deputado_to_db(xml_file, tabela_deputados, engine)
    load_proposicoes_to_db(proposicao_csv, tabela_proposicoes, engine)
    load_situacao_deputados_to_db(sitacao_deputados_csv, tabela_situacao_deputados, engine)
    
    # Iniciar a criação do cubo apenas após a base de dados publica estar criada e populada
    if verificar_tabelas_publicas(engine, esquema_dados_brutos):

        # Cria o esquema do OLAP
        create_new_schema(engine, esquema_olap)

        # Cria tabelas do OLAP
        create_and_populate_dim_time(engine, esquema_olap)
        create_and_populate_dim_deputados(engine, esquema_dados_brutos, esquema_olap)

