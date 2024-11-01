from sqlalchemy import MetaData, String, Table, Column, Integer, select, text

def create_and_populate_dim_time(engine, schema):

    metadata = MetaData(schema=schema)

    # Tabela a ser inserida no esquema 'cubo'
    tempo = Table(
            'dim_tempo', metadata,
            Column('ano', Integer, primary_key=True)
        )
    
    try:
        # Cria a tabela dim_tempo no esquema 'cubo'
        metadata.create_all(engine)
        print("[INFO] Tabela dimensão dim_tempo criada com sucesso.")

        # Inserir os anos de 2002 a 2016 -- alcance de tempo presente no proposicoes.csv
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