from sqlalchemy import text

def create_new_schema(engine, name):
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {name};"))
                print(f"[INFO] Esquema {name} criado com sucesso.")
    
    except Exception as e:
        print(f"[ERROR] Falha ao criar esquema {name}.\n {e}")

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
