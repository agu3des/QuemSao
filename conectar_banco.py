import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configuração do banco
db_user = "postgres"
db_password = "12345678"
db_host = "localhost"
db_port = "5432"
db_name = "enade"

# Pastas por ano
pastas = {2014: "dados2014", 2017: "dados2017", 2021: "dados2021"}

# Arquivos relevantes ao problema
arquivos_interesse = [
    "microdados{}_arq1.txt",
    "microdados{}_arq2.txt",
    "microdados{}_arq5.txt",
    "microdados{}_arq6.txt",
    "microdados{}_arq7.txt",   
    "microdados{}_arq8.txt",
    "microdados{}_arq9.txt",
    "microdados{}_arq12.txt",
    "microdados{}_arq14.txt",
    "microdados{}_arq15.txt",  
    "microdados{}_arq16.txt",  
    "microdados{}_arq18.txt",  
    "microdados{}_arq21.txt",  
    "microdados{}_arq23.txt"   
]

def carregar_txt(caminho):
    """Lê arquivos TXT separados por ; ou tab"""
    try:
        df = pd.read_csv(caminho, sep=';', encoding='latin1', dtype=str, low_memory=False)
    except Exception:
        df = pd.read_csv(caminho, sep='\t', encoding='latin1', dtype=str, low_memory=False)
    return df

def salvar_para_postgres(df, table_name, conn):
    """Salva DataFrame no PostgreSQL"""
    cols = ",".join([f'"{c}"' for c in df.columns])
    values = [tuple(x) for x in df.to_numpy()]
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])

    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
        cur.execute(f'CREATE TABLE "{table_name}" ({col_defs});')
        execute_values(cur, f'INSERT INTO "{table_name}" ({cols}) VALUES %s', values)

    conn.commit()
    print(f"Tabela {table_name} carregada com {len(df)} linhas.")

def carregar_e_salvar_arquivos():
    """Carrega todos os arquivos relevantes e envia ao PostgreSQL"""
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.set_client_encoding('LATIN1')

    for ano, pasta in pastas.items():
        for modelo in arquivos_interesse:
            arq = modelo.format(ano)
            caminho = os.path.join(pasta, arq)
            if not os.path.exists(caminho):
                print(f"Arquivo não encontrado: {caminho}")
                continue

            print(f"Carregando {caminho}...")
            df = carregar_txt(caminho)
            table_name = f"{ano}_{arq.replace('.txt', '').lower()}"
            salvar_para_postgres(df, table_name, conn)

    criar_relacoes_basicas(conn)
    conn.close()

def criar_relacoes_basicas(conn):
    """Cria relações simples entre tabelas por ano"""
    anos = [2014, 2017, 2021]
    relacionados = ["arq1", "arq5", "arq6", "arq7", "arq8", "arq9", "arq12", "arq14",
                    "arq15", "arq16", "arq18", "arq21", "arq23"]

    with conn.cursor() as cur:
        for ano in anos:
            tabela_principal = f'"{ano}_microdados{ano}_arq2"'
            print(f"\nCriando relações para {ano}...")

            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = '{ano}_microdados{ano}_arq2' AND column_name = 'id'
                    ) THEN
                        ALTER TABLE {tabela_principal} ADD COLUMN id SERIAL PRIMARY KEY;
                    END IF;
                END $$;
            """)

            # Adiciona coluna fk_curso nos demais arquivos e copia CO_CURSO
            for arq in relacionados:
                tabela_sec = f'"{ano}_microdados{ano}_{arq}"'
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_name = '{ano}_microdados{ano}_{arq}'
                        ) THEN
                            BEGIN
                                IF NOT EXISTS (
                                    SELECT 1 FROM information_schema.columns
                                    WHERE table_name = '{ano}_microdados{ano}_{arq}'
                                    AND column_name = 'fk_curso'
                                ) THEN
                                    ALTER TABLE {tabela_sec} ADD COLUMN fk_curso TEXT;
                                END IF;

                                IF EXISTS (
                                    SELECT 1 FROM information_schema.columns
                                    WHERE table_name = '{ano}_microdados{ano}_{arq}'
                                    AND column_name = 'CO_CURSO'
                                ) THEN
                                    EXECUTE format('UPDATE %I SET fk_curso = "CO_CURSO"', '{ano}_microdados{ano}_{arq}');
                                END IF;
                            END;
                        END IF;
                    END $$;
                """)

            print(f"Relacoes basicas criadas para {ano}.")

    conn.commit()
    print("\nTodas as relacoes basicas foram criadas com sucesso!")

if __name__ == "__main__":
    carregar_e_salvar_arquivos()
