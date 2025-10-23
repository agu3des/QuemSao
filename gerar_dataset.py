import pandas as pd
from sqlalchemy import create_engine, text

db_user = "postgres"
db_password = "12345678"
db_host = "localhost"
db_port = "5432"
db_name = "enade"

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

cursos_ti = [72, 73, 76, 79, 4004, 4005, 4006, 5809, 5814]
uf_paraiba = 25
anos = [2014, 2017, 2021]

tabelas = {
    "arq1": [],  
    "arq2": [],
    "arq5": ["TP_SEXO"],
    "arq6": ["NU_IDADE"],
    "arq7": ["QE_I01"],      
    "arq8": ["QE_I02", "QE_I03"],
    "arq9": ["QE_I04", "QE_I05"],
    "arq12": ["QE_I06"],
    "arq14": ["QE_I07"],
    "arq15": ["QE_I09"],      
    "arq16": ["QE_I10"],      
    "arq18": ["QE_I12"],      
    "arq21": ["QE_I15"],      
    "arq23": ["QE_I17"]       
}

def tabela_existe(conn, nome_tabela):
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables WHERE table_name = :t
        );
    """), {"t": nome_tabela})
    return result.scalar()

def colunas_existentes(conn, tabela):
    result = conn.execute(text("""
        SELECT column_name FROM information_schema.columns WHERE table_name = :t;
    """), {"t": tabela})
    return [r[0] for r in result.fetchall()]

def gerar_dataset_final():
    dfs = []

    with engine.connect() as conn:
        for ano in anos:
            print(f"\nGerando dataset para {ano}...")

            tabela_base = f"{ano}_microdados{ano}_arq2"
            tabela_arq1 = f"{ano}_microdados{ano}_arq1"

            if not tabela_existe(conn, tabela_base):
                print(f"Tabela base {tabela_base} não encontrada, pulando {ano}.")
                continue

            selects = ['e.*']
            joins = []
            join_counter = 1  # contador de aliases

            # Alias para ARQ1
            if tabela_existe(conn, tabela_arq1):
                alias_arq1 = f"a{join_counter}"
                join_counter += 1
                joins.append(
                    f'LEFT JOIN "{tabela_arq1}" {alias_arq1} ON e."CO_CURSO" = {alias_arq1}."CO_CURSO"'
                )
            else:
                print(f"Tabela ARQ1 {tabela_arq1} não encontrada, CO_GRUPO e UF não serão filtrados.")
                alias_arq1 = None

            # Joins para as outras tabelas
            for nome_arq, colunas in tabelas.items():
                if nome_arq in ["arq1", "arq2"]:
                    continue

                tabela_nome = f"{ano}_microdados{ano}_{nome_arq}"
                if not tabela_existe(conn, tabela_nome):
                    print(f"Tabela {tabela_nome} não existe, ignorando.")
                    continue

                cols_exist = colunas_existentes(conn, tabela_nome)
                cols_validas = [c for c in colunas if c in cols_exist]
                if not cols_validas:
                    continue

                alias = f"a{join_counter}"
                join_counter += 1

                selects += [f'{alias}."{c}"' for c in cols_validas]
                joins.append(
                    f'LEFT JOIN "{tabela_nome}" {alias} ON e."CO_CURSO" = {alias}."CO_CURSO"'
                )

            select_clause = ", ".join(selects)
            join_clause = "\n".join(joins)

            # Construção do WHERE
            if alias_arq1:
                cursos_str = ", ".join(map(str, cursos_ti))
                where_clause = (
                    f'WHERE {alias_arq1}."CO_GRUPO"::int = ANY(ARRAY[{cursos_str}]) '
                    f'AND {alias_arq1}."CO_UF_CURSO"::int = :uf_paraiba'
                )
            else:
                where_clause = ''  # sem filtro se ARQ1 não existir

            query = f"""
                SELECT {select_clause}
                FROM "{tabela_base}" e
                {join_clause}
                {where_clause}
            """

            df = pd.read_sql(
                text(query),
                conn,
                params={"uf_paraiba": uf_paraiba}
            )

            df["ANO"] = ano
            dfs.append(df)

            df.to_csv(f"enade_TI_PB_{ano}_dataset.csv", index=False, sep=';', encoding='utf-8')
            print(f"Dataset {ano} salvo com {len(df)} linhas.")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.to_csv("enade_TI_PB_dataset_final.csv", index=False, sep=';', encoding='utf-8')
        print(f"\nDataset final consolidado salvo com {len(df_final)} linhas.")

    dfs = []

    with engine.connect() as conn:
        for ano in anos:
            print(f"\nGerando dataset para {ano}...")

            tabela_base = f"{ano}_microdados{ano}_arq2"
            tabela_arq1 = f"{ano}_microdados{ano}_arq1"

            if not tabela_existe(conn, tabela_base):
                print(f"Tabela base {tabela_base} não encontrada, pulando {ano}.")
                continue

            selects = ['e.*']
            joins = []
            join_counter = 1  # contador de aliases

            # Alias para arq1 usando join_counter
            if tabela_existe(conn, tabela_arq1):
                alias_arq1 = f"a{join_counter}"
                join_counter += 1
                joins.append(
                    f'LEFT JOIN "{tabela_arq1}" {alias_arq1} ON e."CO_CURSO" = {alias_arq1}."CO_CURSO"'
                )
            else:
                print(f"Tabela ARQ1 {tabela_arq1} não encontrada, CO_GRUPO não será filtrado.")
                alias_arq1 = None

            # Joins para as outras tabelas
            for nome_arq, colunas in tabelas.items():
                if nome_arq in ["arq1", "arq2"]:
                    continue

                tabela_nome = f"{ano}_microdados{ano}_{nome_arq}"
                if not tabela_existe(conn, tabela_nome):
                    print(f"Tabela {tabela_nome} não existe, ignorando.")
                    continue

                cols_exist = colunas_existentes(conn, tabela_nome)
                cols_validas = [c for c in colunas if c in cols_exist]
                if not cols_validas:
                    continue

                alias = f"a{join_counter}"
                join_counter += 1

                selects += [f'{alias}."{c}"' for c in cols_validas]
                joins.append(
                    f'LEFT JOIN "{tabela_nome}" {alias} ON e."CO_CURSO" = {alias}."CO_CURSO"'
                )

            select_clause = ", ".join(selects)
            join_clause = "\n".join(joins)

            # Construção do WHERE usando ARRAY[...] para cursos TI
            if alias_arq1:
                cursos_str = ", ".join(map(str, cursos_ti))
                where_clause = (
                    f'WHERE {alias_arq1}."CO_GRUPO"::int = ANY(ARRAY[{cursos_str}]) '
                    f'AND e."CO_UF_CURSO"::int = :uf_paraiba'
                )
            else:
                where_clause = f'WHERE e."CO_UF_CURSO" = :uf_paraiba'

            query = f"""
                SELECT {select_clause}
                FROM "{tabela_base}" e
                {join_clause}
                {where_clause}
            """

            df = pd.read_sql(
                text(query),
                conn,
                params={"uf_paraiba": uf_paraiba}
            )

            df["ANO"] = ano
            dfs.append(df)

            df.to_csv(f"enade_TI_PB_{ano}_dataset.csv", index=False, sep=';', encoding='utf-8')
            print(f"Dataset {ano} salvo com {len(df)} linhas.")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.to_csv("enade_TI_PB_dataset_final.csv", index=False, sep=';', encoding='utf-8')
        print(f"\nDataset final consolidado salvo com {len(df_final)} linhas.")

    dfs = []

    with engine.connect() as conn:
        for ano in anos:
            print(f"\nGerando dataset para {ano}...")

            tabela_base = f"{ano}_microdados{ano}_arq2"
            tabela_arq1 = f"{ano}_microdados{ano}_arq1"

            if not tabela_existe(conn, tabela_base):
                print(f"Tabela base {tabela_base} não encontrada, pulando {ano}.")
                continue

            selects = ['e.*']
            joins = []
            join_counter = 1  

            if tabela_existe(conn, tabela_arq1):
                alias_arq1 = f"a{join_counter}"
                join_counter += 1
                joins.append(
                    f'LEFT JOIN "{tabela_arq1}" {alias_arq1} ON e."CO_CURSO" = {alias_arq1}."CO_CURSO"'
                )
            else:
                print(f"Tabela ARQ1 {tabela_arq1} não encontrada, CO_GRUPO não será filtrado.")
                alias_arq1 = None

            for nome_arq, colunas in tabelas.items():
                if nome_arq in ["arq1", "arq2"]:
                    continue

                tabela_nome = f"{ano}_microdados{ano}_{nome_arq}"
                if not tabela_existe(conn, tabela_nome):
                    print(f"Tabela {tabela_nome} não existe, ignorando.")
                    continue

                cols_exist = colunas_existentes(conn, tabela_nome)
                cols_validas = [c for c in colunas if c in cols_exist]
                if not cols_validas:
                    continue

                alias = f"a{join_counter}"
                join_counter += 1

                selects += [f'{alias}."{c}"' for c in cols_validas]
                joins.append(
                    f'LEFT JOIN "{tabela_nome}" {alias} ON e."CO_CURSO" = {alias}."CO_CURSO"'
                )

            select_clause = ", ".join(selects)
            join_clause = "\n".join(joins)

            if alias_arq1:
                where_clause = f'WHERE {alias_arq1}."CO_GRUPO"::int IN (:cursos_ti) AND e."CO_UF_CURSO" = :uf_paraiba'
            else:
                where_clause = f'WHERE e."CO_UF_CURSO"::int = :uf_paraiba'

            query = f"""
                SELECT {select_clause}
                FROM "{tabela_base}" e
                {join_clause}
                {where_clause}
            """

            df = pd.read_sql(
                text(query),
                conn,
                params={"cursos_ti": cursos_ti, "uf_paraiba": uf_paraiba}
            )

            df["ANO"] = ano
            dfs.append(df)

            df.to_csv(f"enade_TI_PB_{ano}_dataset.csv", index=False, sep=';', encoding='utf-8')
            print(f"Dataset {ano} salvo com {len(df)} linhas.")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.to_csv("enade_TI_PB_dataset_final.csv", index=False, sep=';', encoding='utf-8')
        print(f"\nDataset final consolidado salvo com {len(df_final)} linhas.")

    dfs = []

    with engine.connect() as conn:
        for ano in anos:
            print(f"\nGerando dataset para {ano}...")

            tabela_base = f"{ano}_microdados{ano}_arq2"
            tabela_arq1 = f"{ano}_microdados{ano}_arq1"

            if not tabela_existe(conn, tabela_base):
                print(f"Tabela base {tabela_base} não encontrada, pulando {ano}.")
                continue

            selects = ['e.*']
            joins = []
            join_counter = 1

            if tabela_existe(conn, tabela_arq1):
                alias_arq1 = f"a1"
                joins.append(
                    f'LEFT JOIN "{tabela_arq1}" {alias_arq1} ON e."CO_CURSO" = {alias_arq1}."CO_CURSO"'
                )
            else:
                print(f"Tabela ARQ1 {tabela_arq1} não encontrada, CO_GRUPO não será filtrado.")
                alias_arq1 = None

            for nome_arq, colunas in tabelas.items():
                if nome_arq in ["arq1", "arq2"]:
                    continue

                tabela_nome = f"{ano}_microdados{ano}_{nome_arq}"
                if not tabela_existe(conn, tabela_nome):
                    print(f"Tabela {tabela_nome} não existe, ignorando.")
                    continue

                cols_exist = colunas_existentes(conn, tabela_nome)
                cols_validas = [c for c in colunas if c in cols_exist]
                if not cols_validas:
                    continue

                alias = f"{nome_arq[0]}{join_counter}"
                join_counter += 1

                selects += [f'{alias}."{c}"' for c in cols_validas]
                joins.append(
                    f'LEFT JOIN "{tabela_nome}" {alias} ON e."CO_CURSO" = {alias}."CO_CURSO"'
                )

            select_clause = ", ".join(selects)
            join_clause = "\n".join(joins)

            if alias_arq1:
                where_clause = f'WHERE {alias_arq1}."CO_GRUPO"::int IN (:cursos_ti) AND e."CO_UF_CURSO" = :uf_paraiba'
            else:
                where_clause = f'WHERE e."CO_UF_CURSO"::int = :uf_paraiba'

            query = f"""
                SELECT {select_clause}
                FROM "{tabela_base}" e
                {join_clause}
                {where_clause}
            """

            df = pd.read_sql(
                text(query),
                conn,
                params={"cursos_ti": cursos_ti, "uf_paraiba": uf_paraiba}
            )

            df["ANO"] = ano
            dfs.append(df)

            df.to_csv(f"enade_TI_PB_{ano}_dataset.csv", index=False, sep=';', encoding='utf-8')
            print(f"Dataset {ano} salvo com {len(df)} linhas.")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.to_csv("enade_TI_PB_dataset_final.csv", index=False, sep=';', encoding='utf-8')
        print(f"\nDataset final consolidado salvo com {len(df_final)} linhas.")

if __name__ == "__main__":
    gerar_dataset_final()
