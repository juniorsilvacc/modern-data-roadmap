import datetime
from sqlalchemy import text, inspect
from datetime import datetime, timedelta

def load_staging_incremental(df, table_name, date_column, engine):
    """
    Faz a carga incremental: se a tabela existe, deleta os últimos 
    60 dias e insere os novos para evitar duplicados e atualizar dados.
    """
    schema_name = 'public'
    inspector = inspect(engine)
    
    # Verifica se a tabela já existe no Postgres
    table_exists = inspector.has_table(table_name, schema=schema_name)

    if not table_exists:
        # --- FULL LOAD (Primeira Execução) ---
        print(f"{table_name} não encontrada. Realizando FULL LOAD inicial...")
        df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        print(f"✅ Full Load concluído: {len(df)} registros.")
    
    else:
        # --- INCREMENTAL (Janela de 60 dias) ---
        cutoff_date = (datetime.now() - timedelta(days=60))
        dt_str = cutoff_date.strftime('%Y-%m-%d')
        
        print(f"Tabela encontrada. Aplicando lógica incremental (Janela: >= {dt_str})...")
        
        # Filtra o que vai ser inserido, apenas o que aconteceu nos últimos 60 dias
        df_incremental = df[df[date_column] >= cutoff_date].copy()
        
        if df_incremental.empty:
            print(f"⚠️  Nenhum dado novo nos últimos 60 dias para {table_name}.")
            return

        with engine.begin() as connection:
            # Deleta a janela de 60 dias para evitar duplicados
            delete_query = text(f"DELETE FROM {schema_name}.{table_name} WHERE {date_column} >= :d")
            connection.execute(delete_query, {"d": dt_str})
            
            # Insere o incremental
            df_incremental.to_sql(table_name, connection, schema=schema_name, if_exists='append', index=False)
            
        print(f"✅ Incremental concluído: {len(df_incremental)} registros atualizados.")

def load_staging(df, table_name, engine):
    """
    Carga simples via Append: cria a tabela se não existir 
    ou apenas adiciona os novos dados ao final da tabela.
    """
    schema_name = 'public'
    inspector = inspect(engine)
    
    table_exists = inspector.has_table(table_name, schema=schema_name)
    
    if not table_exists:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
        print(f"✅ Tabela {table_name} criada e {len(df)} registros salvos!")
    else:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
        print(f"✅ {len(df)} novos registros adicionados à {table_name}!")
