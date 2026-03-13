from src.transform import run_etl_process

if __name__ == "__main__":
    print("🚀 Iniciando Pipeline: CSV -> Parquet -> Postgres")
    try:
        run_etl_process()
    except Exception as e:
        print(f"Falha no processo: {e}")