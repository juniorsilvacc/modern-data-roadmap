from src.database import get_engine
from src.transform import run_etl_process
from src.marts import create_marts

if __name__ == "__main__":
    print("🚀 Iniciando Pipeline: CSV -> Parquet -> Postgres")
    
    try:
        engine = get_engine()

        run_etl_process()
        
        create_marts(engine)
        
        print("\n✅ Pipeline finalizado com sucesso!")
    except Exception as e:
        print(f"Falha no processo: {e}")