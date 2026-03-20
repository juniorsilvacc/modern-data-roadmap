from src.extract.file_sales import process_sales_csv
from src.extract.api_crypto import extract_crypto_to_staging

if __name__ == "__main__":
    print("🚀 Iniciando Pipeline: CSV -> Parquet -> Postgres")
    
    try:
        process_sales_csv()
        
        extract_crypto_to_staging()
        
        print("\n✅ Pipeline finalizado com sucesso!")
    except Exception as e:
        print(f"Falha no processo: {e}")