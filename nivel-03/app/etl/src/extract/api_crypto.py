import os
import json
import requests
import pandas as pd
from datetime import datetime

from src.core.db import get_engine
from src.core.config import LANDING_DIR, RAW_DIR
from src.core.load_raw import load_raw

def extract_crypto_to_raw():
    """
    Consome a API CoinGecko, salva o JSON bruto e carrega o Parquet no Postgres.
    """
    engine = get_engine()
    table_name = 'raw_crypto'
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 50}
    
    print(f"🌐 Acessando API CoinGecko...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # 1. SALVAR RAW
        file_path = os.path.join(LANDING_DIR, 'crypto_data.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"✅ Arquivo bruto salvo em: {file_path}")

        # 2. NORMALIZAR
        df = pd.json_normalize(data)
        df['extracted_at'] = datetime.now()
        df.columns = [col.replace('.', '_').lower() for col in df.columns]
        
        # 3. SALVAR RAW
        parquet_path = os.path.join(RAW_DIR, 'raw_crypto.parquet')
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Parquet de cripto atualizado em: {parquet_path}")
        
        load_raw(df, table_name, engine)
        
    except Exception as e:
        print(f"Erro no processo de ETL de Cripto: {e}")
        return None
