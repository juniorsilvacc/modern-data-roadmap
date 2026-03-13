import pandas as pd
import glob
import os
from src.database import get_engine
from .load_staging import load_staging_incremental

RENAME = {
    "stg_product": {
        "productid": "id_produto",
        "name": "nome_produto",
        "productnumber": "codigo_produto",
        "makeflag": "flag_fabricacao",
        "finishedgoodsflag": "flag_produto_finalizado",
        "color": "cor",
        "safetystocklevel": "nivel_estoque_seguranca",
        "reorderpoint": "ponto_reposicao",
        "standardcost": "custo_padrao",
        "listprice": "preco_lista",
        "size": "tamanho",
        "sizeunitmeasurecode": "cod_unidade_medida_tamanho",
        "weightunitmeasurecode": "cod_unidade_medida_peso",
        "weight": "peso",
        "daystomanufacture": "dias_fabricacao",
        "productline": "linha_produto",
        "class": "classe",
        "style": "estilo",
        "productsubcategoryid": "id_subcategoria_produto",
        "productmodelid": "id_modelo_produto",
        "sellstartdate": "data_inicio_venda",
        "sellenddate": "data_fim_venda",
        "discontinueddate": "data_descontinuado",
        "rowguid": "guid_linha",
        "modifieddate": "data_modificacao"
    },
    "stg_salesorderdetail": {
        "salesorderid": "id_pedido",
        "salesorderdetailid": "id_detalhe_pedido",
        "carriertrackingnumber": "numero_rastreio",
        "orderqty": "quantidade",
        "productid": "id_produto",
        "specialofferid": "id_oferta_especial",
        "unitprice": "preco_unitario",
        "unitpricediscount": "desconto_unitario",
        "linetotal": "total_linha",
        "rowguid": "guid_linha",
        "modifieddate": "data_modificacao"
    },
    "stg_salesorderheader": {
        "salesorderid": "id_pedido",
        "revisionnumber": "numero_revisao",
        "orderdate": "data_pedido",
        "duedate": "data_vencimento",
        "shipdate": "data_envio",
        "status": "status_pedido",
        "onlineorderflag": "flag_pedido_online",
        "salesordernumber": "numero_pedido",
        "purchaseordernumber": "numero_pedido_compra",
        "accountnumber": "numero_conta",
        "customerid": "id_cliente",
        "salespersonid": "id_vendedor",
        "territoryid": "id_territorio",
        "billtoaddressid": "id_endereco_faturamento",
        "shiptoaddressid": "id_endereco_envio",
        "shipmethodid": "id_metodo_envio",
        "creditcardid": "id_cartao_credito",
        "creditcardapprovalcode": "cod_aprovacao_cartao",
        "currencyrateid": "id_taxa_cambio",
        "subtotal": "subtotal",
        "taxamt": "valor_imposto",
        "freight": "valor_frete",
        "totaldue": "valor_total",
        "comment": "comentario",
        "rowguid": "guid_linha",
        "modifieddate": "data_modificacao"
    }
}

MAPPING_DATA = {
    'SalesOrderHeader': 'orderdate',
    'SalesOrderDetail': 'modifieddate',
    'Product': 'modifieddate'
}

def run_etl_process():
    engine = get_engine()
    os.makedirs('data/staging', exist_ok=True)
    
    arquivos = glob.glob('data/raw/*.csv')

    for caminho in arquivos:
        nome_base = os.path.basename(caminho).replace('.csv', '')
        coluna_data = MAPPING_DATA.get(nome_base, 'modifieddate').lower()
        tabela_nome = f"stg_{nome_base.lower()}"
        
        print(f"\nProcessando: {nome_base}")

        # 1. LEITURA
        df = pd.read_csv(caminho, sep=None, engine='python')
        
        # 2. LIMPEZA
        df.columns = [col.lower().strip() for col in df.columns]
        df = df.drop_duplicates()
        df = df.dropna(how='all')
        
        # 3. CASTING ESPECÍFICO 
        if nome_base == 'Product':
            # Datas
            for col in ['sellstartdate', 'sellenddate', 'modifieddate']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    
            # IDs
            for col in ['productid', 'productsubcategoryid', 'productmodelid', 'safetystocklevel', 'reorderpoint']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            # Preenchimento de Nulos para colunas de texto
            for col in ['color', 'size']:
                if col in df.columns:
                    df[col] = df[col].fillna('NA')
        
        elif nome_base == 'SalesOrderDetail':
            # Data
            if 'modifieddate' in df.columns:
                df['modifieddate'] = pd.to_datetime(df['modifieddate'], errors='coerce')
                
            # IDs
            for col in ['salesorderid', 'salesorderdetailid', 'productid', 'specialofferid']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
            # Financeiro
            for col in ['unitprice', 'unitpricediscount', 'linetotal']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        elif nome_base == 'SalesOrderHeader':
            # Datas
            for col in ['orderdate', 'duedate', 'shipdate', 'modifieddate']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # IDs
            ids_header = [
                'salesorderid', 'customerid', 'salespersonid', 'territoryid', 
                'billtoaddressid', 'shiptoaddressid', 'shipmethodid', 
                'creditcardid', 'currencyrateid'
            ]
            for col in ids_header:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            # Campos de texto ou valores financeiros
            if 'totaldue' in df.columns:
                df['totaldue'] = pd.to_numeric(df['totaldue'], errors='coerce').fillna(0.0)
            
            if 'comment' in df.columns:
                df['comment'] = df['comment'].astype(str).replace(['nan', 'None'], '')

        # 4. RENOMEAÇÃO
        if tabela_nome in RENAME:
            print(f"Renomeando colunas para {tabela_nome}...")
            coluna_data_final = RENAME[tabela_nome].get(coluna_data, coluna_data)
            df = df.rename(columns=RENAME[tabela_nome])
        else:
            coluna_data_final = coluna_data
            
        # 5. SALVAR EM PARQUET
        parquet_path = f'data/staging/{tabela_nome}.parquet'
        df.to_parquet(parquet_path, index=False)
        print(f"Parquet atualizado: {parquet_path} ({len(df)} linhas)")

        # 6. CARREGAR NO POSTGRES
        load_staging_incremental(df, tabela_nome, coluna_data_final, engine)
