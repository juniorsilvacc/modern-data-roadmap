import os
import glob
import pandas as pd
from src.core.db import get_engine
from src.core.config import LANDING_DIR, RAW_DIR
from src.core.load_raw import load_raw_incremental

RENAME = {
    "raw_product": {
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
    "raw_salesorderdetail": {
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
    "raw_salesorderheader": {
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

def process_sales_csv_to_raw():
    """
    Lê CSVs da pasta landing, transforma os dados e carrega no Postgres via Staging Incremental.
    """
    engine = get_engine()
    
    files = glob.glob(os.path.join(LANDING_DIR, '*.csv'))
    
    if not files:
        print("Nenhum arquivo CSV encontrado em data/landing")
        return

    for path in files:
        base_name = os.path.basename(path).replace('.csv', '')
        column_data = MAPPING_DATA.get(base_name, 'modifieddate').lower()
        table_name = f"raw_{base_name.lower()}"

        df = pd.read_csv(path, sep=None, engine='python')

        # 1. LIMPEZA BÁSICA DE COLUNA
        df.columns = [col.lower().strip() for col in df.columns]
        
        # 2. RENOMEAÇÃO
        if table_name in RENAME:
            column_end_date = RENAME[table_name].get(column_data, column_data)
            df = df.rename(columns=RENAME[table_name])
        else:
            column_end_date = column_data
        
        # 3. CONVERSÃO
        if column_end_date in df.columns:
            df[column_end_date] = pd.to_datetime(df[column_end_date], errors='coerce')
            df = df.dropna(subset=[column_end_date])
            
        # 4. SALVAR PARQUET
        file_name = f"{table_name}.parquet"
        parquet_path = os.path.join(RAW_DIR, file_name)
        df.to_parquet(parquet_path, index=False)
        print(f"Parquet atualizado: {parquet_path} ({len(df)} linhas)")

        # 6. CARREGAR NO POSTGRES
        load_raw_incremental(df, table_name, column_end_date, engine)
