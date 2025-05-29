import pandas as pd
import logging
from google.cloud import bigquery
import os
from src.common.utils import ensure_bigquery_dataset_exists

logger = logging.getLogger(__name__)

def load_df_to_staging_table(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    staging_table_id: str,
    gcp_location: str = "southamerica-east1"
) -> bool:
    if df.empty:
        logger.info(f"DataFrame para staging em {dataset_id}.{staging_table_id} está vazio. Nenhum dado para carregar.")
        return True

    try:
        client = bigquery.Client(project=project_id)
        ensure_bigquery_dataset_exists(client, dataset_id, project_id, location=gcp_location)
    except Exception as e:
        logger.error(f"Falha ao inicializar o cliente BigQuery ou garantir a existência do dataset {project_id}.{dataset_id}: {e}")
        return False

    table_ref_full = f"{project_id}.{dataset_id}.{staging_table_id}"
    logger.info(f"Iniciando carregamento de {len(df)} linhas para a tabela de STAGING: {table_ref_full}")

    schema = [
        bigquery.SchemaField("data_referencia", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("codigo_serie", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("valor_serie", "FLOAT64", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE", 
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_referencia"
        )
    )

    try:
        load_job = client.load_table_from_dataframe(
            df, table_ref_full, job_config=job_config
        )
        logger.info(f"Job de carregamento para STAGING {load_job.job_id} iniciado para {table_ref_full}.")
        load_job.result()

        if load_job.errors:
            logger.error(f"Job de carregamento para STAGING {table_ref_full} encontrou erros:")
            for error in load_job.errors:
                logger.error(f" - {error['message']}")
            return False
        
        logger.info(f"Carregamento para STAGING {table_ref_full} concluído com sucesso.")
        return True

    except Exception as e:
        logger.error(f"Erro durante o carregamento de dados para STAGING {table_ref_full}: {e}")
        return False
    
def merge_data_to_final_table(
    project_id: str,
    dataset_id: str,
    staging_table_id: str,
    final_table_id: str,
    gcp_location: str = "southamerica-east1"
) -> bool:
    
    try:
        client = bigquery.Client(project=project_id)
        ensure_bigquery_dataset_exists(client, dataset_id, project_id, location=gcp_location)

    except Exception as e:
        logger.error(f"Falha ao inicializar o cliente BigQuery ou garantir a existência do dataset {project_id}.{dataset_id} (função MERGE): {e}")
        return False

    final_table_ref = client.dataset(dataset_id).table(final_table_id)
    final_table_full_id_for_sql = f"`{project_id}.{dataset_id}.{final_table_id}`"
    staging_table_full_id_for_sql = f"`{project_id}.{dataset_id}.{staging_table_id}`"

    logger.info(f"Verificando/Criando tabela final: {final_table_full_id_for_sql}")
    schema_final = [
        bigquery.SchemaField("data_referencia", "DATE", mode="NULLABLE", description="Data de referência do indicador"),
        bigquery.SchemaField("codigo_serie", "INTEGER", mode="NULLABLE", description="Código da série numérica do BCB SGS"),
        bigquery.SchemaField("valor_serie", "FLOAT64", mode="NULLABLE", description="Valor do indicador na data de referência"),
    ]
    
    table_definition = bigquery.Table(final_table_ref, schema=schema_final)
    table_definition.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="data_referencia"
    )
    table_definition.clustering_fields = ["codigo_serie"]

    try:
        client.create_table(table_definition, exists_ok=True) 
        logger.info(f"Tabela final {final_table_full_id_for_sql} verificada/criada com sucesso com esquema, particionamento e clustering.")
    except Exception as e:
        logger.error(f"Falha ao criar/verificar a tabela final {final_table_full_id_for_sql}: {e}")
        return False

    merge_join_keys = "target.data_referencia = source.data_referencia AND target.codigo_serie = source.codigo_serie"
    update_set_clause = "target.valor_serie = source.valor_serie"
    insert_columns = "(data_referencia, codigo_serie, valor_serie)"
    source_columns_for_insert = "(source.data_referencia, source.codigo_serie, source.valor_serie)"

    merge_sql = f"""
    MERGE {final_table_full_id_for_sql} AS target
    USING (SELECT DISTINCT * FROM {staging_table_full_id_for_sql}) AS source
    ON {merge_join_keys}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT {insert_columns}
        VALUES {source_columns_for_insert};
    """

    logger.info(f"Executando MERGE da staging table {staging_table_full_id_for_sql} para a final table {final_table_full_id_for_sql}.")
    logger.debug(f"Consulta MERGE:\n{merge_sql}")

    try:
        query_job = client.query(merge_sql)
        query_job.result()

        if query_job.errors:
            logger.error(f"Operação MERGE para {final_table_full_id_for_sql} falhou com erros:")
            for error in query_job.errors:
                logger.error(f" - {error['message']}")
            return False

        rows_affected_message = "não disponível"
        if query_job.num_dml_affected_rows is not None:
            rows_affected_message = str(query_job.num_dml_affected_rows)
        
        logger.info(f"Operação MERGE para {final_table_full_id_for_sql} concluída com sucesso.")
        logger.info(f"Linhas afetadas pela operação MERGE: {rows_affected_message}.")
        return True

    except Exception as e:
        logger.error(f"Erro durante a operação MERGE para {final_table_full_id_for_sql}: {e}")
        if 'query_job' in locals() and query_job and query_job.errors:
             logger.error("Detalhes do erro do job de query (MERGE):")
             for error_detail in query_job.errors:
                logger.error(f"  Message: {error_detail.get('message', 'N/A')}, Reason: {error_detail.get('reason', 'N/A')}, Location: {error_detail.get('location', 'N/A')}")
        return False