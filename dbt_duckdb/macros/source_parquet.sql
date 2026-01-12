{% macro source_parquet(source_name, table_name) %}
  {%- set base = var('bronze_base_path') -%}
  {%- if table_name == 'customers' -%}
    read_parquet('{{ base }}/customers/signup_date=*/part-*.parquet', union_by_name=true, hive_partitioning=false)
  {%- elif table_name == 'product_catalog' -%}
    read_parquet('{{ base }}/product_catalog/category=*/*.parquet', union_by_name=true, hive_partitioning=false)
  {%- else -%}
    read_parquet('{{ base }}/{{ table_name }}/ingest_dt=*/part-*.parquet', union_by_name=true, hive_partitioning=false)
  {%- endif -%}
{%- endmacro %}
