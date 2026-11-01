{#- Raw tables: on DuckDB read the CSVs directly; on BigQuery they are expected to be loaded into dataset `raw`. -#}
{% macro raw_source(name) -%}
  {%- if target.type == 'duckdb' -%}
    read_csv_auto('{{ env_var("RAW_DIR", "data/raw") }}/{{ name }}.csv', header = true)
  {%- else -%}
    {{ source('raw', name) }}
  {%- endif -%}
{%- endmacro %}
