-- Elementary fabricspark adapter support
-- This package provides fabricspark-specific macro implementations
-- Copied from Elementary's spark__ implementations with find & replace spark__ -> fabricspark__

-- ==============================================================================
-- DATA TYPES
-- ==============================================================================

{% macro fabricspark__edr_type_string() %}
    {% do return("string") %}
{% endmacro %}

{% macro fabricspark__data_type_list(data_type) %}
    {% set string_list = ['string'] | list %}
    {% set numeric_list = ['int','bigint','smallint','tinyint','float','double','long','short','decimal'] | list %}
    {% set timestamp_list = ['timestamp','date'] | list %}
    {% set boolean_list = ["boolean"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}

{% macro fabricspark__get_normalized_data_type(exact_data_type) %}
{# fabricspark also has no concept of data type synonyms :
   see https://spark.apache.org/docs/latest/sql-ref-datatypes.html #}
   {{return (exact_data_type) }}
{% endmacro %}

-- ==============================================================================
-- TIMESTAMP & DATE FUNCTIONS
-- ==============================================================================

{% macro fabricspark__edr_current_timestamp() %}
    cast(current_timestamp() as timestamp)
{% endmacro %}

{% macro fabricspark__edr_current_timestamp_in_utc() %}
    cast(unix_timestamp() as timestamp)
{% endmacro %}

{% macro fabricspark__edr_datediff(first_date, second_date, datepart) %}
    {%- if datepart in ['day', 'week', 'month', 'quarter', 'year'] -%}

        {# make sure the dates are real, otherwise raise an error asap #}
        {% set first_date = assert_not_null('date', first_date) %}
        {% set second_date = assert_not_null('date', second_date) %}

    {%- endif -%}

    {%- if datepart == 'day' -%}

        datediff({{second_date}}, {{first_date}})

    {%- elif datepart == 'week' -%}

        case when {{first_date}} < {{second_date}}
            then floor(datediff({{second_date}}, {{first_date}})/7)
            else ceil(datediff({{second_date}}, {{first_date}})/7)
            end

        -- did we cross a week boundary (Sunday)?
        + case
            when {{first_date}} < {{second_date}} and dayofweek({{second_date}}) < dayofweek({{first_date}}) then 1
            when {{first_date}} > {{second_date}} and dayofweek({{second_date}}) > dayofweek({{first_date}}) then -1
            else 0 end

    {%- elif datepart == 'month' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(date({{second_date}}), date({{first_date}})))
            else ceil(months_between(date({{second_date}}), date({{first_date}})))
            end

        -- did we cross a month boundary?
        + case
            when {{first_date}} < {{second_date}} and dayofmonth({{second_date}}) < dayofmonth({{first_date}}) then 1
            when {{first_date}} > {{second_date}} and dayofmonth({{second_date}}) > dayofmonth({{first_date}}) then -1
            else 0 end

    {%- elif datepart == 'quarter' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(date({{second_date}}), date({{first_date}}))/3)
            else ceil(months_between(date({{second_date}}), date({{first_date}}))/3)
            end

        -- did we cross a quarter boundary?
        + case
            when {{first_date}} < {{second_date}} and (
                (dayofyear({{second_date}}) - (quarter({{second_date}}) * 365/4))
                < (dayofyear({{first_date}}) - (quarter({{first_date}}) * 365/4))
            ) then 1
            when {{first_date}} > {{second_date}} and (
                (dayofyear({{second_date}}) - (quarter({{second_date}}) * 365/4))
                > (dayofyear({{first_date}}) - (quarter({{first_date}}) * 365/4))
            ) then -1
            else 0 end

    {%- elif datepart == 'year' -%}

        year({{second_date}}) - year({{first_date}})

    {%- elif datepart in ('hour', 'minute', 'second', 'millisecond', 'microsecond') -%}

        {%- set divisor -%}
            {%- if datepart == 'hour' -%} 3600
            {%- elif datepart == 'minute' -%} 60
            {%- elif datepart == 'second' -%} 1
            {%- elif datepart == 'millisecond' -%} (1/1000)
            {%- elif datepart == 'microsecond' -%} (1/1000000)
            {%- endif -%}
        {%- endset -%}

        case when {{first_date}} < {{second_date}}
            then floor((
                {# make sure the timestamps are real, otherwise raise an error asap #}
                {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', second_date)) }}
                - {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', first_date)) }}
            ) / {{divisor}})
            else ceil((
                {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', second_date)) }}
                - {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', first_date)) }}
            ) / {{divisor}})
            end

            {% if datepart == 'millisecond' %}
                + cast(date_format({{second_date}}, 'SSS') as int)
                - cast(date_format({{first_date}}, 'SSS') as int)
            {% endif %}

            {% if datepart == 'microsecond' %}
                {% set capture_str = '[0-9]{4}-[0-9]{2}-[0-9]{2}.[0-9]{2}:[0-9]{2}:[0-9]{2}.([0-9]{6})' %}
                -- Spark doesn't really support microseconds, so this is a massive hack!
                -- It will only work if the timestamp-string is of the format
                -- 'yyyy-MM-dd-HH mm.ss.SSSSSS'
                + cast(regexp_extract({{second_date}}, '{{capture_str}}', 1) as int)
                - cast(regexp_extract({{first_date}}, '{{capture_str}}', 1) as int)
            {% endif %}

    {%- else -%}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Spark") }}

    {%- endif -%}

{% endmacro %}

{% macro fabricspark__edr_to_char(column, format) %}
    date_format({{ column }} {%- if format %}, '{{ format }}'){%- else %}, 'YYYY-MM-DD HH:MI:SS'){%- endif %}
{% endmacro %}

-- ==============================================================================
-- CASTING & TYPE CONVERSION
-- ==============================================================================

{% macro fabricspark__edr_safe_cast(field, type) %}
    try_cast({{field}} as {{type}})
{% endmacro %}

-- ==============================================================================
-- TABLE OPERATIONS
-- ==============================================================================

{% macro fabricspark__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro fabricspark__edr_make_temp_relation(base_relation, suffix) %}
    {#-- Fix: Do NOT set schema to none. Fabric schema-enabled lakehouses
         require the full 4-part path (workspace.lakehouse.schema.table).
         Setting schema=none drops it to 3-part, causing:
         "Artifact not found: <workspace>.<workspace>"
         Keep the base relation's schema so temp lands in the same schema. --#}
    {% set tmp_identifier = elementary.table_name_with_suffix(base_relation.identifier, suffix) %}
    {% set tmp_relation = base_relation.incorporate(path = {
        "identifier": tmp_identifier
    }) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro fabricspark__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true, should_commit=true) %}
{% endmacro %}

{% macro fabricspark__replace_table_data(relation, rows) %}
    {% call statement('truncate_relation') -%}
        delete from {{ relation }} where 1=1
    {%- endcall %}
    {% do elementary.insert_rows(relation, rows, should_commit=false, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
{% endmacro %}

{% macro fabricspark__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {# Fabric Lakehouse tables are always Delta. Delta does not support subqueries in DELETE,
       so we always use MERGE instead. The spark__ version checks relation.is_delta which is
       databricks-specific and not available on fabricspark adapter. #}
    {% if delete_relation %}
        {% set delete_query %}
            merge into {{ relation }} as source
            using {{ delete_relation }} as target
            on (source.{{ delete_column_key }} = target.{{ delete_column_key }}) or source.{{ delete_column_key }} is null
            when matched then delete;
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}

{% macro fabricspark__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(127) }}
{% endmacro %}

-- ==============================================================================
-- METADATA & CONFIGURATION
-- ==============================================================================

{% macro fabricspark__target_database() %}
    {% do return(target.catalog or none) %}
{% endmacro %}

{% macro fabricspark__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {{ elementary.get_empty_columns_from_information_schema_table() }}
{% endmacro %}

{% macro fabricspark__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    _parameter("type", target.type),
    _parameter("host", target.host),
    _parameter("http_path", "<HTTP PATH>")
  ] %}
  {% if elementary_database %}
    {% do parameters.append(_parameter("catalog", elementary_database)) %}
  {% endif %}
  {% do parameters.extend([
    _parameter("schema", elementary_schema),
    _parameter("token", "<TOKEN>"),
    _parameter("threads", target.threads),
  ]) %}
  {% do return(parameters) %}
{% endmacro %}

-- ==============================================================================
-- BUCKET OPERATIONS
-- ==============================================================================

{% macro fabricspark__complete_buckets_cte(time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from (select explode(sequence({{ min_bucket_start_expr }}, {{ max_bucket_end_expr }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start)
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

-- ==============================================================================
-- CLEANUP OPERATIONS
-- ==============================================================================

{% macro fabricspark__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transactionless_clean_elementary_test_tables_queries(test_table_relations)) %}
{% endmacro %}
