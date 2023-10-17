
{% if var('UpscribeCollections') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


-- {% if is_incremental() %}
-- {%- set max_loaded_query -%}
-- SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
-- {% endset %}

-- {%- set max_loaded_results = run_query(max_loaded_query) -%}

-- {%- if execute -%}
-- {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
-- {% else %}
-- {% set max_loaded = 0 %}
-- {%- endif -%}
-- {% endif %}

-- {% set table_name_query %}
-- {{set_table_name('%upscribe%collections')}}    
-- {% endset %}  

-- {% set results = run_query(table_name_query) %}

-- {% if execute %}
--     {# Return the first column #}
--     {% set results_list = results.columns[0].values() %}
--     {% set tables_lowercase_list = results.columns[1].values() %}
-- {% else %}
--     {% set results_list = [] %}
--     {% set tables_lowercase_list = [] %}
-- {% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('upscribe_collection_ptrn'),
exclude=var('upscribe_collection_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        body_html,	
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,		
        handle,		
        id,		
        image,
        sort_order,		
        cast(store_id as string) as store_id,		
        title,			
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
       
        from {{i}} a
            
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
              where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('upscribe_collections_lookback') }},0) from {{ this }})
            --WHERE 1=1
            {% endif %}
            qualify
            dense_rank() OVER (partition by id,store_id order by {{daton_batch_runtime()}} desc) =1
       
    {% if not loop.last %} union all {% endif %}
    {% endfor %}