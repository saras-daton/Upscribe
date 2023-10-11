
{% if var('UpscribeProducts') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%upscribe%products')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
    {% set results_list = [] %}
    {% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
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
        a.body_html,		
        {{extract_nested_value("collections","body_html","string")}} as collections_body_html,
        {{extract_nested_value("collections","created_at","TIMESTAMP")}} as collections_created_at,
        {{extract_nested_value("collections","handle","string")}} as collections_handle,
        {{extract_nested_value("collections","id","NUMERIC")}} as collections_id,
        {{extract_nested_value("collections","sort_order","string")}} as collections_sort_order,
        {{extract_nested_value("collections","store_id","NUMERIC")}} as collections_store_id,
        {{extract_nested_value("collections","title","string")}} as collections_title,
        {{extract_nested_value("collections","updated_at","TIMESTAMP")}} as collections_updated_at,
        {{extract_nested_value("image","alt","boolean")}} as image_alt,
        {{extract_nested_value("image","created_at","TIMESTAMP")}} as image_created_at,
        {{extract_nested_value("image","height","NUMERIC")}} as image_height,
        {{extract_nested_value("image","src","string")}} as image_src,
        {{extract_nested_value("image","width","NUMERIC")}} as image_weight,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,			
        a.handle,		
        coalesce(a.id,0) as id ,
       
        --image,
        in_sales_channel,		
        is_subscription,		
        --options,
       
        {{extract_nested_value("images","admin_graphql_api_id","string")}} as images_admin_graphql_api_id,
        {{extract_nested_value("images","alt","boolean")}} as images_alt,
        {{extract_nested_value("images","created_at","TIMESTAMP")}} as images_created_at,
        {{extract_nested_value("images","height","NUMERIC")}} as images_height,
        {{extract_nested_value("images","id","NUMERIC")}} as images_id,
        {{extract_nested_value("images","position","NUMERIC")}} as images_position,
        {{extract_nested_value("images","product_id","NUMERIC")}} as images_product_id,
        {{extract_nested_value("images","src","string")}} as images_src,
        {{extract_nested_value("images","updated_at","TIMESTAMP")}} as images_updated_at,
        {{extract_nested_value("images","width","NUMERIC")}} as images_width,

        cast(a.store_id as string) as store_id,		
        template_suffix,		
        a.title,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        --variants,
        {{extract_nested_value("metafields","admin_graphql_api_id","string")}} as metafields_admin_graphql_api_id,
        {{extract_nested_value("metafields","created_at","TIMESTAMP")}} as metafields_created_at,
        {{extract_nested_value("metafields","description","boolean")}} as metafields_description,
        {{extract_nested_value("metafields","id","numeric")}} as metafields_id,
        {{extract_nested_value("metafields","key","string")}} as metafields_key,
        {{extract_nested_value("metafields","namespace","string")}} as metafields_namespace,
        {{extract_nested_value("metafields","owner_id","numeric")}} as metafields_owner_id,
        {{extract_nested_value("metafields","owner_resource","string")}} as metafields_owner_resource,
        {{extract_nested_value("metafields","updated_at","TIMESTAMP")}} as metafields_updated_at,
        {{extract_nested_value("metafields","value","string")}} as metafields_value,
        {{extract_nested_value("metafields","value_type","string")}} as metafields_value_type,
        vendor,	
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        
        from {{i}} a 
            {{unnesting("images")}}
            {{unnesting("metafields")}}
            {{unnesting("collections")}}
            {{multi_unnesting("collections","image")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
            qualify
            dense_rank() OVER (partition by a.id order by {{daton_batch_runtime()}} desc) =1
        
    
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
