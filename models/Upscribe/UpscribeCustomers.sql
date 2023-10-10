
{% if var('UpscribeCustomers') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%upscribe%customers')}}    
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
        accepts_marketing,
        active_subscription_count,	
        --addresses,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        {{extract_nested_value("default_address","address1","string")}} as default_address_address1,
        {{extract_nested_value("default_address","address2","string")}} as default_address_address2, 
        {{extract_nested_value("default_address","city","string")}} as default_address_city, 
        {{extract_nested_value("default_address","company","string")}} as default_address_company,
        {{extract_nested_value("default_address","country","string")}} as default_address_country,	
        {{extract_nested_value("default_address","country_code","string")}} as default_address_country_code,	
        {{extract_nested_value("default_address","country_name","string")}} as default_address_country_name,						
        {{extract_nested_value("default_address","customer_id","NUMERIC")}} as default_address_customer_id,			
    	{{extract_nested_value("default_address","default","string")}} as default_address_default,		
    	{{extract_nested_value("default_address","first_name","string")}} as default_address_first_name,		
        {{extract_nested_value("default_address","last_name","string")}} as default_address_last_name,		
        {{extract_nested_value("default_address","id","NUMERIC")}} as default_address_id,
        {{extract_nested_value("default_address","name","string")}} as default_address_name,		
		{{extract_nested_value("default_address","phone","string")}} as default_address_phone,	
        {{extract_nested_value("default_address","province","string")}} as default_address_province,		
	    {{extract_nested_value("default_address","province_code","string")}} as default_address_province_code,		
        {{extract_nested_value("default_address","zip","string")}} as default_address_zip,
	    email,		
        a.first_name,		
        coalesce(a.id,0)as id,		
        inactive_subscription_count,		
        language,		
        a.last_name,		
        a.phone,		
        state,		
        cast(store_id as string) as store_id,		
        updated_at,	
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        
        from {{i}} a  
            {{unnesting("default_address")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
            qualify
            DENSE_RANK() OVER (PARTITION BY a.id order by {{daton_batch_runtime()}} desc) =1
        
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
