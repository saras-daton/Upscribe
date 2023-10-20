
{% if var('UpscribeCustomers') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('upscribe_customers_ptrn'),
exclude=var('upscribe_customers_tbl_exclude_ptrn'),
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
        accepts_marketing,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        {{extract_nested_value("addresses","address1","string")}} as addresses_address1,
        {{extract_nested_value("addresses","address2","string")}} as addresses_address2, 
        {{extract_nested_value("addresses","city","string")}} as addresses_city, 
        {{extract_nested_value("addresses","company","string")}} as addresses_company,
        {{extract_nested_value("addresses","country","string")}} as addresses_country,	
        {{extract_nested_value("addresses","country_code","string")}} as addresses_country_code,	
        {{extract_nested_value("addresses","country_name","string")}} as addresses_country_name,						
        {{extract_nested_value("addresses","customer_id","NUMERIC")}} as addresses_customer_id,			
    	{{extract_nested_value("addresses","default","boolean")}} as addresses_default,		
    	{{extract_nested_value("addresses","first_name","string")}} as addresses_first_name,		
        {{extract_nested_value("addresses","last_name","string")}} as addresses_last_name,		
        {{extract_nested_value("addresses","id","NUMERIC")}} as addresses_id,
        {{extract_nested_value("addresses","name","string")}} as addresses_name,		
		{{extract_nested_value("addresses","phone","string")}} as addresses_phone,	
        {{extract_nested_value("addresses","province","string")}} as addresses_province,		
	    {{extract_nested_value("addresses","province_code","string")}} as addresses_province_code,		
        {{extract_nested_value("addresses","zip","string")}} as addresses_zip,
	    email,		
        a.first_name,		
       coalesce(a.id,0)  	as id,		
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        
        from {{i}} a  
            {{unnesting("addresses")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('upscribe_customers_lookback') }},0) from {{ this }})
            {% endif %}
            qualify
            dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) =1
        
 
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
