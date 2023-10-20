
{% if var('UpscribeProducts') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('upscribe_products_ptrn'),
exclude=var('upscribe_products_tbl_exclude_ptrn'),
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
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,			
        handle,		
        coalesce(a.id,0) as id ,
       
        {{extract_nested_value("image","admin_graphql_api_id","string")}} as image_admin_graphql_api_id,
        {{extract_nested_value("image","alt","boolean")}} as image_alt,
        {{extract_nested_value("image","created_at","TIMESTAMP")}} as image_created_at,
        {{extract_nested_value("image","height","NUMERIC")}} as image_height,
        {{extract_nested_value("image","id","NUMERIC")}} as image_id,
        {{extract_nested_value("image","position","NUMERIC")}} as image_position,
        {{extract_nested_value("image","product_id","NUMERIC")}} as image_product_id,
        {{extract_nested_value("image","src","string")}} as image_src,
        {{extract_nested_value("image","updated_at","TIMESTAMP")}} as image_updated_at,
        {{extract_nested_value("image","width","NUMERIC")}} as image_width,
        in_sales_channel,		
        is_subscription,		
        {{extract_nested_value(" options","id","NUMERIC")}} as options_id,
        {{extract_nested_value(" options","name","string")}} as options_name,
        {{extract_nested_value(" options","position","NUMERIC")}} as options_position,
        {{extract_nested_value(" options","product_id","NUMERIC")}} as options_product_id,
        {{extract_nested_value(" options","values","string")}} as options_values,
        store_id,		
        template_suffix,		
        a.title,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {{extract_nested_value(" variants","admin_graphql_api_id","string")}} as variants_admin_graphql_api_id,
        {{extract_nested_value(" variants","barcode","string")}} as variants_barcode,
        {{extract_nested_value(" variants","compare_at_price","string")}} as variants_compare_at_price,
        {{extract_nested_value(" variants","created_at","TIMESTAMP")}} as variants_created_at,
        {{extract_nested_value(" variants","fulfillment_service","string")}} as variants_fulfillment_service,
        {{extract_nested_value(" variants","grams","NUMERIC")}} as variants_grams,
        {{extract_nested_value(" variants","id","NUMERIC")}} as variants_id,
        {{extract_nested_value(" variants","image_id","NUMERIC")}} as variants_image_id,
        {{extract_nested_value(" variants","inventory_item_id","NUMERIC")}} as variants_inventory_item_id,
        {{extract_nested_value(" variants","inventory_management","string")}} as variants_inventory_management,
        {{extract_nested_value(" variants","inventory_policy","string")}} as variants_inventory_policy,
        {{extract_nested_value(" variants","inventory_quantity","NUMERIC")}} as variants_inventory_quantity,
        {{extract_nested_value(" variants","inventory_quantity","NUMERIC")}} as variants_old_inventory_quantity,
        {{extract_nested_value(" variants","option1","string")}} as variants_option1,
        {{extract_nested_value(" variants","option2","string")}} as variants_option2,
        {{extract_nested_value(" variants","option3","string")}} as variants_option3,
        {{extract_nested_value(" variants","position","NUMERIC")}} as variants_position,
        {{extract_nested_value(" variants","price","string")}} as variants_price,
        {{extract_nested_value(" variants","product_id","NUMERIC")}} as variants_product_id,
        {{extract_nested_value(" variants","requires_shipping","boolean")}} as variants_requires_shipping,
        {{extract_nested_value(" variants","sku","string")}} as variants_sku,
        {{extract_nested_value(" variants","taxable","boolean")}} as variants_taxable,
        {{extract_nested_value(" variants","title","string")}} as variants_title,
        {{extract_nested_value(" variants","updated_at","TIMESTAMP")}} as variants_updated_at,
        {{extract_nested_value(" variants","weight","NUMERIC")}} as variants_weight,
        {{extract_nested_value(" variants","weight_unit","string")}} as variants_weight_unit,
        vendor,	
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        
        from {{i}} a 
            {{unnesting("image")}}
            {{unnesting("variants")}}
            {{unnesting("options")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
             where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('upscribe_products_lookback') }},0) from {{ this }})
            {% endif %}
            qualify
            dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) =1
    
    {% if not loop.last %} union all {% endif %}
    {% endfor %}