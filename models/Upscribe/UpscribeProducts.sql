
{% if var('UpscribeProducts') %}
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

    SELECT * {{exclude()}} (row_num)
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        body_html,		
        collections,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,			
        handle,		
        a.id ,
        {% if target.type=='snowflake' %}
            image.VALUE:admin_graphql_api_id :: VARCHAR as image_admin_graphql_api_id,			
            image.VALUE:alt :: BOOLEAN as image_alt,
            image.VALUE:created_at :: TIMESTAMP as image_created_at,	
            image.VALUE:height :: NUMERIC image_height,	
            image.VALUE:id :: NUMERIC as image_id,	
            image.VALUE:position :: NUMERIC as image_position,	
            image.VALUE:product_id :: NUMERIC as image_product_id,	
            image.VALUE:src :: VARCHAR as image_src,			
            image.VALUE:updated_at :: TIMESTAMP as image_updated_at,	
            image.VALUE:width :: NUMERIC as image_width,
        {% else %}    
            image.admin_graphql_api_id as image_admin_graphql_api_id,		
            image.alt as image_alt,	
            image.created_at as image_created_at,		
            image.height as image_height,	
            image.id as image_id,	
            image.position as image_position,	
            image.product_id as image_product_id,		
            image.src as image_src,		
            image.updated_at as image_updated_at,	
            image.width	as image_width,
        {% endif %}
        images,
        in_sales_channel,		
        is_subscription,		
        metafields,
        options,	
        store_id,		
        template_suffix,		
        a.title,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {% if target.type=='snowflake' %}
            variants.VALUE:admin_graphql_api_id :: VARCHAR as variants_admin_graphql_api_id,			
            variants.VALUE:barcode :: VARCHAR as variants_barcode,			
            variants.VALUE:compare_at_price	:: VARCHAR as variants_compare_at_price,		
            variants.VALUE:created_at :: TIMESTAMP as variants_created_at,	
            variants.VALUE:fulfillment_service :: VARCHAR as variants_fulfillment_service,			
            variants.VALUE:grams :: NUMERIC	as variants_grams,
            variants.VALUE:id :: NUMERIC as variants_id,	
            variants.VALUE:image_id	:: NUMERIC as variants_image_id,	
            variants.VALUE:inventory_item_id :: NUMERIC	as variants_inventory_item_id,
            variants.VALUE:inventory_management :: VARCHAR as variant_inventory_management,			
            variants.VALUE:inventory_policy :: VARCHAR as variants_inventory_policy,			
            variants.VALUE:inventory_quantity :: NUMERIC as variants_inventory_quantity,	
            variants.VALUE:old_inventory_quantity :: NUMERIC as variants_old_inventory_quantity,	
            variants.VALUE:option1 :: VARCHAR as variants_option1,			
            variants.VALUE:option2 :: VARCHAR as variants_option2,			
            variants.VALUE:option3 :: VARCHAR as variants_option3,			
            variants.VALUE:position	:: NUMERIC as variants_position,	
            variants.VALUE:price :: VARCHAR as variants_price,			
            variants.VALUE:product_id :: NUMERIC as variants_product_id,	
            variants.VALUE:requires_shipping :: BOOLEAN	as variants_requires_shipping,
            variants.VALUE:sku :: VARCHAR as variants_sku,			
            variants.VALUE:taxable :: BOOLEAN as variants_taxable,	
            variants.VALUE:title :: VARCHAR as variants_title,			
            variants.VALUE:updated_at :: TIMESTAMP as variants_updated_at,	
            variants.VALUE:weight :: NUMERIC as variants_weight,	
            variants.VALUE:weight_unit :: VARCHAR as variants_weight_unit,			
        {% else %}
            variants.admin_graphql_api_id as variants_admin_graphql_api_id,		
            variants.barcode as variants_barcode,		
            variants.compare_at_price as variants_compare_at_price,		
            variants.created_at as variants_created_at,	
            variants.fulfillment_service as variants_fulfillment_service,		
            variants.grams as variants_grams,	
            variants.id	as variants_id,	
            variants.image_id as variants_image_id,		
            variants.inventory_item_id	as variants_inventory_item_id,	
            variants.inventory_management as variants_inventory_management,		
            variants.inventory_policy as variants_inventory_policy,		
            variants.inventory_quantity as variants_inventory_quantity,	
            variants.old_inventory_quantity	as variants_old_inventory_quantity,
            variants.option1 as variants_option1,		
            variants.option2 as variants_option2,		
            variants.option3 as variants_option3,		
            variants.position as variants_position,	
            variants.price as variants_price,		
            variants.product_id	as variants_product_id,	
            variants.requires_shipping	as variants_requires_shipping ,		
            variants.sku as variants_sku,		
            variants.taxable as variants_taxable,	
            variants.title as variants_title,		
            variants.updated_at	as variants_updated_at ,	
            variants.weight	as variants_weight,	
            variants.weight_unit as variants_weight_unit,
        {% endif %}	
        vendor,	
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} a 
            {{unnesting("image")}}
            {{unnesting("variants")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
