
{% if var('UpscribeSubscriptionQueues') %}
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
{{set_table_name('%upscribe%subscriptionqueues')}}    
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
        billing_address,	
        charge_error,		
        charge_errors,		
        coupon_discount,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        created_order,		
        custom_date,		
        date,	
        discount_code,		
        errored,		
        errored_count,		
        a.id,		
        is_addressing_upcoming_notification,	
        {% if target.type=='snowflake' %}
            items.VALUE:compare_at_price :: VARCHAR as items_compare_at_price,		
            items.VALUE:fulfillment_service	:: VARCHAR as items_fulfillment_service ,		
            items.VALUE:gift_card :: VARCHAR as items_gift_card,		
            items.VALUE:grams :: VARCHAR as items_grams,		
            items.VALUE:id :: VARCHAR as items_id,		
            items.VALUE:image_url :: VARCHAR as items_image_url,		
            items.VALUE:in_stock :: VARCHAR as items_in_stock,		
            items.VALUE:key :: VARCHAR as items_key,		
            items.VALUE:line_price :: VARCHAR as items_line_price,		
            items.VALUE:original_line_price :: VARCHAR as items_original_line_price,		
            items.VALUE:original_price :: VARCHAR as items_original_price,		
            items.VALUE:price :: VARCHAR as items_price,		
            items.VALUE:product_id :: VARCHAR as items_product_id,	
            items.VALUE:properties as items_properties,	
            items.VALUE:quantity :: VARCHAR as items_quantity,		
            items.VALUE:requires_shipping :: VARCHAR as items_requires_shipping,		
            items.VALUE:sku :: VARCHAR as items_sku,		
            items.VALUE:subscription_price :: VARCHAR as items_subscription_price,		
            items.VALUE:taxable :: VARCHAR as items_taxable,		
            items.VALUE:title :: VARCHAR as items_title,		
            items.VALUE:variant_id :: VARCHAR as items_variant_id,		
            items.VALUE:variant_title :: VARCHAR as items_variant_title,		
            items.VALUE:vendor :: VARCHAR as items_vendor,		
        {% else %}    	
            items.compare_at_price as items_compare_at_price,		
            items.fulfillment_service as items_fulfillment_service,		
            items.gift_card as items_gift_card,		
            items.grams as items_grams,		
            items.id as items_id,		
            items.image_url as items_image_url,		
            items.in_stock as items_in_stock,		
            items.key as items_key,		
            items.line_price as items_line_price,		
            items.original_line_price as items_original_line_price,		
            items.original_price as items_original_price,		
            items.price as items_price,		
            items.product_id as items_product_id,
            items.properties as items_properties,			
            items.quantity as items_quantity,		
            items.requires_shipping as items_requires_shipping,		
            items.sku as items_sku,		
            items.subscription_price as items_subscription_price,		
            items.taxable as items_taxable,		
            items.title as items_title,		
            items.variant_id as items_variant_id,		
            items.variant_title as items_variant_title,		
            items.vendor as items_vendor,		
        {% endif %}
        origin_subtotal_price,		
        original_total_line_items_price,		
        original_total_price,		
        original_total_tax,		
        payment_method_id,		
        payment_type,		
        processed,		
        processing,		
        quantity_discount,		
        shipping_address,	
        shipping_discount,		
        shipping_lines,		
        shopify_customer_email,		
        shopify_customer_id,		
        shopify_order_id,		
        skip,		
        store_id,		
        subscription_discount,		
        subscription_id,		
        subtotal_price,		
        tax_discount,		
        total_discount,		
        total_discount_inc_tax,		
        total_line_items_price,		
        total_price,		
        total_tax,		
        upcoming_notification_sent,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY a.id,shopify_order_id order by a.{{daton_batch_runtime()}} desc) row_num
        from {{i}} a  
            {{unnesting("items")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
