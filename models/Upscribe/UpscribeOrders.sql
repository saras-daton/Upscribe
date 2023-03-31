
{% if var('UpscribeOrders') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
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
{{set_table_name('%upscribe%orders')}}    
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
        analytics,		
        billing_address,		
        cancel_reason,		
        cancelled_at,		
        contact_email,		
        create_subscription,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,		
        customer,	
        {% if target.type=='snowflake' %}
            discount_applications.VALUE:allocation_method :: VARCHAR as discount_applications_allocation_method,	
            discount_applications.VALUE:code :: VARCHAR as discount_applications_code,		
            discount_applications.VALUE:target_selection :: VARCHAR as discount_applications_target_selection,	
            discount_applications.VALUE:target_type	:: VARCHAR as discount_applications_target_type,		
            discount_applications.VALUE:type :: VARCHAR as discount_applications_type,		
            discount_applications.VALUE:value :: VARCHAR as discount_applications_value,		
            discount_applications.VALUE:value_type :: VARCHAR as discount_applications_value_type,		
            discount_codes.VALUE:amount :: VARCHAR discount_codes_amount, 
            discount_codes.VALUE:code :: VARCHAR discount_codes_code, 
            discount_codes.VALUE:type :: VARCHAR discount_codes_type, 
        {% else %}
            discount_applications.allocation_method as discount_applications_allocation_method,		
            discount_applications.code	as discount_applications_code,		
            discount_applications.target_selection as discount_applications_target_selection,		
            discount_applications.target_type as discount_applications_target_type,		
            discount_applications.type as discount_applications_type,		
            discount_applications.value	as discount_applications_value,		
            discount_applications.value_type as discount_applications_value_type,		
            discount_codes.amount as discount_codes_amount,		
            discount_codes.code as discount_codes_code,		
            discount_codes.type	as discount_codes_type,	
        {% endif %}    	
        email,		
        financial_status,		
        a.fulfillment_status,
        fulfillments,
        guest_checkout,		
        a.id , 	
        {% if target.type=='snowflake' %}
        line_items.VALUE:admin_graphql_api_id as line_items_admin_graphql_api_id,		
        line_items.VALUE:discount_allocations as discount_allocations,	
        line_items.VALUE:fulfillable_quantity :: NUMERIC as line_items_fulfillable_quantity,
        line_items.VALUE:fulfillment_service as line_items_fulfillment_service,		
        line_items.VALUE:fulfillment_status as line_items_fulfillment_status,		
        line_items.VALUE:gift_card :: BOOLEAN  as line_items_gift_card,	
        line_items.VALUE:grams :: NUMERIC as line_items_grams,	
        line_items.VALUE:id	:: NUMERIC as line_items_id,	
        line_items.VALUE:name as line_items_name,		
        line_items.VALUE:pre_tax_price as line_items_pre_tax_price,		
        line_items.VALUE:pre_tax_price_set as line_items_pre_tax_price_set,	
        line_items.VALUE:price as line_items_price,		
        line_items.VALUE:price_set as line_items_price_set,	
        line_items.VALUE:product_exists :: BOOLEAN as line_items_product_exists,	
        line_items.VALUE:product_id :: NUMERIC as line_items_product_id,
        line_items.VALUE:properties as line_items_properties,	
        line_items.VALUE:quantity :: NUMERIC as line_items_quantity,	
        line_items.VALUE:requires_shipping :: BOOLEAN as line_items_requires_shipping ,	
        line_items.VALUE:sku as line_items_sku ,		
        line_items.VALUE:tax_lines as line_items_tax_lines,	
        line_items.VALUE:taxable :: BOOLEAN as line_items_taxable,	
        line_items.VALUE:title as line_items_title,		
        line_items.VALUE:total_discount	as line_items_total_discount,		
        line_items.VALUE:total_discount_set	as line_items_total_discount_set, 
        line_items.VALUE:variant_id :: NUMERIC as line_items_variant_id,	
        line_items.VALUE:variant_inventory_management as line_items_variant_inventory_management,		
        line_items.VALUE:variant_title as line_items_variant_title,		
        line_items.VALUE:vendor as line_items_vendor,
        {% else %}
        line_items.admin_graphql_api_id	as line_items_admin_grapgql_api_id,		
        line_items.discount_allocations	 as line_items_discount_allocations,		
        line_items.fulfillable_quantity as line_items_fulfillable_quantity,		
        line_items.fulfillment_service as line_items_fulfillment_service,		
        line_items.fulfillment_status as line_items_fulfillment_status,		
        line_items.gift_card as line_items_gift_card,		
        line_items.grams as line_items_grams,		
        line_items.id as line_items_id,		
        line_items.name as line_items_name,		
        line_items.pre_tax_price as line_items_pre_tax_price,		
        line_items.pre_tax_price_set as line_items_pre_tax_price_set,		
        line_items.price as line_items_price,		
        line_items.price_set as line_items_price_set,		
        line_items.product_exists as line_items_product_exists,		
        line_items.product_id as line_items_product_id,		
        line_items.properties as line_items_properties,		
        line_items.quantity	as line_items_quantity,		
        line_items.requires_shipping as line_items_requires_shipping,		
        line_items.sku as line_items_sku,		
        line_items.tax_lines as line_items_tax_lines,		
        line_items.taxable as line_items_taxable,		
        line_items.title as line_items_title,		
        line_items.total_discount as line_items_total_discount,		
        line_items.total_discount_set as line_items_total_discount_set,		
        line_items.variant_id as line_items_variant_id,		
        line_items.variant_inventory_management as line_items_variant_inventory_management,		
        line_items.variant_title as line_items_variant_title,		
        line_items.vendor as line_items_vendor,
        {% endif %}			
        a.name,		
        a.note,		
        a.number,		
        order_number,		
        payment_charge_id,		
        payment_customer_id,		
        payment_method_id,		
        payment_type,		
        a.phone,		
        presentment_currency,		
        processed_at,		
        refunds,		
        shipping_address,		
        shipping_lines,		
        shopify_customer_id,		
        store_id,		
        subtotal_price,		
        a.tags,		
        a.tax_lines,		
        a.token,		
        a.total_discounts,		
        total_line_items_price,		
        a.total_price,		
        total_shipping_price_set,
        total_tax,	
        tracking_codes,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp()}}) as updated_at,		
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.presentment_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.presentment_currency as exchange_currency_code, 
        {% endif %}    
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY a.id,a.order_number order by a.{{daton_batch_runtime()}} desc) row_num
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.presentment_currency = c.to_currency_code
            {% endif %}
            {{unnesting("discount_applications")}}
            {{unnesting("discount_codes")}}
            {{unnesting("line_items")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
