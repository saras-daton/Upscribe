
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

   
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        analytics,			
        {{extract_nested_value("billing_address","address1","string")}} as billing_address_address1,

        {{extract_nested_value("billing_address","address2","string")}} as billing_address_address2,

        {{extract_nested_value("billing_address","city","string")}} as billing_address_city,

        {{extract_nested_value("billing_address","company","string")}} as billing_address_company,

        {{extract_nested_value("billing_address","country_code","string")}} as billing_address_country_code,

        {{extract_nested_value("billing_address","first_name","string")}} as billing_address_first_name,

        {{extract_nested_value("billing_address","last_name","string")}} as billing_address_last_name,

        {{extract_nested_value("billing_address","latitude","string")}} as billing_address_latitude,

        {{extract_nested_value("billing_address","longitude","string")}} as billing_address_longitude,

        {{extract_nested_value("billing_address","name","string")}} as billing_address_name,

        {{extract_nested_value("billing_address","phone","string")}} as billing_address_phone,

        {{extract_nested_value("billing_address","province","string")}} as billing_address_province,

        {{extract_nested_value("billing_address","province_code","string")}} as billing_address_province_code,

        {{extract_nested_value("billing_address","zip","string")}} as billing_address_zip,


        cancel_reason,		
        cancelled_at,		
        contact_email,		
        create_subscription,	
        customer,	
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,		
        {{extract_nested_value(" discount_applications","allocation_method","string")}} as discount_applications_allocation_method,
        {{extract_nested_value(" discount_applications","code","string")}} as discount_applications_code,
        {{extract_nested_value(" discount_applications","target_selection","string")}} as discount_applications_target_selection,
        {{extract_nested_value(" discount_applications","target_type","string")}} as discount_applications_target_type,
        {{extract_nested_value(" discount_applications","type","string")}} as discount_applications_type,
        {{extract_nested_value(" discount_applications","value","string")}} as discount_applications_value,
        {{extract_nested_value(" discount_applications","value_type","string")}} as discount_applications_value_type,
        {{extract_nested_value(" discount_codes","amount","string")}} as discount_codes_amount,
        {{extract_nested_value(" discount_codes","code","string")}} as discount_codes_code,
        {{extract_nested_value(" discount_codes","type","string")}} as discount_codes_type,
        a.email,		
        financial_status,		
        a.fulfillment_status,
        --fulfillments,
        guest_checkout,		
        coalesce(a.id,0) as id , 	
       
        {{extract_nested_value(" line_items","admin_graphql_api_id","string")}} as line_items_admin_graphql_api_id,
        {{extract_nested_value(" line_items","fulfillable_quantity","NUMERIC")}} as line_items_fulfillable_quantity,
       	
       	
        --line_items.VALUE:discount_allocations as discount_allocations,	
        {{extract_nested_value(" line_items","fulfillment_service","string")}} as line_items_fulfillment_service,
       	
       	{{extract_nested_value(" line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
        {{extract_nested_value(" line_items","gift_card","boolean")}} as line_items_gift_card,
        {{extract_nested_value(" line_items","grams","numeric")}} as line_items_grams,
        {{extract_nested_value(" line_items","id","numeric")}} as line_items_id,
        {{extract_nested_value(" line_items","name","string")}} as line_items_name,
        {{extract_nested_value(" line_items","pre_tax_price","string")}} as line_items_pre_tax_price,
        {{extract_nested_value(" line_items","price","string")}} as line_items_price,
        {{extract_nested_value(" line_items","product_exists","boolean")}} as line_items_product_exists,
        {{extract_nested_value(" line_items","product_id","numeric")}} as line_items_product_id,

   		
        --line_items.VALUE:pre_tax_price_set as line_items_pre_tax_price_set,	
        		
        --line_items.VALUE:price_set as line_items_price_set,	
      
       -- line_items.VALUE:properties as line_items_properties,	
        {{extract_nested_value(" line_items","quantity","numeric")}} as line_items_quantity,
        {{extract_nested_value(" line_items","requires_shipping","boolean")}} as line_items_requires_shipping,
        {{extract_nested_value(" line_items","sku","string")}} as line_items_sku,

		
       -- line_items.VALUE:tax_lines as line_items_tax_lines,	
       {{extract_nested_value(" line_items","taxable","boolean")}} as line_items_taxable,
       {{extract_nested_value(" line_items","title","string")}} as line_items_title,
       {{extract_nested_value(" line_items","total_discount","string")}} as line_items_total_discount,
        --line_items.VALUE:total_discount_set	as line_items_total_discount_set, 
        {{extract_nested_value(" line_items","variant_id","numeric")}} as line_items_variant_id,
        {{extract_nested_value(" line_items","variant_inventory_management","string")}} as line_items_variant_inventory_management,
        {{extract_nested_value(" line_items","variant_title","string")}} as line_items_variant_title,
        {{extract_nested_value(" line_items","vendor","string")}} as line_items_vendor,
    		
        a.name,		
        a.note,		
        a.number,		
        coalesce(order_number,0) as order_number,		
        cast(payment_charge_id as string) as payment_charge_id,		
        cast(payment_customer_id as string) as payment_customer_id ,		
        cast(payment_method_id as string) as payment_method_id,		
        payment_type,		
        a.phone,		
        presentment_currency,		
        a.processed_at,		
        refunds,
        {{extract_nested_value("refunds","admin_graphql_api_id","string")}} as refunds_admin_graphql_api_id,
        {{extract_nested_value("refunds","created_at","TIMESTAMP")}} as refunds_created_at,
        {{extract_nested_value("refunds","id","NUMERIC")}} as refunds_id,
        {{extract_nested_value("refunds","note","string")}} as refunds_note,
        {{extract_nested_value("refunds","order_id","NUMERIC")}} as refunds_order_id,
        {{extract_nested_value("refunds","processed_at","TIMESTAMP")}} as refunds_processed_at,
        {{extract_nested_value("refunds","restock","boolean")}} as refunds_restock,
        {{extract_nested_value("refunds","user_id","boolean")}} as refunds_user_id,
    
        {{extract_nested_value("shipping_address","address1","string")}} as shipping_address_address1,

        {{extract_nested_value("shipping_address","address2","string")}} as shipping_address_address2,

        {{extract_nested_value("shipping_address","city","string")}} as shipping_address_city,

        {{extract_nested_value("shipping_address","company","string")}} as shipping_address_company,

        {{extract_nested_value("shipping_address","country_code","string")}} as shipping_address_country_code,

        {{extract_nested_value("shipping_address","first_name","string")}} as shipping_address_first_name,

        {{extract_nested_value("shipping_address","last_name","string")}} as shipping_address_last_name,

        {{extract_nested_value("shipping_address","latitude","string")}} as shipping_address_latitude,

        {{extract_nested_value("shipping_address","longitude","string")}} as shipping_address_longitude,

        {{extract_nested_value("shipping_address","name","string")}} as shipping_address_name,

        {{extract_nested_value("shipping_address","phone","string")}} as shipping_address_phone,

        {{extract_nested_value("shipping_address","province","string")}} as shipping_address_province,

        {{extract_nested_value("shipping_address","province_code","string")}} as shipping_address_province_code,

        {{extract_nested_value("shipping_address","zip","string")}} as shipping_address_zip,
	
        --shipping_lines,		
        cast(shopify_customer_id as string) as shopify_customer_id,		
        cast(store_id as string) as store_id,		
        subtotal_price,		
        a.tags,		
       -- a.tax_lines,		
        a.token,		
        a.total_discounts,		
        total_line_items_price,		
        a.total_price,	
        {{extract_nested_value("presentment_money","amount","string")}} as total_shipping_price_set_presentment_money_amount,
        {{extract_nested_value("presentment_money","currency_code","string")}} as total_shipping_price_set_presentment_money_currency_code,
        {{extract_nested_value("shop_money","amount","string")}} as total_shipping_price_set_shop_money_amount,
        {{extract_nested_value("shop_money","currency_code","string")}} as total_shipping_price_set_shop_money_currency_code,	
        total_tax,	
        --tracking_codes,
        {{extract_nested_value("TRACKING_CODES","checkoutAcceptsSMSMarketing","boolean")}} as tracking_codes_checkoutAcceptsSMSMarketing,
        {{extract_nested_value("TRACKING_CODES","clickRef","string")}} as tracking_codes_clickRef,
        {{extract_nested_value("TRACKING_CODES","partnerId","string")}} as tracking_codes_partnerId,
        {{extract_nested_value("TRACKING_CODES","refersion","string")}} as tracking_codes_refersion,
        {{extract_nested_value("TRACKING_CODES","utm_campaign","string")}} as tracking_codes_utm_campaign,
        {{extract_nested_value("TRACKING_CODES","utm_medium","string")}} as tracking_codes_utm_medium,
        {{extract_nested_value("TRACKING_CODES","utm_source","string")}} as tracking_codes_utm_source,
        {{extract_nested_value("TRACKING_CODES","medium","string")}} as tracking_codes_medium,
      	{{extract_nested_value("TRACKING_CODES","source","string")}} as tracking_codes_source,
        {{extract_nested_value("TRACKING_CODES","utm_term","string")}} as tracking_codes_utm_term,
        {{extract_nested_value("TRACKING_CODES","utm_content","string")}} as tracking_codes_utm_content,
        {{extract_nested_value("TRACKING_CODES","affId","string")}} as tracking_codes_affId,
        {{extract_nested_value("TRACKING_CODES","offerId","string")}} as tracking_codes_offerId,
        {{extract_nested_value("TRACKING_CODES","fbc","string")}} as tracking_codes_fbc,
        {{extract_nested_value("TRACKING_CODES","fbp","string")}} as tracking_codes_fbp,
        {{extract_nested_value("TRACKING_CODES","heap_userId","string")}} as tracking_codes_heap_userId,
        {{extract_nested_value("TRACKING_CODES","gclid","string")}} as tracking_codes_gclid,
        {{extract_nested_value("TRACKING_CODES","fbuy_att_id","string")}} as tracking_codes_fbuy_att_id,
        {{extract_nested_value("TRACKING_CODES","loqate_status","string")}} as tracking_codes_loqate_status,
        {{extract_nested_value("TRACKING_CODES","referrer","string")}} as tracking_codes_referrer,
        {{extract_nested_value("TRACKING_CODES","irClickId","string")}} as tracking_codes_irClickId,
        {{extract_nested_value("TRACKING_CODES","page_name","string")}} as tracking_codes_page_name,
        {{extract_nested_value("TRACKING_CODES","vwo_current","string")}} as tracking_codes_vwo_current,
        {{extract_nested_value("TRACKING_CODES","vwo_cart","string")}} as tracking_codes_vwo_cart,
        {{extract_nested_value("TRACKING_CODES","accountFormAcceptsSMSMarketing","boolean")}} as tracking_codes_accountFormAcceptsSMSMarketing,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp()}}) as updated_at,		
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
        
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.presentment_currency = c.to_currency_code
            {% endif %}
            {{unnesting("discount_applications")}}
            {{unnesting("discount_codes")}}
            {{unnesting("line_items")}}
            {{unnesting("billing_address")}}
            {{unnesting("shipping_address")}}
            {{unnesting("tracking_codes")}}
            {{unnesting("total_shipping_price_set")}}
            {{multi_unnesting("total_shipping_price_set","presentment_money")}}
            {{multi_unnesting("total_shipping_price_set","shop_money")}}
            {{unnesting("refunds")}}
            
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
            qualify
            row_number() over (partition by a.id,a.order_number order by a.{{daton_batch_runtime()}} desc) =1
        
  
    {% if not loop.last %} union all {% endif %}
    {% endfor %}