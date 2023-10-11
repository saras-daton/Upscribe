
{% if var('UpscribeOrdersTaxLines') %}
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
        --billing_address,		
        cancel_reason,		
        cancelled_at,		
        contact_email,		
        create_subscription,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,		
        --customer,	 
        --discount_applications,
        --discount_codes,	
        email,		
        financial_status,		
        a.fulfillment_status,
        --fulfillments,
        guest_checkout,		
        coalesce(a.id,0) as id,		
        --line_items,
       {{extract_nested_value("tax_lines","price","string")}} as  tax_lines_price,
       {{extract_nested_value("presentment_money","amount","string")}} as  tax_lines_price_set_presentment_money_amount,
       {{extract_nested_value("presentment_money","currency_code","string")}} as  tax_lines_price_set_presentment_money_currency_code,
       {{extract_nested_value("shop_money","amount","string")}} as  tax_lines_price_set_shop_money_amount,
       {{extract_nested_value("shop_money","currency_code","string")}} as  tax_lines_price_set_shop_money_currency_code,
       {{extract_nested_value("tax_lines","rate","numeric")}} as  tax_lines_rate,
       {{extract_nested_value("tax_lines","title","string")}} as  tax_lines_title,
        a.name,		
        a.note,		
        a.number,		
        coalesce(order_number,0) as order_number,		
        cast(payment_charge_id as string) as payment_charge_id,		
        cast(payment_customer_id as string) as payment_customer_id,		
        cast(payment_method_id as string) as payment_method_id,		
        payment_type,		
        a.phone,		
        presentment_currency,		
        processed_at,		
        --refunds,		
        --shipping_address,	
        fulfillments,	
        shipping_lines,		
        {{extract_nested_value("shipping_lines","carrier_identifier","boolean")}} as  shipping_lines_carrier_identifier,
        {{extract_nested_value("shipping_lines","code","string")}} as  shipping_lines_code,
        {{extract_nested_value("shipping_lines","delivery_category","boolean")}} as  shipping_lines_delivery_category,
        {{extract_nested_value("shipping_lines","id","NUMERIC")}} as  shipping_lines_id,
        {{extract_nested_value("shipping_lines","discounted_price","string")}} as  shipping_lines_discounted_price,
        {{extract_nested_value("shipping_lines","phone","boolean")}} as  shipping_lines_phone,
        {{extract_nested_value("shipping_lines","price","string")}} as  shipping_lines_price,
        {{extract_nested_value("shipping_lines","source","string")}} as  shipping_lines_source,
        {{extract_nested_value("shipping_lines","title","string")}} as  shipping_lines_title,
        {{extract_nested_value("shipping_lines","requested_fulfillment_service_id","boolean")}} as  shipping_lines_requested_fulfillment_service_id,

        shopify_customer_id,		
        cast(store_id as string) as store_id,		
        subtotal_price,		
        a.tags,		
        --a.tax_lines,		
        a.token,		
        a.total_discounts,		
        total_line_items_price,		
        a.total_price,		
       
        total_tax,	
        --tracking_codes,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,		
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
        
        from {{i}}  a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.presentment_currency = c.to_currency_code
            {% endif %}
            {{unnesting("tax_lines")}}
            {{multi_unnesting("tax_lines","price_set")}}
            {{multi_unnesting("price_set","presentment_money")}}
            {{multi_unnesting("price_set","shop_money")}}
            {{unnesting("shipping_lines")}}
            
            
            --Need multi-unnesting macro which can handle this
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
            qualify
            row_number() over (partition by a.id,a.order_number order by a.{{daton_batch_runtime()}} desc) =1
       
  
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
