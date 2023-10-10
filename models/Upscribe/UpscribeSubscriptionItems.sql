    {% if var('UpscribeSubscriptionItems') %}
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
    {{set_table_name('%upscribe%subscriptions')}}    
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
            activated_on,
            active,
            cancellation_reason,
            cancellation_comment,
            cancellation_reminder_sent,
            cancelled_on,
            a.charge_limit,
            a.coupon_discount,
            CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
            coalesce(a.id,0) as subscription_id,
            import_id,
            a.interval,
            	
            {{extract_nested_value("items","compare_at_price","boolean")}} as items_compare_at_price,
            {{extract_nested_value("items","fulfillment_service","string")}} as  items_fulfillment_service,
            {{extract_nested_value("items","gift_card","boolean")}} as  items_gift_card,
            {{extract_nested_value("items","grams","NUMERIC")}} as  items_grams,
            {{extract_nested_value("items","id","string")}} as  items_id,
            {{extract_nested_value("items","image_url","string")}} as  items_image_url,
            {{extract_nested_value("items","in_stock","boolean")}} as  items_in_stock,
            {{extract_nested_value("items","key","string")}} as  items_key,
            {{extract_nested_value("items","line_price","NUMERIC")}} as  items_line_price,
            {{extract_nested_value("items","original_line_price","NUMERIC")}} as  items_original_line_price,
            {{extract_nested_value("items","original_price","NUMERIC")}} as  items_original_price,
            {{extract_nested_value("items","price","NUMERIC")}} as  items_price,
            {{extract_nested_value("items","product_id","NUMERIC")}} as  items_product_id,
            {{extract_nested_value("items","quantity","NUMERIC")}} as  items_quantity,
            {{extract_nested_value("items","requires_shipping","boolean")}} as  items_requires_shipping,
            {{extract_nested_value("items","sku","string")}} as  items_sku,
            {{extract_nested_value("items","subscription_price","NUMERIC")}} as  items_subscription_price,
            {{extract_nested_value("items","taxable","boolean")}} as  items_taxable,
            {{extract_nested_value("items","title","string")}} as  items_title,
            {{extract_nested_value("items","variant_id","NUMERIC")}} as  items_variant_id,
            {{extract_nested_value("items","variant_title","string")}} as  items_variant_title,
            {{extract_nested_value("items","vendor","string")}} as  items_vendor,

            {{extract_nested_value("properties","Charge__Limit","string")}} as items_properties_Charge__Limit,

            {{extract_nested_value("properties","Discount__Amount","string")}} as items_properties_Discount__Amount,

            {{extract_nested_value("properties","Interval__Frequency","string")}} as items_properties_Interval__Frequency,

            {{extract_nested_value("properties","Interval__Unit","string")}} as items_properties_Interval__Unit,

            {{extract_nested_value("properties","Subscription","string")}} as items_properties_Subscription,

            {{extract_nested_value("properties","Subscription__Product__Title","string")}} as items_properties_Subscription__Product__Title,
           
            next,
            order_count,
            order_ids,
            a.original_total_line_items_price,
            a.original_total_price,
            a.original_total_tax,
            payment_customer_id,
            a.payment_method_id,
            a.payment_type,
            a.period as order_interval_frequency,
            a.quantity_discount,
            requires_customer_update,
            requires_update,
            a.shipping_discount,
            shopify_customer_email,
            a.shopify_customer_id,
            a.shopify_discount,
            a.shopify_order_id,
            store_id,
            a.subscription_discount,
            a.tax_discount,
            a.total_discount,
            a.total_discount_inc_tax,
            a.total_line_items_price,
            a.total_price,
            a.total_tax,
            {% if target.type =='snowflake' %}	
            {{extract_nested_value("TRACKING_CODES","checkoutAcceptsSMSMarketing","string")}} as tracking_codes_checkoutAcceptsSMSMarketing,
            {{extract_nested_value("TRACKING_CODES","clickRef","string")}} as tracking_codes_clickRef,
            {{extract_nested_value("TRACKING_CODES","partnerId","string")}} as tracking_codes_partnerId,
            {{extract_nested_value("TRACKING_CODES","refersion","string")}} as tracking_codes_refersion,
            {{extract_nested_value("TRACKING_CODES","utm_campaign","string")}} as tracking_codes_utm_campaign,
            {{extract_nested_value("TRACKING_CODES","utm_medium","string")}} as tracking_codes_utm_medium,
            {{extract_nested_value("TRACKING_CODES","utm_source","string")}} as tracking_codes_utm_source,
            {{extract_nested_value("TRACKING_CODES","medium","string")}} as tracking_codes_medium,
            {{extract_nested_value("TRACKING_CODES","source","string")}} as tracking_codes_source,
	
            {{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as updated_at,
            {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            
            from {{i}} a
                {{unnesting("ITEMS")}} 
                {{unnesting("TRACKING_CODES")}} 
                {{multi_unnesting("items","properties")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
                qualify
                DENSE_RANK() OVER (PARTITION BY date(a.created_at) order by {{daton_batch_runtime()}} desc) =1
           
        {% if not loop.last %} union all {% endif %}
    {% endfor %}