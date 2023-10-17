
{% if var('UpscribeSubscriptionQueues') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
    
-- {% if is_incremental() %}
-- {%- set max_loaded_query -%}
-- select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
-- {% endset %}

-- {%- set max_loaded_results = run_query(max_loaded_query) -%}

-- {%- if execute -%}
-- {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
-- {% else %}
-- {% set max_loaded = 0 %}
-- {%- endif -%}
-- {% endif %}

-- {% set table_name_query %}
-- {{set_table_name('%upscribe%subscriptionqueues')}}    
-- {% endset %}  

-- {% set results = run_query(table_name_query) %}

    {% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern=var('raw_schema'),
    table_pattern=var('upscribe_subscription_queues_ptrn'),
    exclude=var('upscribe_subscription_queues_tbl_exclude_ptrn'),
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

 
        charge_error,		
        charge_errors,		
        coupon_discount,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        created_order,		
        custom_date,		
        date,	
        discount_code,		
        errored,		
        errored_count,		
        a.id,		
        is_addressing_upcoming_notification,	
        {{extract_nested_value("items","compare_at_price","boolean")}} as items_compare_at_price,

        {{extract_nested_value("items","fulfillment_service","string")}} as items_fulfillment_service,

        {{extract_nested_value("items","gift_card","boolean")}} as items_gift_card,

        {{extract_nested_value("items","grams","numeric")}} as items_grams,

        {{extract_nested_value("items","id","string")}} as items_id,    

        {{extract_nested_value("items","image_url","string")}} as items_image_url,  

        {{extract_nested_value("items","in_stock","boolean")}} as items_in_stock,  

        {{extract_nested_value("items","key","string")}} as items_key,  

        {{extract_nested_value("items","line_price","numeric")}} as items_line_price,  

        {{extract_nested_value("items","original_line_price","numeric")}} as items_original_line_price,

        {{extract_nested_value("items","original_price","numeric")}} as items_original_price,

        {{extract_nested_value("items","price","numeric")}} as items_price,

        {{extract_nested_value("items","product_id","numeric")}} as items_product_id,          

        {{extract_nested_value("items","quantity","numeric")}} as items_quantity,

        {{extract_nested_value("items","requires_shipping","boolean")}} as items_requires_shipping,

        {{extract_nested_value("items","sku","string")}} as items_sku,  

        {{extract_nested_value("items","subscription_price","numeric")}} as items_subscription_price,          

        {{extract_nested_value("items","taxable","boolean")}} as items_taxable,

        {{extract_nested_value("items","title","string")}} as items_title,  

        {{extract_nested_value("items","variant_id","numeric")}} as items_variant_id,

        {{extract_nested_value("items","variant_title","string")}} as items_variant_title,

        {{extract_nested_value("items","vendor","string")}} as items_vendor,

        {{extract_nested_value("properties","Charge__Limit","string")}} as items_properties_Charge__Limit,

        {{extract_nested_value("properties","Discount__Amount","string")}} as items_properties_Discount__Amount,

        {{extract_nested_value("properties","Interval__Frequency","string")}} as items_properties_Interval__Frequency,

        {{extract_nested_value("properties","Interval__Unit","string")}} as items_properties_Interval__Unit,

        {{extract_nested_value("properties","Subscription","string")}} as items_properties_Subscription,

        {{extract_nested_value("properties","Subscription__Product__Title","string")}} as items_properties_Subscription__Product__Title,
       
        origin_subtotal_price,		
        original_total_line_items_price,		
        original_total_price,		
        original_total_tax,		
        payment_method_id,		
        payment_type,		
        processed,		
        processing,		
        quantity_discount,		
        
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

 
        shipping_discount,		
        
        {{extract_nested_value("shipping_lines","handle","string")}} as shipping_lines_handle,
        {{extract_nested_value("shipping_lines","price","string")}} as shipping_lines_price,
        {{extract_nested_value("shipping_lines","title","string")}} as shipping_lines_title,

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
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        
        from {{i}} a  
            {{unnesting("items")}}
            {{multi_unnesting("items","properties")}}
            {{unnesting("billing_address")}}
            {{unnesting("shipping_address")}}
            {{unnesting("shipping_lines")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('upscribe_subscription_queues_lookback') }},0) from {{ this }})
            --WHERE 1=1
            {% endif %}
            qualify
            row_number() over (partition by a.id,shopify_order_id order by a.{{daton_batch_runtime()}} desc) =1
       
    {% if not loop.last %} union all {% endif %}
    {% endfor %}