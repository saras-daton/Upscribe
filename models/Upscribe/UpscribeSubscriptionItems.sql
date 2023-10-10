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
            {% if target.type =='snowflake' %}
                ITEMS.VALUE:compare_at_price :: BOOLEAN as items_compare_at_price	,		
                ITEMS.VALUE:fulfillment_service :: VARCHAR as items_fulfillment_service	,		
                ITEMS.VALUE:gift_card :: BOOLEAN as items_gift_card	,		
                ITEMS.VALUE:grams :: FLOAT as items_grams	,		
                ITEMS.VALUE:id :: VARCHAR as items_id	,		
                ITEMS.VALUE:image_url :: VARCHAR as items_image_url	,		
                ITEMS.VALUE:in_stock :: BOOLEAN as items_in_stock	,		
                ITEMS.VALUE:key	:: VARCHAR as items_key,		
                ITEMS.VALUE:line_price :: FLOAT as items_line_price	,		
                ITEMS.VALUE:original_line_price :: FLOAT as items_original_line_price	,		
                ITEMS.VALUE:original_price :: FLOAT as items_original_price	,		
                ITEMS.VALUE:price :: FLOAT as items_price	,		
                ITEMS.VALUE:product_id :: INT as items_product_id	,	
                ITEMS.VALUE:quantity :: FLOAT as items_quantity	,		
                ITEMS.VALUE:requires_shipping :: BOOLEAN as items_requires_shipping	,		
                ITEMS.VALUE:sku :: VARCHAR as items_sku	,		
                ITEMS.VALUE:subscription_price :: FLOAT as items_subscription_price	,		
                ITEMS.VALUE:taxable :: BOOLEAN as items_taxable	,		
                ITEMS.VALUE:title :: VARCHAR as	items_title ,		
                ITEMS.VALUE:variant_id :: INT as items_variant_id	,		
                ITEMS.VALUE:variant_title :: VARCHAR as items_variant_title	,		
                ITEMS.VALUE:vendor :: VARCHAR as items_vendor	,
            {% else %}
                items.compare_at_price	as items_compare_at_price,		
                items.fulfillment_service as items_fulfillment_service	,		
                items.gift_card as items_gift_card	,		
                items.grams	items_grams,		
                items.id as items_id	,		
                items.image_url as items_image_url	,		
                items.in_stock	items_in_stock,		
                items.key as items_key	,		
                items.line_price as items_line_price	,		
                items.original_line_price	as items_original_line_price,		
                items.original_price	items_original_price,		
                items.price	 as items_price,		
                cast(items.product_id as string) as items_product_id,		
                --items.properties,
                items.quantity as items_quantity	,		
                items.requires_shipping	items_requires_shipping,		
                items.sku as items_sku,	
                items.subscription_price items_subscription_price	,		
                items.taxable	as items_taxable,		
                items.title	as items_title,		
                cast(items.variant_id as string) as items_variant_id,	
                items.variant_title	as items_variant_title,		
                items.vendor	as items_vendor,	
            {% endif %}	
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
            TRACKING_CODES.VALUE:checkoutAcceptsSMSMarketing :: VARCHAR AS tracking_codes_checkoutAcceptsSMSMarketing,
            TRACKING_CODES.VALUE:clickRef :: VARCHAR AS tracking_codes_clickRef,
            TRACKING_CODES.VALUE:partnerId :: VARCHAR AS tracking_codes_partnerId,
            TRACKING_CODES.VALUE:refersion :: VARCHAR AS tracking_codes_refersion,
            TRACKING_CODES.VALUE:utm_campaign :: VARCHAR AS tracking_codes_utm_campaign,
            TRACKING_CODES.VALUE:utm_medium :: VARCHAR AS tracking_codes_utm_medium,
            TRACKING_CODES.VALUE:utm_source :: VARCHAR AS tracking_codes_utm_source,
            TRACKING_CODES.VALUE:medium :: VARCHAR AS tracking_codes_medium,		
            TRACKING_CODES.VALUE:source :: VARCHAR AS tracking_codes_source,
            {% else %}
            tracking_codes.checkoutAcceptsSMSMarketing as tracking_codes_checkoutAcceptsSMSMarketing,	
            tracking_codes.clickRef	as tracking_codes_clickRef, 		
            tracking_codes.partnerId as tracking_codes_partnerId, 	 		
            tracking_codes.refersion as tracking_codes_refersion, 			
            tracking_codes.utm_campaign as tracking_codes_utm_campaign, 			
            tracking_codes.utm_medium as tracking_codes_utm_medium, 			
            tracking_codes.utm_source as tracking_codes_utm_source, 			
            tracking_codes.medium as tracking_codes_medium, 			
            tracking_codes.source as tracking_codes_source, 				
            {% endif %}
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