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

        SELECT * {{exclude()}} (row_num)
        FROM (
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
                ITEMS.VALUE:compare_at_price :: BOOLEAN as compare_at_price	,		
                ITEMS.VALUE:fulfillment_service :: VARCHAR as fulfillment_service	,		
                ITEMS.VALUE:gift_card :: BOOLEAN as item_gift_card	,		
                ITEMS.VALUE:grams :: FLOAT as item_grams	,		
                ITEMS.VALUE:id :: VARCHAR as id	,		
                ITEMS.VALUE:image_url :: VARCHAR as image_url	,		
                ITEMS.VALUE:in_stock :: BOOLEAN as in_stock	,		
                ITEMS.VALUE:key	:: VARCHAR as key,		
                ITEMS.VALUE:line_price :: FLOAT as line_price	,		
                ITEMS.VALUE:original_line_price :: FLOAT as original_line_price	,		
                ITEMS.VALUE:original_price :: FLOAT as original_price	,		
                ITEMS.VALUE:price :: FLOAT as price	,		
                ITEMS.VALUE:product_id :: INT as product_id	,	
                ITEMS.VALUE:quantity :: FLOAT as quantity	,		
                ITEMS.VALUE:requires_shipping :: BOOLEAN as requires_shipping	,		
                ITEMS.VALUE:sku :: VARCHAR as items_sku	,		
                ITEMS.VALUE:subscription_price :: FLOAT as subscription_price	,		
                ITEMS.VALUE:taxable :: BOOLEAN as taxable	,		
                ITEMS.VALUE:title :: VARCHAR as	title ,		
                ITEMS.VALUE:variant_id :: INT as items_variant_id	,		
                ITEMS.VALUE:variant_title :: VARCHAR as variant_title	,		
                ITEMS.VALUE:vendor :: VARCHAR as vendor	,
            {% else %}
                items.compare_at_price	,		
                items.fulfillment_service	,		
                items.gift_card	,		
                items.grams	,		
                items.id as id	,		
                items.image_url	,		
                items.in_stock	,		
                items.key	,		
                items.line_price	,		
                items.original_line_price	,		
                items.original_price	,		
                items.price	,		
                cast(items.product_id as string) as product_id,		
                items.properties,
                items.quantity	,		
                items.requires_shipping	,		
                items.sku as items_sku,	
                items.subscription_price	,		
                items.taxable	,		
                items.title	,		
                cast(items.variant_id as string) as items_variant_id,	
                items.variant_title	,		
                items.vendor	,	
            {% endif %}	
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
            DENSE_RANK() OVER (PARTITION BY date(a.created_at) order by {{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {{unnesting("ITEMS")}} 
                {{unnesting("TRACKING_CODES")}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            ) 
        where row_num = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}