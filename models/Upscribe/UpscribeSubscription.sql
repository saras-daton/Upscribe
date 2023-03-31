    {% if var('UpscribeSubscription') %}
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
            '{{brand}}' as Brand,
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
            coalesce(CAST(a.id as string),'') as subscription_id,
            import_id,
            a.interval,
            items,
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
            tracking_codes,
            updated_at,
            {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            DENSE_RANK() OVER (PARTITION BY date(created_at) order by {{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            )
        where row_num = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}

    