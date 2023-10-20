    {% if var('UpscribeSubscription') %}
        {{ config( enabled = True ) }}
    {% else %}
        {{ config( enabled = False ) }}
    {% endif %}


    {% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern=var('raw_schema'),
    table_pattern=var('upscribe_subscription_ptrn'),
    exclude=var('upscribe_subscription_tbl_exclude_ptrn'),
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
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
            coalesce(CAST(a.id as string),'NA') as subscription_id,
            import_id,
            a.interval,
            --items,
            --next,
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
            --tracking_codes,
            updated_at,
            {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
           
            from {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('upscribe_subscription_lookback') }},0) from {{ this }})
                {% endif %}
            qualify
             dense_rank() over (partition by date(created_at) order by {{daton_batch_runtime()}} desc) =1

      
        {% if not loop.last %} union all {% endif %}
    {% endfor %}

    