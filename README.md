# Upscribe Data Unification

This dbt package is for the Upscribe data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Amazon Seller Partner
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Upscribe
- Exchange Rates(Optional, if currency conversion is not required)

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  upscibe:
    +schema: custom_schema_name
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the datetime columns from local timezone to given timezone, please mark the timezone_conversion_flag f as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours for each raw table

Example:
```yaml
vars:
  timezone_conversion_flag : True
  raw_table_timezone_offset_hours: {
    "edm-saras.EDM_Daton.Brand_US_Upscribe_BQ_Customers" : -7,
    "edm-saras.EDM_Daton.Brand_US_Upscribe_BQ_Orders" : -7,
    "edm-saras.EDM_Daton.Brand_US_Upscribe_BQ_Subscriptions" : -7,
    "edm-saras.EDM_Daton.Brand_US_Upscribe_BQ_Refunds"  : -7,
    "edm-saras.EDM_Daton.Brand_US_Upscribe_BQ_SubscriptionQueues" : -7,
    "edm-saras.EDM_Daton.Brand_US_Upscribe_BQ_Products" : -7 
    }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
  UpscribeSubscriptionItems : True
  UpscribeSubscription : True
  UpscribeCustomers : True
  UpscribeOrders : True
  UpscribeRefunds : True
  UpscribeProducts : True
  UpscribeSubscriptionQueues : True
  UpscribeCollections : True
```

## Models

This package contains models from the Amazon Selling Partner API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Inventory | [UpscribeCollections](models/Upscribe/UpscribeCollections.sql)  | This table returns all the List of collections information associated with upscribe account and your given domain. |
|Customer | [UpscribeCustomers](models/Upscribe/UpscribeCustomers.sql)  | This table returns all the List of customer information and cards associated with upscribe account and your given domain. |
|Order | [UpscribeOrders](models/Upscribe/UpscribeOrders.sql)  | This table returns all the List of order information associated with upscribe account and your given domain. |
|Order | [UpscribeOrdersTaxLines](models/Upscribe/UpscribeOrdersTaxLines.sql)  | This table returns all the List of order information associated with upscribe account along the tax lines. |
|Product | [UpscribeProducts](models/Upscribe/UpscribeProducts.sql)  |This table returns all the List of product information associated with upscribe account and your given domain. |
|Refund | [UpscribeRefunds](models/Upscribe/UpscribeRefunds.sql)  | This table returns all the List of refunds associated with upscribe account and your given domain. |
|Subscription | [UpscribeSubscription](models/Upscribe/UpscribeSubscription.sql)  | This table returns all the List of subscriptions associated with upscribe account and your given domain.|
|Subscription | [UpscribeSubscriptionItems](models/Upscribe/UpscribeSubscriptionItems.sql)  | This table returns all the List of subscriptions associated with upscribe account and your given domain.|
|Subscription | [UpscribeSubscriptionQueues](models/Upscribe/UpscribeSubscriptionQueues.sql)  | This table returns all the List of subscriptions changelogs associated with upscribe account and your given domain|


### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: UpscribeSubscriptionItems
    description: This table provides Returns all the List of subscriptions associated with upscribe account and given domain based on the item.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['subscription_id', 'items_sku','id']
        partition_by : { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': day }
        granularity: 'day'
        cluster_by : ['subscription_id']

  - name: UpscribeSubscription
    description: This table returns all the List of subscriptions associated with upscribe account and your given domain.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['subscription_id']
        partition_by : { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': day }
        granularity: 'day'
        cluster_by : ['subscription_id']

  - name: UpscribeCollections
    description: This table returns all the List of collections information associated with upscribe account and your given domain.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id']
        partition_by : {'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day'}
        cluster_by : ['id']


  - name: UpscribeCustomers
    description: This table returns all the List of customer information and cards associated with upscribe account and your given domain.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id']
        partition_by : {'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
        cluster_by : ['id']

  - name: UpscribeOrders
    description: This table returns all the List of order information associated with upscribe account and your given domain.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id','order_number','line_items_id']
        partition_by : {'field': 'updated_at', 'data_type': 'timestamp'}
        cluster_by : ['id'] 
    
  - name: UpscribeOrdersTaxLines
    description: This table returns all the List of order information associated with upscribe account along the tax lines.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id','order_number']
        partition_by : {'field': 'updated_at', 'data_type': 'timestamp'}
        cluster_by : ['id'] 

  - name: UpscribeProducts
    description: This table returns all the List of product information associated with upscribe account and your given domain.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id']
        partition_by : {'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day'}
        cluster_by : ['id']

  - name: UpscribeRefunds
    description: This table returns all the List of refunds associated with upscribe account and your given domain.
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id']
        partition_by : {'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day'}
        cluster_by : ['id']

  - name: UpscribeSubscriptionQueues
    description: This table returns all the List of subscriptions changelogs associated with upscribe account and your given domain
    config:
        materialized : incremental
        incremental_strategy : merge
        unique_key : ['id','shopify_order_id']
        partition_by : {'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day'}
        cluster_by : ['id']
```
## Resources:
- Have questions, feedback, or need [help](https://calendly.com/priyanka-vankadaru/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
