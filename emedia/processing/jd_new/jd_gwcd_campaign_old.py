# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, current_timestamp, lit
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_gwcd_campaign_etl_old():
    # stg.gwcd_campaign_daily_old_v2 处理
    dw_to_dbr("dbo.tb_emedia_jg_gwcd_tp_fact", "stg.gwcd_campaign_daily_old_v2")
    spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from stg.gwcd_campaign_daily_old_v2 a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    ).createOrReplaceTempView("gwcd_campaign_daily_old_v2")

    spark.sql(
        """
        select
          cast(`ad_date` as date) as ad_date,
          cast(pin_name as string) as pin_name,
          cast(effect as string) as effect,
          case
              when effect = '0' then '0'
              when effect = '7' then '1'
              when effect = '7' then '8'
              when effect = '15' then '24'
              else cast(effect as string)
          end as effect_days,
          cast(emedia_category_id as string) as emedia_category_id,
          cast(emedia_brand_id as string) as emedia_brand_id,
          case
              when mdm_category_id is null then ''
              else cast(mdm_category_id as string)
          end as mdm_category_id,
          case
              when mdm_brand_id is null then ''
              else cast(mdm_brand_id as string)
          end as mdm_brand_id,
          cast(mdm_productline_id as string) as mdm_productline_id,
          cast(campaign_id as string) as campaign_id,
          cast(campaign_name as string) as campaign_name,
          cast(cost as decimal(20, 4)) as cost,
          cast(clicks as bigint) as clicks,
          cast(impressions as bigint) as impressions,
          cast('' as string) as cpa,
          cast(cpc as decimal(20, 4)) as cpc,
          cast(cpm as decimal(20, 4)) as cpm,
          cast(ctr as decimal(9, 4)) as ctr,
          cast(total_order_roi as decimal(9, 4)) as total_order_roi,
          cast(total_order_cvs as decimal(9, 4)) as total_order_cvs,
          cast(direct_cart_cnt as bigint) direct_cart_cnt,
          cast(indirect_cart_cnt as bigint) as indirect_cart_cnt,
          cast(total_cart_cnt as bigint) as total_cart_quantity,
          cast(direct_order_sum as decimal(20, 4)) as direct_order_value,
          cast(indirect_order_sum as decimal(20, 4)) as indirect_order_value,
          cast(total_order_sum as decimal(20, 4)) as order_value,
          cast(direct_order_cnt as bigint) as direct_order_quantity,
          cast(indirect_order_cnt as bigint) as indirect_order_quantity,
          cast(total_order_cnt as bigint) as order_quantity,
          cast(goods_attention_cnt as bigint) as favorite_item_quantity,
          cast(shop_attention_cnt as bigint) as favorite_shop_quantity,
          cast(coupon_cnt as bigint) as coupon_quantity,
          cast(preorder_cnt as bigint) as preorder_quantity,
          cast(depth_passenger_cnt as bigint) as depth_passenger_quantity,
          cast(new_customers_cnt as bigint) as new_customer_quantity,
          cast(visit_time_average as decimal(20, 4)) as visit_time_length,
          cast(visitor_cnt as bigint) as visitor_quantity,
          cast(visit_page_cnt as bigint) as visit_page_quantity,
          cast(null as bigint) as presale_direct_order_cnt,
          cast(null as bigint) as presale_indirect_order_cnt,
          cast(null as bigint) as total_presale_order_cnt,
          cast(null as decimal(20, 4)) as presale_direct_order_sum,
          cast(null as decimal(20, 4)) as presale_indirect_order_sum,
          cast(null as decimal(20, 4)) as total_presale_order_sum,
          cast('' as string) as deliveryVersion,
          cast('' as string) as put_type,
          cast('' as string) as mobile_type,
          cast(campaign_type as string) as campaign_type,
          cast(campaign_type as string) as campaign_put_type,
          cast('' as string) as click_date,
          cast('' as string) as business_type,
          cast('' as string) as gift_flag,
          cast('' as string) as order_status_category,
          cast(click_or_order_caliber as string) as click_or_order_caliber,
          cast('' as string) as impression_or_click_effect,
          cast(null as date) as start_day,
          cast(null as date) as end_day,
          cast('' as string) as is_daily,
          'stg.gwcd_campaign_daily_old_v2' as data_source,
          cast(dw_batch_id as string) as dw_batch_id
        from gwcd_campaign_daily_old_v2
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.gwcd_campaign_daily_old_v2"
    )

    spark.sql("delete from dwd.tb_media_emedia_gwcd_daily_fact where report_level = 'campaign' and etl_source_table='dwd.gwcd_campaign_daily_old_v2' ")


    spark.sql(""" select 
            ad_date,
            '购物触点' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            '' as adgroup_id,
            '' as adgroup_name,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            campaign_type,
            deliveryVersion as delivery_version,
            '' as delivery_type,
            mobile_type,
            '' as source,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            put_type,
            campaign_put_type,
            cost,
            clicks as click,
            impressions as impression,
            order_quantity,
            order_value,
            total_cart_quantity,
            new_customer_quantity,
            data_source as dw_source,
            cast(dw_etl_date as string) as dw_create_time,
            dw_batch_id as dw_batch_number,
            'dwd.gwcd_campaign_daily_old_v2' as etl_source_table from dwd.gwcd_campaign_daily_old_v2 where ad_date<'2022-02-01'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_gwcd_daily_fact"
    )


    # stg.gwcd_campaign_daily_old 处理
    dw_to_dbr("dbo.tb_emedia_jd_gwcd_daily_campaign_report_fact", "stg.gwcd_campaign_daily_old")
    gwcd_campaign_daily_res_old = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id_new,
          c.brand_code as mdm_brand_id_new
        from stg.gwcd_campaign_daily_old a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    )

    gwcd_campaign_daily_res_old = gwcd_campaign_daily_res_old.drop(*["mdm_category_id", "mdm_brand_id"])
    gwcd_campaign_daily_res_old = gwcd_campaign_daily_res_old.withColumnRenamed("mdm_category_id_new", "mdm_category_id")
    gwcd_campaign_daily_res_old = gwcd_campaign_daily_res_old.withColumnRenamed("mdm_brand_id_new", "mdm_brand_id")
    gwcd_campaign_daily_res_old.createOrReplaceTempView("gwcd_campaign_daily_old")

    spark.sql(
        """
        select
          cast(`ad_date` as date) as ad_date,
          cast(pin_name as string) as pin_name,
   
          case
            when req_clickOrOrderDay = '0E-8' then '0'
            when cast(req_clickOrOrderDay as bigint) = req_clickOrOrderDay then cast(cast(req_clickOrOrderDay as bigint) as string) 
          else cast(req_clickOrOrderDay as string)
          end as effect,
          
          case
              when req_clickOrOrderDay = 0 then '0'
              when req_clickOrOrderDay = 1 then '1'
              when req_clickOrOrderDay = 7 then '8'
              when req_clickOrOrderDay = 15 then '24'
              else cast(req_clickOrOrderDay as string)
          end as effect_days,
          cast(emedia_category_id as string) as emedia_category_id,
          cast(emedia_brand_id as string) as emedia_brand_id,
          case
              when mdm_category_id is null then ''
              else cast(mdm_category_id as string)
          end as mdm_category_id,
          case
              when mdm_brand_id is null then ''
              else cast(mdm_brand_id as string)
          end as mdm_brand_id,
          cast(campaign_id as string) as campaign_id,
          cast(campaign_name as string) as campaign_name,
          cast(cost as decimal(20, 4)) as cost,
          cast(clicks as bigint) as clicks,
          cast(impressions as bigint) as impressions,          
          case
            when CPA = '0E-8' then '0'
          else cast(CPA as string)
          end as cpa,
          cast(CPC as decimal(20, 4)) as cpc,
          cast(CPM as decimal(20, 4)) as cpm,
          cast(CTR as decimal(9, 4)) as ctr,
          cast(totalOrderROI as decimal(9, 4)) as total_order_roi,
          cast(totalOrderCVS as decimal(9, 4)) as total_order_cvs,
          cast(directCartCnt as bigint) as direct_cart_cnt,
          cast(indirectCartCnt as bigint) as indirect_cart_cnt,
          cast(totalCartCnt as bigint) as total_cart_quantity,
          cast(directOrderSum as decimal(20, 4)) as direct_order_value,
          cast(indirectOrderSum as decimal(20, 4)) as indirect_order_value,
          cast(totalOrderSum as decimal(20, 4)) as order_value,
          cast(directOrderCnt as bigint) as direct_order_quantity,
          cast(indirectOrderCnt as bigint) as indirect_order_quantity,
          cast(totalOrderCnt as bigint) as order_quantity,
          cast(goodsAttentionCnt as bigint) as favorite_item_quantity,
          cast(shopAttentionCnt as bigint) as favorite_shop_quantity,
          cast(couponCnt as bigint) as coupon_quantity,
          cast(preorderCnt as bigint) as preorder_quantity,
          cast(depthPassengerCnt as bigint) as depth_passenger_quantity,
          cast(newCustomersCnt as bigint) as new_customer_quantity,
          cast(visitTimeAverage as decimal(20, 4)) as visit_time_length,
          cast(visitorCnt as bigint) as visitor_quantity,
          cast(visitPageCnt as bigint) as visit_page_quantity,
          cast(presaleDirectOrderCnt as bigint) as presale_direct_order_cnt,
          cast(presaleIndirectOrderCnt as bigint) as presale_indirect_order_cnt,
          cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
          cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum,
          cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum,
          cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,
          case
              when deliveryVersion = '0E-8' then '0'
              else cast(deliveryVersion as string)
          end as deliveryVersion,
          cast(putType as string) as put_type,
          cast('' as string) as mobile_type,          
          case
            when campaign_type = '0E-8' then '0'
            when cast(campaign_type as bigint) = campaign_type then cast(cast(campaign_type as bigint) as string) 
          else cast(campaign_type as string)
          end as campaign_type,
          
          case
            when campaign_put_type = '0E-8' then '0'
            when cast(campaign_put_type as bigint) = campaign_put_type then cast(cast(campaign_put_type as bigint) as string) 
          else cast(campaign_put_type as string)
          end as campaign_put_type,
          
          to_date(cast(clickDate as string), 'yyyyMMdd') as click_date,
          cast('' as string) as business_type,
          cast('' as string) as gift_flag,
          cast('' as string) as order_status_category,

          case
            when req_clickOrOrderCaliber = '0E-8' then '0'
            when cast(req_clickOrOrderCaliber as bigint) = req_clickOrOrderCaliber then cast(cast(req_clickOrOrderCaliber as bigint) as string) 
          else cast(req_clickOrOrderCaliber as string)
          end as click_or_order_caliber,
    
          cast('' as string) as impression_or_click_effect,
          cast(req_startDay as date) as start_day,
          cast(req_endDay as date) as end_day,
          cast(req_isDaily as string) as is_daily,
          'stg.gwcd_campaign_daily_old' as data_source,
          cast('' as string) as dw_batch_id
        from gwcd_campaign_daily_old
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.gwcd_campaign_daily_old"
    )
    spark.sql("delete from dwd.tb_media_emedia_gwcd_daily_fact where report_level = 'campaign' and etl_source_table='dwd.gwcd_campaign_daily_old' ")


    spark.sql(""" select 
            ad_date,
            '购物触点' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            '' as adgroup_id,
            '' as adgroup_name,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            campaign_type,
            deliveryVersion as delivery_version,
            '' as delivery_type,
            mobile_type,
            '' as source,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            put_type,
            campaign_put_type,
            cost,
            clicks as click,
            impressions as impression,
            order_quantity,
            order_value,
            total_cart_quantity,
            new_customer_quantity,
            data_source as dw_source,
            cast(dw_etl_date as string) as dw_create_time,
            dw_batch_id as dw_batch_number,
            'dwd.gwcd_campaign_daily_old' as etl_source_table from dwd.gwcd_campaign_daily_old where ad_date>='2022-02-01' and ad_date<='2022-10-12'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_gwcd_daily_fact"
    )

    return 0


def dw_to_dbr(dw_table, dbr_table):
    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'pgadmin'
    password = 'C4AfoNNqxHAJvfzK'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                f"select * from {dw_table}") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").insertInto(dbr_table)

    return 0


# dw_table = 'emedia.jd_ppzq_cost'
# dbr_table = 'stg.emedia_jd_ppzq_cost_mapping'
# dw_to_dbr(dw_table, dbr_table)