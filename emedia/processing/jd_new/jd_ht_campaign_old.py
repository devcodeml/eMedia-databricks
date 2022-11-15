# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, current_timestamp, lit, col, udf
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from pyspark.sql.types import DateType
spark = get_spark()


def jd_ht_campaign_etl_old():
    # dwd.jdht_campaign_daily
    get_history_data()
    spark.sql("delete from dwd.tb_media_emedia_jdht_daily_fact where report_level = 'campaign' and etl_source_table='dwd.jdht_campaign_daily'")

    spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from dwd.jdht_campaign_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    ).createOrReplaceTempView("jdht_campaign_daily")

    spark.sql(""" select 
            to_date(cast(`ad_date` as string), 'yyyyMMdd') as ad_date,
            '海投' as ad_format_lv2,
            pin_name,
            case
              when effect_days = '0' then '0'
              when effect_days = '1' then '1'
              when effect_days = '8' then '7'
              when effect_days = '24' then '15'
              else cast(effect_days as string)
            end as effect,
            effect_days,
            campaign_id,
            campaign_name,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            orderStatusCategory as order_status_category,
            req_clickOrOrderCaliber as click_or_order_caliber,
            cast(cost as decimal(20, 4)) as cost,
            cast(clicks as bigint) as click,
            cast(impressions as bigint) as impression,
            cast(totalOrderCnt as bigint) as order_quantity,
            cast(totalOrderSum as decimal(20, 4)) as order_value,
            cast(totalCartCnt as bigint) as total_cart_quantity,
            cast(newCustomersCnt as bigint) as new_customer_quantity,
            'dwd.jdht_campaign_daily' as dw_source,
            '' as dw_create_time,
            '' as dw_batch_number,
            'dwd.jdht_campaign_daily' as etl_source_table from jdht_campaign_daily where ad_date>='2022-02-01'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_jdht_daily_fact"
    )

    # stg.jdht_campaign_daily_old
    dw_to_dbr("dbo.tb_emedia_jg_ht_campaign_fact", "stg.jdht_campaign_daily_old")
    jdht_campaign_daily_old = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id_new,
          c.brand_code as mdm_brand_id_new
        from stg.jdht_campaign_daily_old a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    )

    def f_mau_vec(effect):
        if effect == "0":
            return "0"
        elif effect == "1":
            return "1"
        elif effect == "7":
            return "8"
        elif effect == "15":
            return "24"
        else:
            return effect
    udf_mau_vec = udf(f_mau_vec, StringType())

    jdht_campaign_daily_old = jdht_campaign_daily_old.withColumn('effect_days', udf_mau_vec(col('effect')))
    jdht_campaign_daily_old = jdht_campaign_daily_old.drop(*["mdm_category_id", "mdm_brand_id"])
    jdht_campaign_daily_old = jdht_campaign_daily_old.withColumn("ad_date", jdht_campaign_daily_old['ad_date'].cast(DateType()))
    jdht_campaign_daily_old = jdht_campaign_daily_old.withColumnRenamed("mdm_category_id_new", "mdm_category_id")
    jdht_campaign_daily_old = jdht_campaign_daily_old.withColumnRenamed("mdm_brand_id_new", "mdm_brand_id")
    jdht_campaign_daily_old.write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdht_campaign_daily_old"
    )
    spark.sql("delete from dwd.tb_media_emedia_jdht_daily_fact where report_level = 'campaign' and etl_source_table='dwd.jdht_campaign_daily_old'")
    spark.sql(""" select 
            cast(ad_date as date) as ad_date,
            '海投' as ad_format_lv2,
            pin_name,
            effect,
            case
              when effect = '0' then '0'
              when effect = '7' then '1'
              when effect = '7' then '8'
              when effect = '15' then '24'
              else cast(effect as string)
            end as effect_days,
            campaign_id,
            campaign_name,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            '' as order_status_category,
            cast(click_or_order_caliber as string) as click_or_order_caliber,
            cast(cost as decimal(20, 4)) as cost,
            cast(clicks as bigint) as click,
            cast(impressions as bigint) as impression,
            cast(total_order_cnt as bigint) as order_quantity,
            cast(total_order_sum as decimal(20, 4)) as order_value,
            cast(total_cart_cnt as bigint) as total_cart_quantity,
            cast(new_customers_cnt as bigint) as new_customer_quantity,
            data_source as dw_source,
            cast(dw_etl_date as string) as dw_create_time,
            dw_batch_id as dw_batch_number,
            'dwd.jdht_campaign_daily_old' as etl_source_table from dwd.jdht_campaign_daily_old where ad_date<'2022-02-01'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_jdht_daily_fact"
    )

    # 推送到 dws.media_emedia_overview_daily_fact
    # spark.sql("delete from dws.media_emedia_overview_daily_fact where ad_format_lv2 = '海投' ")
    #
    # spark.sql(""" select
    #         ad_date,
    #         ad_format_lv2,
    #         '' as store_id,
    #         effect,
    #         effect_days,
    #         emedia_category_id,
    #         emedia_brand_id,
    #         mdm_category_id,
    #         mdm_brand_id,
    #         sum(cost) as cost,
    #         sum(click) as click,
    #         sum(impression) as impression,
    #         cast(null as decimal(20, 4)) as uv_impression,
    #         sum(order_quantity) as order_quantity,
    #         sum(order_value) as order_amount,
    #         sum(total_cart_quantity) as cart_quantity,
    #         sum(new_customer_quantity) as new_customer_quantity
    #         from dwd.tb_media_emedia_jdht_daily_fact group by ad_date, ad_format_lv2, effect, effect_days, emedia_category_id, emedia_brand_id, mdm_brand_id, mdm_category_id"""
    #       ).distinct().withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time",                                                                                current_timestamp()).write.mode(
    #     "append"
    # ).option(
    #     "mergeSchema", "true"
    # ).insertInto(
    #     "dws.media_emedia_overview_daily_fact"
    # )

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


def get_history_data():
    output_jd_ht_campaign_pks = [
        'ad_date'
        , 'pin_name'
        , 'campaign_id'
        , 'effect_days'
        , 'req_clickOrOrderDay'
    ]
    spark.sql(f'''
        SELECT
            date AS ad_date
            , req_pin AS pin_name
            , category_id
            , brand_id
            , campaignId AS campaign_id
            , campaignName AS campaign_name
            , CASE req_clickOrOrderDay WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days
            , impressions
            , clicks
            , cost
            , directCartCnt
            , directOrderCnt
            , directOrderSum
            , indirectCartCnt
            , indirectOrderCnt
            , indirectOrderSum
            , totalCartCnt
            , totalOrderCnt
            , totalOrderSum
            , activityType
            , commentCnt
            , couponCnt
            , CPA
            , CPC
            , CPM
            , CPR
            , CTR
            , depthPassengerCnt
            , effectCartCnt
            , effectOrderCnt
            , effectOrderSum
            , followCnt
            , goodsAttentionCnt
            , interActCnt
            , IR
            , likeCnt
            , newCustomersCnt
            , clickDate
            , orderStatusCategory
            , preorderCnt
            , presaleDirectOrderCnt
            , presaleDirectOrderSum
            , presaleIndirectOrderCnt
            , presaleIndirectOrderSum
            , shareCnt
            , shopAttentionCnt
            , totalAuctionCnt
            , totalAuctionMarginSum
            , totalOrderCVS
            , totalOrderROI
            , totalPresaleOrderCnt
            , totalPresaleOrderSum
            , visitorCnt
            , visitPageCnt
            , visitTimeAverage
            , watchCnt
            , watchTimeAvg
            , watchTimeSum
            , req_startDay
            , req_endDay
            , req_productName
            , req_page
            , req_pageSize
            , req_clickOrOrderDay
            , req_clickOrOrderCaliber
            , req_isDaily
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_ht_campaign_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_ht_campaign_mapping_fail
        )
    ''').dropDuplicates(output_jd_ht_campaign_pks).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdht_campaign_daily"
    )
    return 0