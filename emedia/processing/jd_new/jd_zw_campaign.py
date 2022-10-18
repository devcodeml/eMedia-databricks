# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, lit
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_zw_campaign_etl(airflow_execution_date, run_id):
    """
    airflow_execution_date: to identify upstream file
    """
    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    jd_zw_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/dmp_daily_Report/jd_dmp_campaign_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_zw_campaign_path: " + jd_zw_campaign_path)

    jd_zw_campaign_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_zw_campaign_path}",
        header=True,
        multiLine=True,
        sep="|",
    )

    # stg.jdzw_campaign_daily
    jd_zw_campaign_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.campaign.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdzw_campaign_daily"
    )

    # ods.jdzw_campaign_daily
    spark.sql(
        """
        select
          to_date(cast(`date` as string), 'yyyyMMdd') as ad_date,
          cast(pin as string) as pin_name,
          cast(req_clickOrOrderDay as string) as effect,
          case
              when req_clickOrOrderDay = 7 then '8'
              when req_clickOrOrderDay = 15 then '24'
              else cast(req_clickOrOrderDay as string)
          end as effect_days,
          cast(campaignId as string) as campaign_id,
          cast(campaignName as string) as campaign_name,
          cast(cost as decimal(20, 4)) as cost,
          cast(clicks as bigint) as clicks,
          cast(impressions as bigint) as impressions,
          cast(CPA as string) as cpa,
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
          cast(visitPageCnt as bigint) as visit_page_quantity,
          cast(visitTimeAverage as decimal(20, 4)) as visit_time_length,
          cast(visitorCnt as bigint) as visitor_quantity,
          cast(presaleDirectOrderCnt as bigint) as presale_direct_order_cnt,
          cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum, 
          cast(presaleIndirectOrderCnt as bigint) as presale_indirect_order_cnt,
          cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum,
          cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
          cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,
          cast(putType as string) as put_type,
          cast(campaignType as string) as campaign_type,
          cast(campaignType as string) as campaign_put_type,
          cast(mobileType as string) as mobile_type,
          cast(req_businessType as string) as business_type,
          cast(req_giftFlag as string) as gift_flag,
          cast(req_orderStatusCategory as string) as order_status_category,
          cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
          cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
          to_date(cast(clickDate as string), 'yyyyMMdd') as click_date,
          cast(deliveryVersion as string) as delivery_version,
          cast(req_startDay as date) as start_day,
          cast(req_endDay as date) as end_day,
          cast(req_isDaily as string) as is_daily,
          'stg.jdzw_campaign_daily' as data_source,
          cast(dw_batch_id as string) as dw_batch_id
        from stg.jdzw_campaign_daily
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdzw_campaign_daily"
    )

    # dwd.jdzw_campaign_daily
    jdzw_campaign_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "put_type",
        "campaign_type",
        "campaign_put_type",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]

    jdzw_campaign_df = (
        spark.table("ods.jdzw_campaign_daily").drop("dw_etl_date").drop("data_source")
    )
    jdzw_campaign_fail_df = (
        spark.table("dwd.jdzw_campaign_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jdzw_campaign_daily_mapping_success,
        jdzw_campaign_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark, jdzw_campaign_df.union(jdzw_campaign_fail_df), "dmp"
    )
    jdzw_campaign_daily_mapping_success.dropDuplicates(
        jdzw_campaign_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdzw_campaign_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in jdzw_campaign_pks]
    )
    spark.sql(
        f"""
            MERGE INTO {dwd_table}
            USING {tmp_table}
            ON {and_str}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
            """
    )

    jdzw_campaign_daily_mapping_fail.dropDuplicates(jdzw_campaign_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.jdzw_campaign_daily_mapping_fail")

    spark.table("dwd.jdzw_campaign_daily_mapping_success").union(
        spark.table("dwd.jdzw_campaign_daily_mapping_fail")
    ).createOrReplaceTempView("jdzw_campaign_daily")

    jdzw_campaign_daily_res = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from jdzw_campaign_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
          left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    )

    jdzw_campaign_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
        "campaign_id",
        "campaign_name",
        "cost",
        "clicks",
        "impressions",
        "cpa",
        "cpc",
        "cpm",
        "ctr",
        "total_order_roi",
        "total_order_cvs",
        "direct_cart_cnt",
        "indirect_cart_cnt",
        "total_cart_quantity",
        "direct_order_value",
        "indirect_order_value",
        "order_value",
        "direct_order_quantity",
        "indirect_order_quantity",
        "order_quantity",
        "favorite_item_quantity",
        "favorite_shop_quantity",
        "coupon_quantity",
        "preorder_quantity",
        "depth_passenger_quantity",
        "new_customer_quantity",
        "visit_page_quantity",
        "visit_time_length",
        "visitor_quantity",
        "presale_direct_order_cnt",
        "presale_direct_order_sum",
        "presale_indirect_order_cnt",
        "presale_indirect_order_sum",
        "total_presale_order_cnt",
        "total_presale_order_sum",
        "put_type",
        "campaign_type",
        "campaign_put_type",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "click_date",
        "delivery_version",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.jdzw_campaign_daily' as data_source",
        "dw_batch_id",
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdzw_campaign_daily"
    )

    return 0

    # dwd.tb_media_emedia_jdzw_daily_fact
