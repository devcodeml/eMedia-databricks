# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, json_tuple, lit, current_timestamp

import pyspark.sql.functions as F
from pyspark.sql.types import *
from emedia import log, get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping2 import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia

spark = get_spark()


def jd_jdzt_account_daily_etl(airflow_execution_date,run_id):
    file_date = datetime.datetime.strptime(
        airflow_execution_date, "%Y-%m-%d %H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    jdzt_account_daily_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/jd/zt_daily_Report"
        f"/jd_zt_account_{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    origin_jdzt_account_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{jdzt_account_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    )
    # origin_jdzt_account_daily_df.show()

    origin_jdzt_account_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.account.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdzt_account_daily"
    )

    spark.sql(
        """
        select
            cast(`date` as date) as ad_date,
            cast(req_pin as string) as pin_name,
            cast(pinId as string) as pin_id,
            cast(req_clickOrOrderDay as string) as effect,
            case
                when req_clickOrOrderDay = 7 then '8'
                when req_clickOrOrderDay = 15 then '24'
                else cast(req_clickOrOrderDay as string)
            end as effect_days,

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
            cast(presaleIndirectOrderCnt as bigint) as presale_indirect_order_cnt,
            cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
            cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum,
            cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum,
            cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,

            to_date(cast(`clickDate` as string), 'yyyyMMdd') as click_date,
            cast(platformCnt as bigint) as platform_cnt,
            cast(platformGmv as decimal(20, 4)) as platform_gmv,
            cast(departmentCnt as bigint) as department_cnt,
            cast(departmentGmv as decimal(20, 4)) as department_gmv,
            cast(channelROI as decimal(20, 4)) as channel_roi,

            cast(mobileType as string) as mobile_type,
            cast(req_businessType as string) as business_type,
            cast(req_giftFlag as string) as gift_flag,
            cast(req_orderStatusCategory as string) as order_status_category,
            cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
            cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
            cast(req_startDay as date) as start_day,
            cast(req_endDay as date) as end_day,
            cast(req_isDaily as string) as is_daily,
            cast(data_source as string) as data_source,
            cast(dw_batch_id as string) as dw_batch_id
        from stg.jdzt_account_daily
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdzt_account_daily"
    )

    jd_zt_account_daily_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]

    jdzt_account_df = (
        spark.table("ods.jdzt_account_daily").drop("dw_etl_date").drop("data_source")
    )
    # jdzc_account_fail_df = (
    #     spark.table("dwd.jdzc_account_daily_mapping_fail")
    #     .drop("category_id")
    #     .drop("brand_id")
    #     .drop("etl_date")
    #     .drop("etl_create_time")
    # )

    (
        jdzt_account_daily_mapping_success,
        jdzt_account_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark, jdzt_account_df, "jdzt"
    )

    jdzt_account_daily_mapping_success.dropDuplicates(
        jd_zt_account_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdzt_account_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in jd_zt_account_daily_pks]
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

    jdzt_account_daily_mapping_fail.dropDuplicates(jd_zt_account_daily_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.jdzt_account_daily_mapping_fail")

    jdzt_account_daily_res = spark.table("dwd.jdzt_account_daily_mapping_success").union(
        spark.table("dwd.jdzt_account_daily_mapping_fail")
    )

    jdzt_account_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "pin_id",
        "effect",
        "effect_days",
        "category_id",
        "brand_id",
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
        "presale_indirect_order_cnt",
        "total_presale_order_cnt",
        "presale_direct_order_sum",
        "presale_indirect_order_sum",
        "total_presale_order_sum",
        "click_date",
        "platform_cnt",
        "platform_gmv",
        "department_cnt",
        "department_gmv",
        "channel_roi",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.jdzt_account_daily' as data_source",
        "dw_batch_id"
    ).distinct().withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdzt_account_daily"
    )
    return 0