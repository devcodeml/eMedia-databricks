# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, json_tuple, lit, udf
from pyspark.sql.types import StringType

from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_gwcd_adgroup_etl(airflow_execution_date, run_id):
    """
    airflow_execution_date: to identify upstream file
    """
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

    jd_gwcd_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/gwcd_daily_Report/jd_gwcd_adgroup_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_gwcd_adgroup_path: " + jd_gwcd_adgroup_path)

    jd_gwcd_adgroup_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_gwcd_adgroup_path}",
        header=True,
        multiLine=True,
        sep="|",
    )

    jd_gwcd_adgroup_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.group.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.gwcd_adgroup_daily"
    )

    retrieval_types = [
        col
        for col in jd_gwcd_adgroup_daily_df.columns
        if col.startswith("retrievalType")
    ]

    update = udf(
        lambda x: x.strip('"').strip("[").strip("]").replace('""', '"'), StringType()
    )
    spark.udf.register("update", update)
    spark.sql(
        f"""
      select
        dw_etl_date,
        date,
        adBillingType,
        IR,
        adGroupId,
        pin,
        commentCnt,
        followCnt,
        shareCnt,
        campaignType,
        adGroupName,
        campaignId,
        deliveryType,
        likeCnt,
        interactCnt,
        campaignName,
        req_giftFlag,
        req_startDay,
        req_endDay,
        req_clickOrOrderDay,
        req_orderStatusCategory,
        req_page,
        req_pageSize,
        req_impressionOrClickEffect,
        req_clickOrOrderCaliber,
        req_isDaily,
        req_businessType,
        req_pin,
        data_source,
        dw_batch_id,
        stack({len(retrieval_types)},
                {", ".join([f"'{col[-1]}', update(`{col}`)" for col in retrieval_types])}
                ) AS (`source`, `retrievalType`)
      from stg.gwcd_adgroup_daily
      """
    ).select(
        "*",
        json_tuple(
            "retrievalType",
            "CTR",
            "depthPassengerCnt",
            "CPM",
            "videoInteractRate",
            "preorderCnt",
            "indirectOrderCnt",
            "indirectCartCnt",
            "videoEfficientplayCnt",
            "videoFinishRate",
            "visitPageCnt",
            "visitTimeAverage",
            "videoCommentCnt",
            "directCartCnt",
            "videoUV",
            "videoPlayTimeSum",
            "couponCnt",
            "totalOrderSum",
            "totalOrderROI",
            "newCustomersCnt",
            "videoClickUV",
            "indirectOrderSum",
            "directOrderSum",
            "goodsAttentionCnt",
            "watchTimeAvg",
            "videoInteractCnt",
            "videoEfficientplayRate",
            "totalOrderCVS",
            "watchTimeSum",
            "presaleDirectOrderSum",
            "totalOrderCnt",
            "shopAttentionCnt",
            "videoFinishCnt",
            "presaleIndirectOrderSum",
            "presaleDirectOrderCnt",
            "videoLikeCnt",
            "directOrderCnt",
            "videoPlayCnt",
            "totalPresaleOrderSum",
            "visitorCnt",
            "cost",
            "totalCartCnt",
            "presaleIndirectOrderCnt",
            "watchCnt",
            "videoShareCnt",
            "impressions",
            "CPA",
            "CPC",
            "totalPresaleOrderCnt",
            "clicks",
        ).alias(
            "CTR",
            "depthPassengerCnt",
            "CPM",
            "videoInteractRate",
            "preorderCnt",
            "indirectOrderCnt",
            "indirectCartCnt",
            "videoEfficientplayCnt",
            "videoFinishRate",
            "visitPageCnt",
            "visitTimeAverage",
            "videoCommentCnt",
            "directCartCnt",
            "videoUV",
            "videoPlayTimeSum",
            "couponCnt",
            "totalOrderSum",
            "totalOrderROI",
            "newCustomersCnt",
            "videoClickUV",
            "indirectOrderSum",
            "directOrderSum",
            "goodsAttentionCnt",
            "watchTimeAvg",
            "videoInteractCnt",
            "videoEfficientplayRate",
            "totalOrderCVS",
            "watchTimeSum",
            "presaleDirectOrderSum",
            "totalOrderCnt",
            "shopAttentionCnt",
            "videoFinishCnt",
            "presaleIndirectOrderSum",
            "presaleDirectOrderCnt",
            "videoLikeCnt",
            "directOrderCnt",
            "videoPlayCnt",
            "totalPresaleOrderSum",
            "visitorCnt",
            "cost",
            "totalCartCnt",
            "presaleIndirectOrderCnt",
            "watchCnt",
            "videoShareCnt",
            "impressions",
            "CPA",
            "CPC",
            "totalPresaleOrderCnt",
            "clicks",
        ),
    ).drop(
        "retrievalType"
    ).createOrReplaceTempView(
        "stack_retrivialType"
    )

    spark.sql(
        """
        select
          to_date(cast(`date` as string), 'yyyyMMdd') as ad_date,
          cast(pin as string) as pin,
          cast(req_clickOrOrderDay as string) as effect,
          case
              when req_clickOrOrderDay = 7 then '8'
              when req_clickOrOrderDay = 15 then '24'
              else cast(req_clickOrOrderDay as string)
          end as effect_days,
          cast(campaignId as string) as campaign_id,
          cast(campaignName as string) as campaignName,
          cast(adGroupId as string) as adgroup_id,
          cast(adGroupName as string) as adgroup_name,
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
          cast(videoInteractRate as decimal(9, 4)) as video_interact_rate,
          cast(videoEfficientplayCnt as bigint) as video_efficientplay_cnt,
          cast(videoFinishRate as decimal(9, 4)) as video_finish_rate,
          cast(videoCommentCnt as bigint) as video_comment_cnt,
          cast(videoUV as bigint) as video_uv,
          cast(videoPlayTimeSum as decimal(20, 4)) as video_play_time_sum,
          cast(videoClickUV as bigint) as video_click_uv,
          cast(watchTimeAvg as decimal(20, 4)) as watch_time_avg,
          cast(videoInteractCnt as bigint) as video_interact_cnt,
          cast(videoEfficientplayRate as decimal(20, 4)) as video_efficientplay_rate,
          cast(watchTimeSum as decimal(20, 4)) as watch_time_sum,
          cast(videoFinishCnt as bigint) as video_finish_cnt,
          cast(videoLikeCnt as bigint) as video_like_cnt,
          cast(videoPlayCnt as bigint) as video_play_cnt,
          cast(watchCnt as bigint) as watch_cnt,
          cast(videoShareCnt as bigint) as video_share_cnt,
          cast(adBillingType as string) as ad_billing_type,
          cast(IR as decimal(20, 4)) as ir,
          cast(likeCnt as bigint) as like_cnt,
          cast(commentCnt as bigint) as comment_cnt,
          cast(followCnt as bigint) as follow_cnt,
          cast(shareCnt as bigint) as share_cnt,
          cast(campaignType as string) as campaign_type,
          cast(deliveryType as string) as delivery_type,
          cast(req_businessType as string) as business_type,
          cast(req_giftFlag as string) as gift_flag,
          cast(req_orderStatusCategory as string) as order_status_category,
          cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
          cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
          cast(source as string) as source,
          cast(req_startDay as date) as start_day,
          cast(req_endDay as date) as end_day,
          cast(req_isDaily as string) as is_daily,
          'stg.gwcd_adgroup_daily' as data_source,
          cast(dw_batch_id as string) as dw_batch_id
        from stack_retrivialType
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.gwcd_adgroup_daily"
    )

    jd_gwcd_adgroup_pks = [
        "ad_date",
        "pin",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "campaign_type",
        "delivery_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "source",
    ]

    jd_gwcd_adgroup_df = (
        spark.table("ods.gwcd_adgroup_daily").drop("dw_etl_date").drop("data_source")
    )
    jd_gwcd_adgroup_fail_df = (
        spark.table("dwd.gwcd_adgroup_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jd_gwcd_adgroup_daily_mapping_success,
        jd_gwcd_adgroup_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark, jd_gwcd_adgroup_df.union(jd_gwcd_adgroup_fail_df), "gwcd"
    )
    jd_gwcd_adgroup_daily_mapping_success.dropDuplicates(
        jd_gwcd_adgroup_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.gwcd_adgroup_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in jd_gwcd_adgroup_pks]
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

    jd_gwcd_adgroup_daily_mapping_fail.dropDuplicates(jd_gwcd_adgroup_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.gwcd_adgroup_daily_mapping_fail")

    spark.table("dwd.gwcd_adgroup_daily_mapping_success").union(
        spark.table("dwd.gwcd_adgroup_daily_mapping_fail")
    ).createOrReplaceTempView("gwcd_adgroup_daily")

    gwcd_adgroup_daily_res = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          c.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from gwcd_adgroup_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code and
          a.category_id = c.emedia_category_code
        """
    )

    gwcd_adgroup_daily_res.selectExpr(
        "ad_date",
        "pin as pin_name",
        "effect",
        "effect_days",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
        "campaign_id",
        "campaignName as campaign_name",
        "adgroup_id",
        "adgroup_name",
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
        "video_interact_rate",
        "video_efficientplay_cnt",
        "video_finish_rate",
        "video_comment_cnt",
        "video_uv",
        "video_play_time_sum",
        "video_click_uv",
        "watch_time_avg",
        "video_interact_cnt",
        "video_efficientplay_rate",
        "watch_time_sum",
        "video_finish_cnt",
        "video_like_cnt",
        "video_play_cnt",
        "watch_cnt",
        "video_share_cnt",
        "ad_billing_type",
        "ir",
        "like_cnt",
        "comment_cnt",
        "follow_cnt",
        "share_cnt",
        "campaign_type",
        "delivery_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "source",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.gwcd_adgroup_daily' as data_source",
        "dw_batch_id",
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.gwcd_adgroup_daily"
    )

    # dwd.tb_media_emedia_gwcd_daily_fact

    # stg.gwcd_adgroup_daily
    # ods.gwcd_adgroup_daily
    # dwd.gwcd_adgroup_daily

    spark.sql("optimize dwd.gwcd_adgroup_daily_mapping_success")

    # create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0
