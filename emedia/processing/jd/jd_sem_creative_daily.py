# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, json_tuple, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jdkc_creative_daily_etl(airflow_execution_date, run_id):
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

    # daily report
    jd_sem_creative_daily_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/jd/sem_daily_Report"
        f"/jd_sem_creative_{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    origin_jd_sem_creative_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{jd_sem_creative_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    )

    origin_jd_sem_creative_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.ad.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdkc_creative_daily"
    )

    retrieval_types = [
        col
        for col in origin_jd_sem_creative_daily_df.columns
        if col.startswith("retrievalType")
    ]
    spark.sql(
        f"""
            SELECT
                date,
                req_pin,
                req_clickOrOrderDay,
                campaignId,
                campaignName,
                adGroupId,
                adGroupName,
                adId,
                adName,
                stack({len(retrieval_types)},
                    {", ".join([f"'{col[-1]}', `{col}`" for col in retrieval_types])}
                    ) AS (`source`, `retrievalType`),
                deliveryVersion,
                mobileType,
                status,
                req_businessType,
                req_giftFlag,
                req_orderStatusCategory,
                req_clickOrOrderCaliber,
                req_impressionOrClickEffect,
                req_startDay,
                req_endDay,
                req_isDaily,
                data_source,
                dw_batch_id
            FROM stg.jdkc_creative_daily
            """
    ).select(
        "*",
        json_tuple(
            "retrievalType",
            "cost",
            "clicks",
            "impressions",
            "CPA",
            "CPC",
            "CPM",
            "CTR",
            "totalOrderROI",
            "totalOrderCVS",
            "directCartCnt",
            "indirectCartCnt",
            "totalCartCnt",
            "directOrderSum",
            "indirectOrderSum",
            "totalOrderSum",
            "directOrderCnt",
            "indirectOrderCnt",
            "totalOrderCnt",
            "goodsAttentionCnt",
            "shopAttentionCnt",
            "couponCnt",
            "preorderCnt",
            "depthPassengerCnt",
            "newCustomersCnt",
            "visitTimeAverage",
            "visitorCnt",
            "presaleDirectOrderCnt",
            "presaleIndirectOrderCnt",
            "totalPresaleOrderCnt",
            "presaleDirectOrderSum",
            "presaleIndirectOrderSum",
            "totalPresaleOrderSum",
            "adVisitorCntForInternalSummary",
            "departmentCnt",
            "platformCnt",
            "platformGmv",
            "departmentGmv",
            "channelROI",
        ).alias(
            "cost",
            "clicks",
            "impressions",
            "CPA",
            "CPC",
            "CPM",
            "CTR",
            "totalOrderROI",
            "totalOrderCVS",
            "directCartCnt",
            "indirectCartCnt",
            "totalCartCnt",
            "directOrderSum",
            "indirectOrderSum",
            "totalOrderSum",
            "directOrderCnt",
            "indirectOrderCnt",
            "totalOrderCnt",
            "goodsAttentionCnt",
            "shopAttentionCnt",
            "couponCnt",
            "preorderCnt",
            "depthPassengerCnt",
            "newCustomersCnt",
            "visitTimeAverage",
            "visitorCnt",
            "presaleDirectOrderCnt",
            "presaleIndirectOrderCnt",
            "totalPresaleOrderCnt",
            "presaleDirectOrderSum",
            "presaleIndirectOrderSum",
            "totalPresaleOrderSum",
            "adVisitorCntForInternalSummary",
            "departmentCnt",
            "platformCnt",
            "platformGmv",
            "departmentGmv",
            "channelROI",
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
            cast(req_pin as string) as pin_name,
            cast(req_clickOrOrderDay as string) as effect,
            case
                when req_clickOrOrderDay = 7 then '8'
                when req_clickOrOrderDay = 15 then '24'
                else cast(req_clickOrOrderDay as string)
            end as effect_days,
            cast(campaignId as string) as campaign_id,
            cast(campaignName as string) as campaign_name,
            cast(adGroupId as string) as adgroup_id,
            cast(adGroupName as string) as adgroup_name,
            cast(adId as string) as ad_id,
            cast(adName as string) as ad_name,
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
            cast(visitTimeAverage as decimal(20, 4)) as visit_time_length,
            cast(visitorCnt as bigint) as visitor_quantity,
            cast(presaleDirectOrderCnt as bigint) as presale_direct_order_cnt,
            cast(presaleIndirectOrderCnt as bigint) as presale_indirect_order_cnt,
            cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
            cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum,
            cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum,
            cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,
            cast(adVisitorCntForInternalSummary as string) as advisitor_cnt_for_internal_summary,
            cast(departmentCnt as string) as department_cnt,
            cast(platformCnt as string) as platform_cnt,
            cast(platformGmv as string) as platform_gmv,
            cast(departmentGmv as string) as department_gmv,
            cast(channelROI as string) as channel_roi,
            cast(deliveryVersion as string) as delivery_version,
            cast(mobileType as string) as mobile_type,
            cast(source as string) as source,
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
        from stack_retrivialType
        """
    ).distinct().withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdkc_creative_daily"
    )

    jd_kc_creative_daily_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "ad_id",
        "delivery_version",
        "mobile_type",
        "source",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]

    jdkc_creative_df = (
        spark.table("ods.jdkc_creative_daily").drop("dw_etl_date").drop("data_source")
    )
    jdkc_creative_fail_df = (
        spark.table("dwd.jdkc_creative_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jdkc_creative_daily_mapping_success,
        jdkc_creative_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark, jdkc_creative_df.union(jdkc_creative_fail_df), "sem"
    )

    jdkc_creative_daily_mapping_success.dropDuplicates(
        jd_kc_creative_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdkc_creative_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in jd_kc_creative_daily_pks]
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

    jdkc_creative_daily_mapping_fail.dropDuplicates(
        jd_kc_creative_daily_pks
    ).write.mode("overwrite").option("mergeSchema", "true").insertInto(
        "dwd.jdkc_creative_daily_mapping_fail"
    )

    spark.table("dwd.jdkc_creative_daily_mapping_success").union(
        spark.table("dwd.jdkc_creative_daily_mapping_fail")
    ).createOrReplaceTempView("jdkc_creative_daily")

    jdkc_creative_daily_res = spark.sql(
        """
        select
            a.*,
            '' as mdm_productline_id,
            c.category2_code as emedia_category_id,
            c.brand_code as emedia_brand_id
        from jdkc_creative_daily a 
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code and
            a.category_id = c.emedia_category_code
    """
    )

    jdkc_creative_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "emedia_category_id as category_id",
        "emedia_brand_id as brand_id",
        "campaign_id",
        "campaign_name",
        "adgroup_id",
        "adgroup_name",
        "ad_id",
        "ad_name",
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
        "visit_time_length",
        "visitor_quantity",
        "presale_direct_order_cnt",
        "presale_indirect_order_cnt",
        "total_presale_order_cnt",
        "presale_direct_order_sum",
        "presale_indirect_order_sum",
        "total_presale_order_sum",
        "advisitor_cnt_for_internal_summary",
        "department_cnt",
        "platform_cnt",
        "platform_gmv",
        "department_gmv",
        "channel_roi",
        "delivery_version",
        "mobile_type",
        "source",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.jdkc_creative_daily' as data_source",
        "dw_batch_id",
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdkc_creative_daily"
    )


def push_to_dw():
    project_name = "emedia"
    table_name = "jdkc_creative_daily"
    emedia_conf_dict = get_emedia_conf_dict()
    user = "pgadmin"
    password = "93xx5Px1bkVuHgOo"
    synapseaccountname = emedia_conf_dict.get("synapseaccountname")
    synapsedirpath = emedia_conf_dict.get("synapsedirpath")
    synapsekey = emedia_conf_dict.get("synapsekey")
    url = (
        "jdbc:sqlserver://b2bmptbiqa0101.database.chinacloudapi.cn:1433;"
        "database=B2B-qa-MPT-DW-01;encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;"
    )

    # b2bmptbiqa0101.database.chinacloudapi.cn
    # pgadmin    93xx5Px1bkVuHgOo
    # 获取key
    blob_key = "fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(
        synapseaccountname
    )
    # 获取然后拼接 blob 临时路径
    temp_dir = r"{0}/{1}/{2}/".format(synapsedirpath, project_name, table_name)
    # 将key配置到环境中
    spark.conf.set(blob_key, synapsekey)

    spark.table("dwd.jdkc_creative_daily").distinct().write.mode("overwrite").format(
        "com.databricks.spark.sqldw"
    ).option("url", url).option("user", user).option("password", password).option(
        "forwardSparkAzureStorageCredentials", "true"
    ).option(
        "dbTable", "dbo.tb_emedia_jd_kc_creative_daily_v202209_fact"
    ).option(
        "tempDir", temp_dir
    ).save()

    # dwd.tb_media_emedia_jdkc_daily_fact

    # stg.jdkc_adgroup_daily
    # ods.jdkc_adgroup_daily
    # dwd.jdkc_adgroup_daily

    spark.sql("optimize dwd.jdkc_creative_daily_mapping_success")

    # create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0
