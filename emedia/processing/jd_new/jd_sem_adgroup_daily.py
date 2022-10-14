# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, json_tuple, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia

spark = get_spark()


def jdkc_adgroup_daily_etl(airflow_execution_date, run_id):
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
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
    jd_sem_adgroup_daily_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/jd/sem_daily_Report"
        f"/jd_sem_adgroup_{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    origin_jd_sem_adgroup_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{jd_sem_adgroup_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    )

    origin_jd_sem_adgroup_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.group.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdkc_adgroup_daily"
    )

    retrieval_types = [
        col
        for col in origin_jd_sem_adgroup_daily_df.columns
        if col.startswith("retrievalType")
    ]
    spark.sql(
        f"""
        SELECT
            date,
            pin,
            req_clickOrOrderDay,
            campaignId,
            campaignName,
            adGroupId,
            adGroupName,
            stack({len(retrieval_types)},
                {", ".join([f"'{col[-1]}', `{col}`" for col in retrieval_types])}
                ) AS (`source`, `retrievalType`),
            mobileType,
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
        FROM stg.jdkc_adgroup_daily
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
            "visitPageCnt",
            "visitTimeAverage",
            "visitorCnt",
            "presaleDirectOrderCnt",
            "presaleIndirectOrderCnt",
            "totalPresaleOrderCnt",
            "presaleDirectOrderSum",
            "presaleIndirectOrderSum",
            "totalPresaleOrderSum",
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
            "visitPageCnt",
            "visitTimeAverage",
            "visitorCnt",
            "presaleDirectOrderCnt",
            "presaleIndirectOrderCnt",
            "totalPresaleOrderCnt",
            "presaleDirectOrderSum",
            "presaleIndirectOrderSum",
            "totalPresaleOrderSum",
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
            cast(pin as string) as pin_name,
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
            cast(mobileType as string) as mobile_type,
            cast(req_businessType as string) as business_type,
            cast(req_giftFlag as string) as gift_flag,
            cast(req_orderStatusCategory as string) as order_status_category,
            cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
            cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
            cast(source as string) as source,
            cast(req_startDay as date) as start_day,
            cast(req_endDay as date) as end_day,
            cast(req_isDaily as string) as is_daily,
            data_source,
            cast(dw_batch_id as string) as dw_batch_id
        from stack_retrivialType
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdkc_adgroup_daily"
    )

    jd_kc_adgroup_daily_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "source"
    ]

    jdkc_adgroup_df = (
        spark.table("ods.jdkc_adgroup_daily").drop("dw_etl_date")
    )
    jdkc_adgroup_fail_df = (
        spark.table("dwd.jdkc_adgroup_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jdkc_adgroup_daily_mapping_success,
        jdkc_adgroup_daily_mapping_fail,
    ) = emedia_brand_mapping(spark, jdkc_adgroup_df.union(jdkc_adgroup_fail_df), "sem")

    jdkc_adgroup_daily_mapping_success.dropDuplicates(
        jd_kc_adgroup_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdkc_adgroup_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in jd_kc_adgroup_daily_pks]
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

    jdkc_adgroup_daily_mapping_fail.dropDuplicates(jd_kc_adgroup_daily_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.jdkc_adgroup_daily_mapping_fail")

    spark.table("dwd.jdkc_adgroup_daily_mapping_success").union(
        spark.table("dwd.jdkc_adgroup_daily_mapping_fail")
    ).createOrReplaceTempView("jdkc_adgroup_daily")

    jdkc_adgroup_daily_res = spark.sql(
        """
        select
            a.*,
            '' as mdm_productline_id,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            c.category2_code as mdm_category_id,
            c.brand_code as mdm_brand_id
        from jdkc_adgroup_daily a 
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code and
            a.category_id = c.emedia_category_code
    """
    )

    jdkc_adgroup_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "mdm_productline_id",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
        "campaign_id",
        "campaign_name",
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
        "presale_indirect_order_cnt",
        "total_presale_order_cnt",
        "presale_direct_order_sum",
        "presale_indirect_order_sum",
        "total_presale_order_sum",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "source",
        "start_day",
        "end_day",
        "is_daily",
        "data_source as dw_source",
        "'ods.jdkc_adgroup_daily' as etl_source_table",
        "dw_batch_id"
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdkc_adgroup_daily"
    )


    push_to_dw(spark.table("dwd.jdkc_adgroup_daily"), 'dbo.tb_emedia_jd_kc_adgroup_daily_v202209_fact', 'overwrite',
               'jdkc_adgroup_daily')

    file_name = 'sem/EMEDIA_JD_SEM_DAILY_ADGROUP_REPORT_FACT.CSV'
    job_name = 'tb_emedia_jd_sem_daily_adgroup_report_fact'
    push_status(airflow_execution_date, file_name, job_name)


    eab_db = spark.sql(f"""
            select  	ad_date,
                        pin_name as pin_name,
                        campaign_id as campaign_id,
                        campaign_name as campaign_name,
                        adgroup_id as adgroup_id,
                        adgroup_name as adgroup_name,
                        '' as category_id,
                        '' as brand_id,
                        is_daily as req_isdaily,
                        0 as req_isorderorclick,
                        effect as req_istodayor15days,
                        effect_days as effect_days,
                        order_status_category as req_orderstatuscategory,
                        mobile_type as mobiletype,
                        clicks as clicks,
                        cost as cost,
                        direct_order_quantity as direct_order_quantity,
                        direct_order_value as direct_order_value,
                        impressions as impressions,
                        indirect_order_quantity as indirect_order_quantity,
                        indirect_order_value as indirect_order_value,
                        total_cart_quantity as total_cart_quantity,
                        total_order_roi as total_order_roi,
                        total_order_cvs as total_order_cvs,
                        indirect_cart_cnt as indirect_cart_cnt,
                        direct_cart_cnt as direct_cart_cnt,
                        cpc as cpc,
                        cpa as cpa,
                        ctr as ctr,
                        cpm as cpm,
                        order_quantity as order_quantity,
                        order_value as order_value,
                        depth_passenger_quantity as depth_passenger_quantity,
                        visit_time_length as visit_time_length,
                        visit_page_quantity as visit_page_quantity,
                        new_customer_quantity as new_customer_quantity,
                        favorite_item_quantity as favorite_item_quantity,
                        favorite_shop_quantity as favorite_shop_quantity,
                        preorder_quantity as preorder_quantity,
                        coupon_quantity as coupon_quantity,
                        visitor_quantity as visitor_quantity,
                        data_source as data_source,
                        dw_etl_date as dw_etl_date,
                        dw_batch_id as dw_batch_id,
                        source as source,
                        effect as effect
                        ,presale_direct_order_cnt,presale_indirect_order_cnt,total_presale_order_cnt,presale_direct_order_sum,presale_indirect_order_sum,total_presale_order_sum
            from    ods.jdkc_adgroup_daily
        """)

    date = airflow_execution_date[0:10]
    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_adgroup_day-{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    spark.sql("optimize dwd.jdkc_adgroup_daily_mapping_success")

    return 0

