# coding: utf-8

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, lit

from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_status, push_to_dw
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_zw_creative_etl(airflow_execution_date, run_id):
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

    jd_dmp_creative_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/dmp_daily_Report/jd_dmp_creative_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_dmp_creative_path: " + jd_dmp_creative_path)

    jd_dmp_creative_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_dmp_creative_path}",
        header=True,
        multiLine=True,
        sep="|",
    )

    (
        jd_dmp_creative_daily_df.withColumn(
            "data_source", lit("jingdong.ads.ibg.UniversalJosService.ad.query")
        )
        .withColumn("dw_batch_id", lit(run_id))
        .withColumn("dw_etl_date", current_date())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("stg.jdzw_creative_daily")
        .insertInto("stg.jdzw_creative_daily")
    )

    (
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
          cast(visitPageCnt as bigint) as visit_page_quantity,
          cast(presaleDirectOrderCnt as bigint) as presale_direct_order_cnt, 
          cast(presaleIndirectOrderCnt as bigint) as presale_indirect_order_cnt,
          cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
          cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum,
          cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum,
          cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,
          cast(followCnt as string) as follow_cnt,
          cast(likeCnt as string) as like_cnt,
          cast(interactCnt as string) as interact_cnt,
          cast(commentCnt as string) as comment_cnt,
          cast(pullCustomerCnt as string) as pull_customer_cnt,
          cast(pullCustomerCPC as string) as pull_customer_cpc,
          cast(shareCnt as string) as share_cnt,
          cast(IR as decimal(20, 4)) as ir,
          to_date(cast(clickDate as string), 'yyyyMMdd') as click_date,
          cast(status as string) as status,
          cast(campaignType as string) as campaign_type,
          cast(mobileType as string) as mobile_type,
          cast(req_businessType as string) as business_type,
          cast(req_giftFlag as string) as gift_flag,
          cast(req_orderStatusCategory as string) as order_status_category,
          cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
          cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
          cast(req_startDay as date) as start_day,
          cast(req_endDay as date) as end_day,
          cast(req_isDaily as string) as is_daily,
          'stg.jdzw_creative_daily' as data_source,
          cast(dw_batch_id as string) as dw_batch_id
        from stg.jdzw_creative_daily
        """
        )
        .distinct()
        .withColumn("dw_etl_date", current_date())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("ods.jdzw_creative_daily")
        .insertInto("ods.jdzw_creative_daily")
    )

    jdzw_creative_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "ad_id",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]

    jdzw_creative_df = (
        spark.table("ods.jdzw_creative_daily").drop("dw_etl_date").drop("data_source")
    )
    jdzw_creative_fail_df = (
        spark.table("dwd.jdzw_creative_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )
    (
        jdzw_creative_daily_mapping_success,
        jdzw_creative_daily_mapping_fail,
    ) = emedia_brand_mapping(
        #    spark, jdzw_creative_df, "dmp"
        spark,
        jdzw_creative_df.union(jdzw_creative_fail_df),
        "dmp",
    )
    (
        jdzw_creative_daily_mapping_success.dropDuplicates(jdzw_creative_pks)
        # .write.mode("overwrite")
        # .saveAsTable("dwd.jdzw_creative_daily_mapping_success")
        .createOrReplaceTempView("all_mapping_success")
    )

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdzw_creative_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in jdzw_creative_pks]
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

    (
        jdzw_creative_daily_mapping_fail.dropDuplicates(jdzw_creative_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.jdzw_creative_daily_mapping_fail")
        # .saveAsTable("dwd.jdzw_creative_daily_mapping_fail")
    )

    spark.table("dwd.jdzw_creative_daily_mapping_success").union(
        spark.table("dwd.jdzw_creative_daily_mapping_fail")
    ).createOrReplaceTempView("jdzw_creative_daily")

    spark.sql(
        """
        select
            a.adgroup_name,
            a.brand_id,
            c.audience_name as audience_name1,
            c.id as id1,
            d.audience_name as audience_name2,
            d.id as id2
        from (
            select
                adgroup_name,
                brand_id
            from jdzw_creative_daily
            group by adgroup_name, brand_id
            ) a
        left join (
            select
                cate_code,
                audience_keyword,
                id,
                audience_name
            from stg.emedia_sem_audience_mapping
            where cate_code is not null
                and platform = 'jd'
            ) c
        on c.cate_code <=> a.brand_id
            and instr(a.adgroup_name, c.audience_keyword) > 0
        left join (
            select
                audience_keyword,
                id,
                audience_name
            from stg.emedia_sem_audience_mapping
            where cate_code is null
                and platform = 'jd') d
        on instr(a.adgroup_name, d.audience_keyword) > 0
        """
    ).createOrReplaceTempView("audience_name_full_mapping")

    spark.sql(
        """
        select
            t1.adgroup_name,
            t1.brand_id,
            ifnull(ifnull(t1.audience_name1, t2.audience_name2), 'JD Others') as audience_name
        from (
            select
                a.adgroup_name,
                a.brand_id,
                a.audience_name1
            from audience_name_full_mapping a
            left join (
                select
                    adgroup_name,
                    brand_id,
                    max(id1) as max_id
                from audience_name_full_mapping
                group by adgroup_name, brand_id
                ) b
            on a.adgroup_name = b.adgroup_name
                and a.brand_id <=> b.brand_id
                and a.id1 = b.max_id
            ) t1
        join (
            select
                a.adgroup_name,
                a.brand_id,
                a.audience_name2
            from audience_name_full_mapping a
            left join (
                select
                    adgroup_name,
                    brand_id,
                    max(id2) as max_id
                from audience_name_full_mapping
                group by adgroup_name, brand_id
                ) b
            on a.adgroup_name = b.adgroup_name
                and a.brand_id <=> b.brand_id
            and a.id2 = b.max_id
            ) t2
        on t1.adgroup_name = t2.adgroup_name
            and t1.brand_id <=> t2.brand_id
      """
    ).createOrReplaceTempView("audience_name_mapping")

    jdzw_creative_daily_res = spark.sql(
        """
        select
            a.*,
            '' as mdm_productline_id,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            b.category2_code as mdm_category_id,
            b.brand_code as mdm_brand_id,
            c.audience_name
        from jdzw_creative_daily a
        left join ods.media_category_brand_mapping b
          on a.brand_id = b.emedia_brand_code and
          a.category_id = b.emedia_category_code
        left join audience_name_mapping c
        on a.adgroup_name = c.adgroup_name
          and a.brand_id <=> c.brand_id
        """
    )

    (
        jdzw_creative_daily_res.selectExpr(
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
            "adgroup_id",
            "adgroup_name",
            "ad_id",
            "ad_name",
            "'' as sku_id",
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
            "visit_page_quantity",
            "presale_direct_order_cnt",
            "presale_indirect_order_cnt",
            "total_presale_order_cnt",
            "presale_direct_order_sum",
            "presale_indirect_order_sum",
            "total_presale_order_sum",
            "follow_cnt",
            "like_cnt",
            "interact_cnt",
            "comment_cnt",
            "pull_customer_cnt",
            "pull_customer_cpc",
            "share_cnt",
            "ir",
            "click_date",
            "status",
            "campaign_type",
            "mobile_type",
            "business_type",
            "gift_flag",
            "order_status_category",
            "click_or_order_caliber",
            "impression_or_click_effect",
            "start_day",
            "end_day",
            "is_daily",
            "'ods.jdzw_creative_daily' as data_source",
            "dw_batch_id",
            "audience_name",
        )
        .distinct()
        .withColumn("dw_etl_date", current_date())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.jdzw_creative_daily")
        .insertInto("dwd.jdzw_creative_daily")
    )

    push_to_dw(
        spark.table("dwd.jdzw_creative_daily"),
        "dbo.tb_emedia_jd_zw_creative_daily_v202209_fact",
        "overwrite",
        "jdzw_creative_daily",
    )

    file_name = "dmp/EMEDIA_JD_DMP_DAILY_CREATIVE_REPORT_FACT.CSV"
    job_name = "tb_emedia_jd_dmp_creative_new_fact"
    push_status(airflow_execution_date, file_name, job_name)

    # dwd.tb_media_emedia_jdzw_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_jdzw_daily_fact
        where `report_level` = 'creative' 
        """
    )

    tables = [
        "dwd.jdzw_creative_daily",
        "dwd.jdzw_creative_daily_old",
        "dwd.jdzw_creative_daily_old_v2",
    ]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("jdzw_creative_daily")

    jd_zw_daily_fact_pks = [
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "report_level",
        "report_level_id",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]
    spark.sql(
        """
        SELECT
            ad_date,
            '品牌聚效' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            adgroup_id,
            adgroup_name,
            'creative' as report_level,
            ad_id as report_level_id,
            ad_name as report_level_name,
            audience_name,
            sku_id as sku_id,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            mobile_type,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            impression_or_click_effect,
            '' as put_type,
            '' as campaign_type,
            '' as campaign_put_type,
            round(nvl(cost, 0), 4) as cost,
            nvl(clicks, 0) as click,
            nvl(impressions, 0) as impression,
            nvl(order_quantity, 0) as order_quantity,
            round(nvl(order_value, 0), 4) as order_value,
            nvl(total_cart_quantity, 0) as total_cart_quantity,
            nvl(new_customer_quantity, 0) as new_customer_quantity,
            data_source as dw_source,
            dw_etl_date as dw_create_time,
            dw_batch_id as dw_batch_number,
            etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        FROM 
            jdzw_creative_daily
        """
    ).dropDuplicates(jd_zw_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_jdzw_daily_fact"
    )

    return 0


def jdzw_creative_daily_old_stg_etl():
    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")
    url = server_name + ";" + "databaseName=" + database_name + ";"
    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_jd_dmp_creative_new_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.jdzw_creative_daily_old")
    )


def jdzw_creative_daily_old_v2_stg_etl():
    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")
    url = server_name + ";" + "databaseName=" + database_name + ";"
    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_jd_dmp_creative_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.jdzw_creative_daily_old_v2")
    )


def jdzw_creative_daily_old_dwd_etl():
    jdzw_creative_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "ad_id",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]
    (
        spark.sql(
            """
            select
              cast(ad_date as DATE) as ad_date,
              cast(pin_name as STRING) as pin_name,
              case
                when effect_days = 8 then '7'
                when effect_days = 24 then '15'
                else cast(effect_days as string)
              end as effect,
              cast(effect_days as STRING) as effect_days,
              cast(category_id as STRING) as emedia_category_id,
              cast(brand_id as STRING) as emedia_brand_id,
              cast(mdm_category_id as STRING) as mdm_category_id,
              cast(mdm_brand_id as STRING) as mdm_brand_id,
              cast(campaign_id as STRING) as campaign_id,
              cast(campaign_name as STRING) as campaign_name,
              cast(adgroup_id as STRING) as adgroup_id,
              cast(adgroup_name as STRING) as adgroup_name,
              cast(creative_id as STRING) as ad_id,
              cast(creative_name as STRING) as ad_name,
              cast(sku_id as string) as sku_id,
              cast(cost as DECIMAL(20,4)) as cost,
              cast(clicks as BIGINT) as clicks,
              cast(impressions as BIGINT) as impressions,
              '' as cpa,
              cast(cpc as DECIMAL(20,4)) as cpc,
              cast(cpm as DECIMAL(20,4)) as cpm,
              cast(null as DECIMAL(9,4)) as ctr,
              cast(total_order_roi as DECIMAL(9,4)) as total_order_roi,
              cast(total_order_cvs as DECIMAL(9,4)) as total_order_cvs,
              cast(direct_cart_quantity as BIGINT) as direct_cart_cnt,
              cast(indirect_cart_quantity as BIGINT) as indirect_cart_cnt,
              cast(total_cart_quantity as BIGINT) as total_cart_quantity,
              cast(direct_order_value as DECIMAL(20,4)) as direct_order_value,
              cast(indirect_order_value as DECIMAL(20,4)) as indirect_order_value,
              cast(order_value as DECIMAL(20,4)) as order_value,
              cast(direct_order_quantity as BIGINT) as direct_order_quantity,
              cast(indirect_order_quantity as BIGINT) as indirect_order_quantity,
              cast(order_quantity as BIGINT) as order_quantity,
              cast(favorite_item_quantity as BIGINT) as favorite_item_quantity,
              cast(favorite_shop_quantity as BIGINT) as favorite_shop_quantity,
              cast(coupon_quantity as BIGINT) as coupon_quantity,
              cast(preorder_quantity as BIGINT) as preorder_quantity,
              cast(depth_passenger_quantity as BIGINT) as depth_passenger_quantity,
              cast(new_customer_quantity as BIGINT) as new_customer_quantity,
              cast(visit_time_length as DECIMAL(20,4)) as visit_time_length,
              cast(visitor_quantity as BIGINT) as visitor_quantity,
              cast(visit_page_quantity as BIGINT) as visit_page_quantity,
              cast(null as BIGINT) as presale_direct_order_cnt,
              cast(null as BIGINT) as presale_indirect_order_cnt,
              cast(null as BIGINT) as total_presale_order_cnt,
              cast(null as DECIMAL(20,4)) as presale_direct_order_sum,
              cast(null as DECIMAL(20,4)) as presale_indirect_order_sum,
              cast(null as DECIMAL(20,4)) as total_presale_order_sum,
              '' as follow_cnt,
              '' as like_cnt,
              '' as interact_cnt,
              '' as comment_cnt,
              '' as pull_customer_cnt,
              '' as pull_customer_cpc,
              '' as share_cnt,
              cast(null as DECIMAL(20,4)) as ir,
              cast(null as DATE) as click_date,
              '' as status,
              '' as campaign_type,
              cast(mobiletype as STRING) as mobile_type,
              '' as business_type,
              '' as gift_flag,
              cast(req_orderstatuscategory as STRING) as order_status_category,
              cast(req_clickOrOrderCaliber as STRING) as click_or_order_caliber,
              cast(req_impressionOrClickEffect as STRING) as impression_or_click_effect,
              cast(req_startDay as DATE) as start_day,
              cast(req_endDay as DATE) as end_day,
              cast(req_isDaily as STRING) as is_daily,
              cast(data_source as STRING) as data_source,
              cast(dw_batch_id as STRING) as dw_batch_id,
              cast(audience_name as STRING) as audience_name,
              cast(dw_etl_date as DATE) as dw_etl_date
          from stg.jdzw_creative_daily_old
          where ad_date >= '2022-03-01'
          """
        )
        .dropDuplicates(jdzw_creative_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .insertInto("dwd.jdzw_creative_daily_old")
        .saveAsTable("dwd.jdzw_creative_daily_old")
    )


def jdzw_creative_daily_old_v2_dwd_etl():
    jdzw_creative_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "ad_id",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]
    (
        spark.sql(
            """
            select
              cast(ad_date as DATE) as ad_date,
              cast(pin_name as STRING) as pin_name,
              case
                when effect_days = 8 then '7'
                when effect_days = 24 then '15'
                else cast(effect_days as string)
              end as effect,
              cast(effect_days as STRING) as effect_days,
              cast(category_id as STRING) as emedia_category_id,
              cast(brand_id as STRING) as emedia_brand_id,
              cast(c.category2_code as STRING) as mdm_category_id,
              cast(c.brand_code as STRING) as mdm_brand_id,
              cast(campaign_id as STRING) as campaign_id,
              cast(campaign_name as STRING) as campaign_name,
              cast(adgroup_id as STRING) as adgroup_id,
              cast(adgroup_name as STRING) as adgroup_name,
              cast(creative_id as STRING) as ad_id,
              cast(creative_name as STRING) as ad_name,
              cast(sku_id as string) as sku_id,
              cast(cost as DECIMAL(20,4)) as cost,
              cast(clicks as BIGINT) as clicks,
              cast(impressions as BIGINT) as impressions,
              '' as cpa,
              cast(null as DECIMAL(20,4)) as cpc,
              cast(null as DECIMAL(20,4)) as cpm,
              cast(null as DECIMAL(9,4)) as ctr,
              cast(null as DECIMAL(9,4)) as total_order_roi,
              cast(null as DECIMAL(9,4)) as total_order_cvs,
              cast(null as BIGINT) as direct_cart_cnt,
              cast(null as BIGINT) as indirect_cart_cnt,
              cast(total_cart_quantity as BIGINT) as total_cart_quantity,
              cast(direct_order_value as DECIMAL(20,4)) as direct_order_value,
              cast(indirect_order_value as DECIMAL(20,4)) as indirect_order_value,
              cast(order_value as DECIMAL(20,4)) as order_value,
              cast(direct_order_quantity as BIGINT) as direct_order_quantity,
              cast(indirect_order_quantity as BIGINT) as indirect_order_quantity,
              cast(order_quantity as BIGINT) as order_quantity,
              cast(favorite_item_quantity as BIGINT) as favorite_item_quantity,
              cast(favorite_shop_quantity as BIGINT) as favorite_shop_quantity,
              cast(coupon_quantity as BIGINT) as coupon_quantity,
              cast(preorder_quantity as BIGINT) as preorder_quantity,
              cast(depth_passenger_quantity as BIGINT) as depth_passenger_quantity,
              cast(new_customer_quantity as BIGINT) as new_customer_quantity,
              cast(visit_time_length as DECIMAL(20,4)) as visit_time_length,
              cast(visitor_quantity as BIGINT) as visitor_quantity,
              cast(visit_page_quantity as BIGINT) as visit_page_quantity,
              cast(null as BIGINT) as presale_direct_order_cnt,
              cast(null as BIGINT) as presale_indirect_order_cnt,
              cast(null as BIGINT) as total_presale_order_cnt,
              cast(null as DECIMAL(20,4)) as presale_direct_order_sum,
              cast(null as DECIMAL(20,4)) as presale_indirect_order_sum,
              cast(null as DECIMAL(20,4)) as total_presale_order_sum,
              '' as follow_cnt,
              '' as like_cnt,
              '' as interact_cnt,
              '' as comment_cnt,
              '' as pull_customer_cnt,
              '' as pull_customer_cpc,
              '' as share_cnt,
              cast(null as DECIMAL(20,4)) as ir,
              cast(null as DATE) as click_date,
              '' as status,
              '' as campaign_type,
              cast(mobiletype as STRING) as mobile_type,
              '' as business_type,
              '' as gift_flag,
              cast(req_orderstatuscategory as STRING) as order_status_category,
              '' as click_or_order_caliber,
              '' as impression_or_click_effect,
              cast(null as DATE) as start_day,
              cast(null as DATE) as end_day,
              '' as is_daily,
              cast(data_source as STRING) as data_source,
              cast(dw_batch_id as STRING) as dw_batch_id,
              cast(audience_name as STRING) as audience_name,
              cast(dw_etl_date as DATE) as dw_etl_date
            from stg.jdzw_creative_daily_old_v2 a
            left join ods.media_category_brand_mapping c
              on a.brand_id = c.emedia_brand_code and
              a.category_id = c.emedia_category_code
            where a.ad_date < '2022-03-01'
          """
        )
        .dropDuplicates(jdzw_creative_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .insertInto("dwd.jdzw_creative_daily_old_v2")
        .saveAsTable("dwd.jdzw_creative_daily_old_v2")
    )
