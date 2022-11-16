# coding: utf-8

import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import current_date, lit
from pyspark.sql.types import *

from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_status, push_to_dw
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia

spark = get_spark()


def jdkc_target_daily_etl(airflow_execution_date, run_id):
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
    jd_sem_target_daily_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/jd/sem_daily_targetreport"
        f"/jd_sem_targetReport_{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{jd_sem_target_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdkc_target_daily"
    )

    spark.sql(
        """
        select
            cast(t1.`req_startDay` as date) as ad_date,
            cast(t1.req_pin as string) as pin_name,
            cast(t1.req_clickOrOrderDay as string) as effect,
            case
                when t1.req_clickOrOrderDay = 7 then '8'
                when t1.req_clickOrOrderDay = 15 then '24'
                else cast(t1.req_clickOrOrderDay as string)
            end as effect_days,
            cast(t1.campaignId as string) as campaign_id,
            cast(t2.campaign_name as string) as campaign_name,
            cast(t1.groupId as string) as adgroup_id,
            cast(t3.adgroup_name as string) as adgroup_name,
            cast(t1.dmpId as string) as target_audience_id,
            cast(t1.dmpName as string) as target_audience_name,
            cast(t1.dmpFactor as bigint) as dmpFactor,
            cast(t1.dmpStatus as boolean) as dmpStatus,
            cast(t1.cost as decimal(20, 4)) as cost,
            cast(t1.clicks as bigint) as clicks,
            cast(t1.impressions as bigint) as impressions,
            cast(t1.CPC as decimal(20, 4)) as cpc,
            cast(t1.CPM as decimal(20, 4)) as cpm,
            cast(t1.CTR as decimal(9, 4)) as ctr,
            cast(t1.totalOrderROI as decimal(9, 4)) as total_order_roi,
            cast(t1.totalOrderCVS as decimal(9, 4)) as total_order_cvs,
            cast(t1.directCartCnt as bigint) as direct_cart_cnt,
            cast(t1.indirectCartCnt as bigint) as indirect_cart_cnt,
            cast(t1.totalCartCnt as bigint) as total_cart_quantity,
            cast(t1.directOrderSum as decimal(20, 4)) as direct_order_value,
            cast(t1.indirectOrderSum as decimal(20, 4)) as indirect_order_value,
            cast(t1.totalOrderSum as decimal(20, 4)) as order_value,
            cast(t1.directOrderCnt as bigint) as direct_order_quantity,
            cast(t1.indirectOrderCnt as bigint) as indirect_order_quantity,
            cast(t1.totalOrderCnt as bigint) as order_quantity,
            cast(t1.req_giftFlag as string) as gift_flag,
            cast(t1.req_orderStatusCategory as string) as order_status_category,
            cast(t1.req_clickOrOrderCaliber as string) as click_or_order_caliber,
            cast(t1.req_startDay as date) as start_day,
            cast(t1.req_endDay as date) as end_day,
            'stg.jdkc_target_daily' as data_source,
            cast(t1.dw_batch_number as string) as dw_batch_id,
            t1.req_page,
            t1.req_pageSize,
            t1.dw_create_time
        from stg.jdkc_target_daily t1
        left join (
          select distinct(campaign_id), campaign_name
          from dwd.jdkc_adgroup_daily) t2
        on t1.campaignId = t2.campaign_id
        left join (
          select distinct(adgroup_id), adgroup_name
          from dwd.jdkc_adgroup_daily) t3
        on t1.groupId = t3.adgroup_id
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdkc_target_daily"
    )

    jd_kc_target_daily_pks = [
        "ad_date",
        "pin_name",
        "campaign_id",
        "adgroup_id",
        "target_audience_id",
        "effect_days",
    ]

    jdkc_target_df = (
        spark.table("ods.jdkc_target_daily").drop("dw_etl_date").drop("data_source")
    )
    jdkc_target_fail_df = (
        spark.table("dwd.jdkc_target_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jdkc_target_daily_mapping_success,
        jdkc_target_daily_mapping_fail,
    ) = emedia_brand_mapping(
        #    spark, jdkc_target_df, "sem"
        spark,
        jdkc_target_df.union(jdkc_target_fail_df),
        "sem",
    )

    jdkc_target_daily_mapping_success.dropDuplicates(
        jd_kc_target_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdkc_target_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in jd_kc_target_daily_pks]
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

    jdkc_target_daily_mapping_fail.dropDuplicates(jd_kc_target_daily_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").saveAsTable("dwd.jdkc_target_daily_mapping_fail")

    spark.table("dwd.jdkc_target_daily_mapping_success").union(
        spark.table("dwd.jdkc_target_daily_mapping_fail")
    ).createOrReplaceTempView("jdkc_target_daily")

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
            from jdkc_target_daily
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

    jdkc_target_daily_res = spark.sql(
        """
        select
            a.*,
            '' as mdm_productline_id,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            b.category2_code as mdm_category_id,
            b.brand_code as mdm_brand_id,
            case
              when a.campaign_name like '%智能%' then '智能推广'
              else '标准推广'
            end as campaign_subtype,
            c.audience_name
        from jdkc_target_daily a 
        left join ods.media_category_brand_mapping b
          on a.brand_id = b.emedia_brand_code
          and a.category_id = b.emedia_category_code
        left join audience_name_mapping c
        on a.adgroup_name = c.adgroup_name
    """
    )

    jdkc_target_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "campaign_id",
        "campaign_name",
        "adgroup_id",
        "adgroup_name",
        "target_audience_id",
        "target_audience_name",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
        "effect",
        "effect_days",
        "mdm_productline_id",
        "campaign_subtype",
        "audience_name",
        "dmpFactor",
        "dmpStatus",
        "cost",
        "clicks",
        "impressions",
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
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "start_day",
        "end_day",
        "dw_batch_id",
        "req_page",
        "req_pageSize",
        "dw_create_time",
        "'ods.jdkc_target_daily' as data_source",
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).saveAsTable(
        "dwd.jdkc_target_daily"
    )

    spark.sql("delete from dwd.tb_media_emedia_jdkc_daily_fact where report_level = 'target' ")
    keyword_df = spark.sql("""
                select ad_date
                ,'京东快车' as ad_format_lv2
                ,pin_name
                ,effect
                ,effect_days
                ,campaign_id
                ,campaign_name
                ,case when campaign_name like '%智能%' then '智能推广' else '标准推广' end as campaign_subtype
                ,adgroup_id
                ,adgroup_name
                ,'target' as report_level
                ,target_audience_id as report_level_id
                ,target_audience_name as report_level_name
                ,'' as sku_id
                ,'' as keyword_type
                ,'' as niname
                ,emedia_category_id
                ,emedia_brand_id
                ,mdm_category_id
                ,mdm_brand_id
                ,mdm_productline_id
                ,audience_name
                ,'' as delivery_version
                ,'' as delivery_type
                ,'' as mobile_type
                ,'' as source
                ,'' as business_type
                ,'' as gift_flag
                ,order_status_category
                ,click_or_order_caliber
                ,'' as put_type
                ,'' as campaign_put_type
                ,'' as targeting_type
                ,cost
                ,clicks as click
                ,impressions as impression
                ,order_quantity
                ,order_value
                ,total_cart_quantity
                ,'' as new_customer_quantity
                ,'' as dw_source
                ,dw_create_time
                ,dw_batch_id as dw_batch_number
                ,'dwd.jdkc_target_daily' as etl_source_table from dwd.jdkc_target_daily
            """)
    keyword_df = keyword_df.withColumn('new_customer_quantity', keyword_df.new_customer_quantity.cast(IntegerType()))

    keyword_df.withColumn("etl_create_time", F.current_timestamp()) \
        .withColumn("etl_update_time", F.current_timestamp()).distinct() \
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")

