# coding: utf-8

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict

spark = get_spark()


def jdzt_account_daily_etl(airflow_execution_date, run_id):
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

    # jt zt daily adgroup report
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

    (
        origin_jdzt_account_daily_df.withColumn(
            "data_source", lit("jingdong.ads.ibg.UniversalJosService.account.query")
        )
        .withColumn("dw_batch_id", lit(run_id))
        .withColumn("dw_etl_date", current_date())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("stg.jdzt_account_daily")
        .insertInto("stg.jdzt_account_daily")
    )

    (
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
            'stg.jdzt_account_daily' as data_source,
            cast(dw_batch_id as string) as dw_batch_id
        from stg.jdzt_account_daily
        """
        )
        .withColumn("dw_etl_date", current_date())
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("ods.jdzt_account_daily")
        .insertInto("ods.jdzt_account_daily")
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
    jdzc_account_fail_df = (
        spark.table("dwd.jdzt_account_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    daily_reports = jdzt_account_df.union(jdzc_account_fail_df)
    mapping_blob_account = emedia_conf_dict.get("mapping_blob_account")
    mapping_blob_container = emedia_conf_dict.get("mapping_blob_container")
    mapping_sas = emedia_conf_dict.get("mapping_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn",
        mapping_sas,
    )

    # Loading Mapping tbls
    otd_vip_mapping1_path = (
        "hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv"
    )
    otd_vip_mapping2_path = (
        "hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv"
    )
    otd_vip_mapping3_path = (
        "hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv"
    )

    emedia_adformat_mapping_path = (
        "hdi_etl_brand_mapping/emedia_adformat_mapping/emedia_adformat_mapping.csv"
    )

    emedia_adformat_mapping = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core."
        f"chinacloudapi.cn/{emedia_adformat_mapping_path}",
        header=True,
        multiLine=True,
        sep=",",
    )

    match_keyword_column = (
        emedia_adformat_mapping.fillna("req_storeId")
        .filter(emedia_adformat_mapping["adformat_en"] == "jdzt")
        .toPandas()
    )
    account_id = match_keyword_column["match_store_column"][0]

    mapping1_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping1_path}",
        header=True,
        multiLine=True,
        sep="=",
    )
    mapping1_df.createOrReplaceTempView("mapping1")

    mapping2_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping2_path}",
        header=True,
        multiLine=True,
        sep="=",
    )
    mapping2_df.createOrReplaceTempView("mapping2")

    mapping3_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping3_path}",
        header=True,
        multiLine=True,
        sep="=",
    )
    mapping3_df.createOrReplaceTempView("mapping3")

    daily_reports.createOrReplaceTempView("daily_reports")
    # First map result
    mapping1_result_df = spark.sql(
        f"""
        SELECT
            dr.*,
            m1.category_id,
            m1.brand_id
        FROM daily_reports dr
        LEFT JOIN mapping1 m1
            ON dr.{account_id} = m1.account_id
        """
    )
    mapping1_result_df.filter("category_id IS null AND brand_id IS null").drop(
        "category_id"
    ).drop("brand_id").createOrReplaceTempView("mapping_fail_1")
    mapping1_result_df.filter(
        "category_id IS NOT null or brand_id IS NOT null"
    ).createOrReplaceTempView("mapping_success_1")

    # Second map result
    mapping2_result_df = spark.sql(
        f"""
        SELECT
            mfr1.*,
            m2.category_id,
            m2.brand_id
        FROM mapping_fail_1 mfr1
        LEFT JOIN mapping2 m2
            ON mfr1.{account_id} = m2.account_id
        """
    )

    mapping2_result_df.filter("category_id IS null and brand_id IS null").drop(
        "category_id"
    ).drop("brand_id").createOrReplaceTempView("mapping_fail_2")
    mapping2_result_df.filter(
        "category_id IS NOT null or brand_id IS NOT null"
    ).createOrReplaceTempView("mapping_success_2")

    # Third map result
    mapping3_result_df = spark.sql(
        f"""
        SELECT
            mfr2.*,
            m3.category_id,
            m3.brand_id
        FROM mapping_fail_2 mfr2
        LEFT JOIN mapping3 m3
            ON mfr2.{account_id} = m3.account_id
        """
    )
    mapping3_result_df.filter(
        "category_id is null and brand_id is null"
    ).createOrReplaceTempView("mapping_fail_3")
    mapping3_result_df.filter(
        "category_id IS NOT null or brand_id IS NOT null"
    ).createOrReplaceTempView("mapping_success_3")

    (
        spark.table("mapping_success_1")
        .union(spark.table("mapping_success_2"))
        .union(spark.table("mapping_success_3"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .dropDuplicates(jd_zt_account_daily_pks)
        # .write.mode("overwrite")
        # .saveAsTable("dwd.jdzt_account_daily_mapping_success")
        .createOrReplaceTempView("all_mapping_success")
    )

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

    (
        spark.table("mapping_fail_3")
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .dropDuplicates(jd_zt_account_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.jdzt_account_daily_mapping_fail")
        # .saveAsTable("dwd.jdzt_account_daily_mapping_fail")
    )

    spark.table("dwd.jdzt_account_daily_mapping_success").union(
        spark.table("dwd.jdzt_account_daily_mapping_fail")
    ).createOrReplaceTempView("jdzt_account_daily")

    jdzt_account_daily_res = spark.sql(
        """
        select
          a.*,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          c.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from jdzt_account_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code and
          a.category_id = c.emedia_category_code
        """
    )

    jdzt_account_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "pin_id",
        "effect",
        "effect_days",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
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
        "dw_batch_id",
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdzt_account_daily"
    )

    # dwd.tb_media_emedia_jdzt_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_jdzt_daily_fact
        where `report_level` = 'account' 
        """
    )

    tables = [
        "dwd.jdzt_account_daily",
        "dwd.jdzt_account_daily_old_v2",
        "dwd.jdzt_account_daily_old",
    ]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("jdzt_account_daily")

    jd_zt_daily_fact_pks = [
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "campaign_id",
        "mobile_type",
        "media_type",
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
            '京东直投' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            '' as campaign_id,
            '' as campaign_name,
            '' as adgroup_id,
            '' as adgroup_name,
            'account' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id as emedia_category_id,
            emedia_brand_id as emedia_brand_id,
            mdm_category_id as mdm_category_id,
            mdm_brand_id as mdm_brand_id,
            mobile_type,
            '' as media_type,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            impression_or_click_effect,
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
            jdzt_account_daily
        """
    ).dropDuplicates(jd_zt_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_jdzt_daily_fact"
    )

    return 0


def jd_zt_account_daily_old_stg_etl():
    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")

    # server_name = "jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn"
    # database_name = "B2B-prd-MPT-DW-01"
    # username = "etl_user_read"
    # password = "1qaZcde3"
    url = server_name + ";" + "databaseName=" + database_name + ";"
    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_jd_zt_new_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.jdzt_account_daily_old")
    )


def jd_zt_account_daily_old_v2_stg_etl():
    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")

    # server_name = "jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn"
    # database_name = "B2B-prd-MPT-DW-01"
    # username = "etl_user_read"
    # password = "1qaZcde3"
    url = server_name + ";" + "databaseName=" + database_name + ";"
    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_jd_zt_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.jdzt_account_daily_old_v2")
    )


def jd_zt_account_daily_old_dwd_etl():
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
    (
        spark.sql(
            """
            select
                cast(ad_date as date) as ad_date,
                cast(pin_name as string) as pin_name,
                '' as pin_id,
                cast(effect as string) as effect,
                case
                  when effect = 3 then '4'
                  when effect = 7 then '8'
                  when effect = 15 then '24'
                  else cast(cast(effect as int) as string)
                end as effect_days,
                cast(category_id as string) as emedia_category_id,
                cast(brand_id as string) as emedia_brand_id,
                cast(mdm_category_id as string) as mdm_category_id,
                cast(mdm_brand_id as string) as mdm_brand_id,
                cast(cost as decimal(20, 4)) as cost,
                cast(clicks as bigint) as clicks,
                cast(impressions as bigint) as impressions,
                cast(order_cpa as string) as cpa,
                cast(cpc as decimal(20, 4)) as cpc,
                cast(cpm as decimal(20, 4)) as cpm,
                cast(ctr as decimal(9, 4)) as ctr,
                cast(total_order_roi as decimal(9, 4)) as total_order_roi,
                cast(total_order_cvs as decimal(9, 4)) as total_order_cvs,
                cast(null as bigint) as direct_cart_cnt,
                cast(null as bigint) as indirect_cart_cnt,
                cast(total_cart_cnt as bigint) as total_cart_quantity,
                cast(null as decimal(20, 4)) as direct_order_value,
                cast(null as decimal(20, 4)) as indirect_order_value,
                cast(order_sum as decimal(20, 4)) as order_value,
                cast(null as bigint) as direct_order_quantity,
                cast(null as bigint) as indirect_order_quantity,
                cast(order_cnt as bigint) as order_quantity,
                cast(null as bigint) as favorite_item_quantity,
                cast(null as bigint) as favorite_shop_quantity,
                cast(coupon_cnt as bigint) as coupon_quantity,
                cast(preorder_cnt as bigint) as preorder_quantity,
                cast(null as bigint) as depth_passenger_quantity,
                cast(new_customers_cnt as bigint) as new_customer_quantity,
                cast(null as bigint) as visit_page_quantity,
                cast(null as decimal(20, 4)) as visit_time_length,
                cast(null as bigint) as visitor_quantity,
                cast(null as bigint) as presale_direct_order_cnt,
                cast(null as bigint) as presale_indirect_order_cnt,
                cast(total_presale_order_cnt as bigint) as total_presale_order_cnt,
                cast(null as decimal(20, 4)) as presale_direct_order_sum,
                cast(null as decimal(20, 4)) as presale_indirect_order_sum,
                cast(total_presale_order_sum as decimal(20, 4)) as total_presale_order_sum,
                cast(null as date) as click_date,
                cast(platform_cnt as bigint) as platform_cnt,
                cast(platform_gmv as decimal(20, 4)) as platform_gmv,
                cast(department_cnt as bigint) as department_cnt,
                cast(department_gmv as decimal(20, 4)) as department_gmv,
                cast(channel_roi as decimal(20, 4)) as channel_roi,
                '' as mobile_type,
                '' as business_type,
                '' as gift_flag,
                '' as order_status_category,
                cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
                '' as impression_or_click_effect,
                cast(req_startDay as date) as start_day,
                cast(req_endDay as date) as end_day,
                '' as is_daily,
                'stg.jdzt_adgroup_daily_old' as data_source,
                '' as dw_batch_id,
                current_date() as dw_etl_date
            from stg.jdzt_account_daily_old
            """
        )
        .dropDuplicates(jd_zt_account_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .insertInto("dwd.jdzt_account_daily_old")
        .saveAsTable("dwd.jdzt_account_daily_old")
    )


def jd_zt_account_daily_old_v2_dwd_etl():
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
    (
        spark.sql(
            """
            select
                cast(a.ad_date as date) as ad_date,
                cast(a.pin_name as string) as pin_name,
                '' as pin_id,
                cast(a.effect as string) as effect,
                case
                  when a.effect = 7 then '8'
                  when a.effect = 15 then '24'
                  else cast(a.effect as string)
                end as effect_days,
                cast(a.category_id as string) as emedia_category_id,
                cast(a.brand_id as string) as emedia_brand_id,
                cast(c.category2_code as string) as mdm_category_id,
                cast(c.brand_code as string) as mdm_brand_id,
                cast(a.cost as decimal(20, 4)) as cost,
                cast(a.clicks as bigint) as clicks,
                cast(a.impressions as bigint) as impressions,
                '' as cpa,
                cast(a.cpc as decimal(20, 4)) as cpc,
                cast(a.ecpm as decimal(20, 4)) as cpm,
                cast(null as decimal(9, 4)) as ctr,
                cast(roi as decimal(9, 4)) as total_order_roi,
                cast(null as decimal(9, 4)) as total_order_cvs,
                cast(null as bigint) as direct_cart_cnt,
                cast(null as bigint) as indirect_cart_cnt,
                cast(total_cart_cnt as bigint) as total_cart_quantity,
                cast(null as decimal(20, 4)) as direct_order_value,
                cast(null as decimal(20, 4)) as indirect_order_value,
                cast(order_sum as decimal(20, 4)) as order_value,
                cast(null as bigint) as direct_order_quantity,
                cast(null as bigint) as indirect_order_quantity,
                cast(order_cnt as bigint) as order_quantity,
                cast(null as bigint) as favorite_item_quantity,
                cast(null as bigint) as favorite_shop_quantity,
                cast(coupon_cnt as bigint) as coupon_quantity,
                cast(preorder_cnt as bigint) as preorder_quantity,
                cast(null as bigint) as depth_passenger_quantity,
                cast(new_customers_cnt as bigint) as new_customer_quantity,
                cast(null as bigint) as visit_page_quantity,
                cast(null as decimal(20, 4)) as visit_time_length,
                cast(null as bigint) as visitor_quantity,
                cast(null as bigint) as presale_direct_order_cnt,
                cast(null as bigint) as presale_indirect_order_cnt,
                cast(preorder_cnt as bigint) as total_presale_order_cnt,
                cast(null as decimal(20, 4)) as presale_direct_order_sum,
                cast(null as decimal(20, 4)) as presale_indirect_order_sum,
                cast(null as decimal(20, 4)) as total_presale_order_sum,
                cast(null as date) as click_date,
                cast(null as bigint) as platform_cnt,
                cast(null as decimal(20, 4)) as platform_gmv,
                cast(null as bigint) as department_cnt,
                cast(null as decimal(20, 4)) as department_gmv,
                cast(roi as decimal(20, 4)) as channel_roi,
                '' as mobile_type,
                '' as business_type,
                '' as gift_flag,
                '' as order_status_category,
                '' as click_or_order_caliber,
                '' as impression_or_click_effect,
                cast(null as date) as start_day,
                cast(null as date) as end_day,
                '' as is_daily,
                'stg.jdzt_adgroup_daily_old' as data_source,
                cast(dw_batch_id as string) as dw_batch_id,
                current_date() as dw_etl_date
            from stg.jdzt_account_daily_old_v2 a
            left join ods.media_category_brand_mapping c
              on a.brand_id = c.emedia_brand_code and
              a.category_id = c.emedia_category_code
            """
        )
        .dropDuplicates(jd_zt_account_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .insertInto("dwd.jdzt_account_daily_old_v2")
        .saveAsTable("dwd.jdzt_account_daily_old_v2")
    )
