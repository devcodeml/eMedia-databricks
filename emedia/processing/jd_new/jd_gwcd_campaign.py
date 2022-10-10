# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_gwcd_campaign_etl_new(airflow_execution_date, run_id):
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

    jd_gwcd_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/gwcd_daily_Report/jd_gwcd_campaign_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_gwcd_campaign_path: " + jd_gwcd_campaign_path)

    jd_gwcd_campaign_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_gwcd_campaign_path}",
        header=True,
        multiLine=True,
        sep="|",
    )

    jd_gwcd_campaign_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.campaign.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.gwcd_campaign_daily"
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
          cast(deliveryVersion as string) as deliveryVersion,
          cast(putType as string) as put_type,
          cast(mobileType as string) as mobile_type,
          cast(campaignType as string) as campaign_type,
          cast(campaignType as string) as campaign_put_type,
          to_date(cast(clickDate as string), 'yyyyMMdd') as click_date,
          cast(req_businessType as string) as business_type,
          cast(req_giftFlag as string) as gift_flag,
          cast(req_orderStatusCategory as string) as order_status_category,
          cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
          cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
          cast(req_startDay as date) as start_day,
          cast(req_endDay as date) as end_day,
          cast(req_isDaily as string) as is_daily,
          'stg.gwcd_campaign_daily' as data_source,
          cast(dw_batch_id as string) as dw_batch_id
        from stg.gwcd_campaign_daily
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.gwcd_campaign_daily"
    )

    jd_gwcd_campaign_pks = [
        "ad_date",
        "pin",
        "effect_days",
        "campaign_id",
        "mobile_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]

    jd_gwcd_campaign_df = (
        spark.table("ods.gwcd_campaign_daily").drop("dw_etl_date").drop("data_source")
    )
    jd_gwcd_campaign_fail_df = (
        spark.table("dwd.gwcd_campaign_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jd_gwcd_campaign_daily_mapping_success,
        jd_gwcd_campaign_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark, jd_gwcd_campaign_df.union(jd_gwcd_campaign_fail_df), "gwcd"
    )
    jd_gwcd_campaign_daily_mapping_success.dropDuplicates(
        jd_gwcd_campaign_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.gwcd_campaign_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in jd_gwcd_campaign_pks]
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

    jd_gwcd_campaign_daily_mapping_fail.dropDuplicates(jd_gwcd_campaign_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.gwcd_campaign_daily_mapping_fail")

    spark.table("dwd.gwcd_campaign_daily_mapping_success").union(
        spark.table("dwd.gwcd_campaign_daily_mapping_fail")
    ).createOrReplaceTempView("gwcd_campaign_daily")

    gwcd_campaign_daily_res = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          c.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from gwcd_campaign_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code and
          a.category_id = c.emedia_category_code
        """
    )

    gwcd_campaign_daily_res.selectExpr(
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
        "deliveryVersion",
        "put_type",
        "mobile_type",
        "campaign_type",
        "campaign_put_type",
        "click_date",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.gwcd_campaign_daily' as data_source",
        "dw_batch_id",
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.gwcd_campaign_daily"
    )

    # 推送数据到dw
    write_to_dw = spark.table("dwd.gwcd_campaign_daily").distinct()
    table_name = "dbo.tb_emedia_jd_gwcd_campaign_daily_v202209_fact"
    model = "overwrite"
    push_to_dw(write_to_dw, table_name, model)


# 以下这次不上线  待测试验证
#     spark.sql("delete from dwd.tb_media_emedia_gwcd_daily_fact where report_level = 'campaign' ")
#     spark.table("dwd.gwcd_campaign_daily").selectExpr('ad_date',"ad_format_lv2", 'pin_name', 'effect', 'effect_days', 'campaign_id', 'campaign_name',
#                                         "adgroup_id" ,"adgroup_name","report_level","report_level_id","report_level_name",
#                                         'emedia_category_id', 'emedia_brand_id','mdm_category_id','mdm_brand_id','mdm_productline_id','delivery_version',
#                                          "delivery_type", 'mobile_type',"source",'business_type','gift_flag','order_status_category','click_or_order_caliber',
#                                         'put_type', 'campaign_type', 'campaign_put_type',"cost", 'click', 'impressions','order_quantity','order_value',
#                                         'total_cart_quantity', 'new_customer_quantity', "dw_source" ,'dw_create_time',
#                                         'dw_batch_number', "'dwd.gwcd_campaign_daily' as etl_source_table") \
#         .withColumn("etl_create_time", F.current_timestamp()) \
#         .withColumn("etl_update_time", F.current_timestamp()).distinct().write.mode(
#         "append").insertInto("dwd.tb_media_emedia_gwcd_daily_fact")


# ds.hc_emedia_gwcd_deep_dive_download_campaign_adgroup_daily_fact
def gwcd_deep_dive_download_campaign_adgroup_daily_fact():
    spark.table("dwd.tb_media_emedia_gwcd_daily_fact").selectExpr(
        "ad_date",
        "ad_format_lv2",
        "pin_name",
        "effect",
        "effect_days",
        "campaign_id",
        "campaign_name",
        "adgroup_id",
        "adgroup_name",
        "report_level",
        "report_level_id",
        "report_level_name",
        "mdm_category_id as emedia_category_id",
        "mdm_brand_id as emedia_brand_id",
        "mdm_productline_id",
        "delivery_version",
        "delivery_type",
        "mobile_type",
        "source",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "put_type",
        "campaign_type",
        "campaign_put_type",
        "cost",
        "click",
        "impressions",
        "order_quantity",
        "order_value",
        "total_cart_quantity",
        "new_customer_quantity",
        "dw_source",
        "dw_create_time",
        "dw_batch_number",
        "'dwd.tb_media_emedia_gwcd_daily_fact' as etl_source_table",
    ).withColumn("etl_create_time", current_timestamp()).withColumn(
        "etl_update_time", current_timestamp()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ds.hc_emedia_gwcd_deep_dive_download_campaign_adgroup_daily_fact"
    )


def push_status(airflow_execution_date):
    output_date = airflow_execution_date[0:10]
    output_date_time = output_date + "T" + airflow_execution_date[11:19]
    output_date_text = ""

    # jd_gwcd_compaign
    # 写空文件到blob
    output_df.create_blob_by_text(
        f"{output_date}/{output_date_time}/gwcd/EMEDIA_JD_GWCD_DAILY_CAMPAIGN_REPORT_FACT.CSV",
        output_date_text,
        "target",
    )
    # 写状态到dw
    status_sql = spark.sql(
        f"""
        select 'jd_gwcd_campaign_etl' as job_name,'emedia' as type,'{output_date}/{output_date_time}/gwcd/EMEDIA_JD_GWCD_DAILY_CAMPAIGN_REPORT_FACT.CSV' as file_name,'' as job_id,
        1 as status,now() as updateAt,'{output_date}' as period,'{output_date_time}' as flag,'' as related_job
    """
    )
    push_to_dw(status_sql, "dbo.mpt_etl_job", "append")


def push_to_dw(dataframe, table, model):
    project_name = "emedia"
    table_name = "gwcd_campaign_daily"
    emedia_conf_dict = get_emedia_conf_dict()
    user = "pgadmin"
    password = "93xx5Px1bkVuHgOo"
    synapseaccountname = emedia_conf_dict.get("synapseaccountname")
    synapsedirpath = emedia_conf_dict.get("synapsedirpath")
    synapsekey = emedia_conf_dict.get("synapsekey")
    url = "jdbc:sqlserver://b2bmptbiqa0101.database.chinacloudapi.cn:1433;database=B2B-qa-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;"

    # b2bmptbiqa0101.database.chinacloudapi.cn
    # pgadmin    93xx5Px1bkVuHgOo
    # 获取key
    blobKey = "fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(
        synapseaccountname
    )
    # 获取然后拼接 blob 临时路径
    tempDir = r"{0}/{1}/{2}/".format(synapsedirpath, project_name, table_name)
    # 将key配置到环境中
    spark.conf.set(blobKey, synapsekey)

    dataframe.write.mode(model).format("com.databricks.spark.sqldw").option(
        "url", url
    ).option("user", user).option("password", password).option(
        "forwardSparkAzureStorageCredentials", "true"
    ).option(
        "dbTable", table
    ).option(
        "tempDir", tempDir
    ).save()

    return 0
