import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from functools import reduce
from emedia import spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

from emedia.utils.output_df import output_to_emedia


def tmall_ylmf_daliy_adzone_cumul_etl_new(airflow_execution_date):
    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))
    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]

    # 输入输出mapping blob信息，自行确认

    emedia_conf_dict = get_emedia_conf_dict()
    input_blob_account = emedia_conf_dict.get('input_blob_account')
    input_blob_container = emedia_conf_dict.get('input_blob_container')
    input_blob_sas = emedia_conf_dict.get('input_blob_sas')
    spark.conf.set(f"fs.azure.sas.{input_blob_container}.{input_blob_account}.blob.core.chinacloudapi.cn"
                   , input_blob_sas)

    file_date = etl_date - datetime.timedelta(days=1)
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ylmf_cumul_daily_displayreport/aliylmf_day_adzoneReport_cumul_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    tmall_ylmf_adzone_cumul_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )


    # extend json content
    read_json_content_df = tmall_ylmf_adzone_cumul_df.select("*", F.json_tuple("rpt_info",
                                                                  "add_new_charge", "add_new_uv",
                                                                  "add_new_uv_cost", "add_new_uv_rate",
                                                                  "alipay_inshop_amt", "alipay_inshop_num",
                                                                  "avg_access_page_num", "avg_deep_access_times",
                                                                  "cart_num", "charge", "click", "cpc", "cpm", "ctr",
                                                                  "cvr",
                                                                  "deep_inshop_pv", "dir_shop_col_num",
                                                                  "gmv_inshop_amt", "gmv_inshop_num", "icvr",
                                                                  "impression", "inshop_item_col_num",
                                                                  "inshop_potential_uv", "inshop_potential_uv_rate",
                                                                  "inshop_pv", "inshop_pv_rate", "inshop_uv",
                                                                  "prepay_inshop_amt", "prepay_inshop_num",
                                                                  "return_pv", "return_pv_cost", "roi",
                                                                  "search_click_cnt", "search_click_cost"
                                                                  ).alias("add_new_charge",
                                                                          "add_new_uv",
                                                                          "add_new_uv_cost",
                                                                          "add_new_uv_rate",
                                                                          "alipay_inshop_amt",
                                                                          "alipay_inshop_num",
                                                                          "avg_access_page_num",
                                                                          "avg_deep_access_times",
                                                                          "cart_num", "charge", "click", "cpc", "cpm",
                                                                          "ctr", "cvr",
                                                                          "deep_inshop_pv", "dir_shop_col_num",
                                                                          "gmv_inshop_amt", "gmv_inshop_num", "icvr",
                                                                          "impression", "inshop_item_col_num",
                                                                          "inshop_potential_uv",
                                                                          "inshop_potential_uv_rate",
                                                                          "inshop_pv", "inshop_pv_rate", "inshop_uv",
                                                                          "prepay_inshop_amt", "prepay_inshop_num",
                                                                          "return_pv", "return_pv_cost", "roi",
                                                                          "search_click_cnt",
                                                                          "search_click_cost")).drop('rpt_info')

    tmall_ylmf_adzone_daily_df = read_json_content_df.na.fill("").selectExpr("log_data as ad_date", "campaign_group_id",
                                                            "campaign_group_name",
                                                            "campaign_id", "campaign_name",
                                                            "adzone_id", "adzone_name",
                                                            "promotion_entity_id",
                                                            "promotion_entity_name", "add_new_charge", "add_new_uv",
                                                            "add_new_uv_cost", "add_new_uv_rate",
                                                            "alipay_inshop_amt", "alipay_inshop_num",
                                                            "avg_access_page_num", "avg_deep_access_times",
                                                            "cart_num", "charge", "click", "cpc", "cpm", "ctr", "cvr",
                                                            "deep_inshop_pv", "dir_shop_col_num",
                                                            "gmv_inshop_amt", "gmv_inshop_num", "icvr", "impression",
                                                            "inshop_item_col_num", "inshop_potential_uv",
                                                            "inshop_potential_uv_rate",
                                                            "inshop_pv", "inshop_pv_rate", "inshop_uv",
                                                            "prepay_inshop_amt", "prepay_inshop_num",
                                                            "return_pv", "return_pv_cost", "roi", "search_click_cnt",
                                                            "`req_api_service_context.biz_code` as biz_code",
                                                            "search_click_cost",
                                                            "`req_report_query.offset` as offset",
                                                            "`req_report_query.page_size` as page_size",
                                                            "`req_report_query.query_time_dim` as query_time_dim",
                                                            "`req_report_query.query_domain` as query_domain",
                                                            "`req_report_query.group_by_campaign_id` as group_by_campaign_id",
                                                            "`req_report_query.group_by_log_date` as group_by_log_date",
                                                            "`req_report_query.group_by_promotion_entity_id` as group_by_promotion_entity_id",
                                                            "`req_report_query.start_time` as start_time",
                                                            "`req_report_query.end_time` as end_time",
                                                            "`req_report_query.effect_type` as effect_type",
                                                            "`req_report_query.effect` as effect",
                                                            "`report_query.effect_days` as effect_days",
                                                            "req_storeId",
                                                            "dw_resource", "dw_create_time", "dw_batch_number")


    tmall_ylmf_adzone_daily_df.withColumn("etl_date", F.current_date())\
        .withColumn("etl_create_time", F.current_timestamp())\
        .distinct().write.mode("overwrite").insertInto("stg.ylmf_adzone_cumul_daily")


    spark.sql(
        """
        select 
            cast(ad_date as date) as ad_date,
            campaign_group_id,
            campaign_group_name,
            campaign_id,
            campaign_name,
            adzone_id,
            adzone_name,
            promotion_entity_id,
            promotion_entity_name,
            if(isnull(add_new_charge) or add_new_charge = '' or add_new_charge = '0.00000', 0, cast(add_new_charge as decimal(20,5))) as add_new_charge,
            if(isnull(add_new_uv) or add_new_uv = '' or add_new_uv = '0.00000', 0, add_new_uv) as add_new_uv,
            if(isnull(add_new_uv_cost) or add_new_uv_cost = '' or add_new_uv_cost = '0.00000', 0, cast(add_new_uv_cost as decimal(20,5))) as add_new_uv_cost,
            if(isnull(add_new_uv_rate) or add_new_uv_rate = '' or add_new_uv_rate = '0.00000', 0, cast(add_new_uv_rate as decimal(20,5))) as add_new_uv_rate,
            if(isnull(alipay_inshop_amt) or alipay_inshop_amt = '' or alipay_inshop_amt = '0.00000', 0, cast(alipay_inshop_amt as decimal(20,5))) as alipay_inshop_amt,
            if(isnull(alipay_inshop_num) or alipay_inshop_num = '' or alipay_inshop_num = '0.00000', 0, alipay_inshop_num) as alipay_inshop_num,
            if(isnull(avg_access_page_num) or avg_access_page_num = '' or avg_access_page_num = '0.00000', 0, cast(avg_access_page_num as decimal(20,5))) as avg_access_page_num,
            if(isnull(avg_deep_access_times) or avg_deep_access_times = '' or avg_deep_access_times = '0.00000', 0, cast(avg_deep_access_times as decimal(20,5))) as avg_deep_access_times,
            if(isnull(cart_num) or cart_num = '' or cart_num = '0.00000', 0, cart_num) as cart_num,
            if(isnull(charge) or charge = '' or charge = '0.00000', 0, cast(charge as decimal(20,5))) as charge,
            if(isnull(click) or click = '' or click = '0.00000', 0, click) as click,
            if(isnull(cpc) or cpc = '' or cpc = '0.00000', 0, cast(cpc as decimal(20,5))) as cpc,
            if(isnull(cpm) or cpm = '' or cpm = '0.00000', 0, cast(cpm as decimal(20,5))) as cpm,
            if(isnull(ctr) or ctr = '' or ctr = '0.00000', 0, cast(ctr as decimal(20,5))) as ctr,
            if(isnull(cvr) or cvr = '' or cvr = '0.00000', 0, cast(cvr as decimal(20,5))) as cvr,
            if(isnull(deep_inshop_pv) or deep_inshop_pv = '' or deep_inshop_pv = '0.00000', 0, deep_inshop_pv) as deep_inshop_pv,
            if(isnull(dir_shop_col_num) or dir_shop_col_num = '' or dir_shop_col_num = '0.00000', 0, dir_shop_col_num) as dir_shop_col_num,
            if(isnull(gmv_inshop_amt) or gmv_inshop_amt = '' or gmv_inshop_amt = '0.00000', 0, cast(gmv_inshop_amt as decimal(20,5))) as gmv_inshop_amt,
            if(isnull(gmv_inshop_num) or gmv_inshop_num = '' or gmv_inshop_num = '0.00000', 0, gmv_inshop_num) as gmv_inshop_num,
            if(isnull(icvr) or icvr = '' or icvr = '0.00000', 0, cast(icvr as decimal(20,5))) as icvr,
            if(isnull(impression) or impression = '' or impression = '0.00000', 0, impression) as impression,
            if(isnull(inshop_item_col_num) or inshop_item_col_num = '' or inshop_item_col_num = '0.00000', 0, inshop_item_col_num) as inshop_item_col_num,
            if(isnull(inshop_potential_uv) or inshop_potential_uv = '' or inshop_potential_uv = '0.00000', 0, inshop_potential_uv) as inshop_potential_uv,
            if(isnull(inshop_potential_uv_rate) or inshop_potential_uv_rate = '' or inshop_potential_uv_rate = '0.00000', 0, cast(inshop_potential_uv_rate as decimal(20,5))) as inshop_potential_uv_rate,
            if(isnull(inshop_pv) or inshop_pv = '' or inshop_pv = '0.00000', 0, inshop_pv) as inshop_pv,
            if(isnull(inshop_pv_rate) or inshop_pv_rate = '' or inshop_pv_rate = '0.00000', 0, cast(inshop_pv_rate as decimal(20,5))) as inshop_pv_rate,
            if(isnull(inshop_uv) or inshop_uv = '' or inshop_uv = '0.00000', 0, inshop_uv) as inshop_uv,
            if(isnull(prepay_inshop_amt) or prepay_inshop_amt = '' or prepay_inshop_amt = '0.00000', 0, cast(prepay_inshop_amt as decimal(20,5))) as prepay_inshop_amt,
            if(isnull(prepay_inshop_num) or prepay_inshop_num = '' or prepay_inshop_num = '0.00000', 0, prepay_inshop_num) as prepay_inshop_num,
            if(isnull(return_pv) or return_pv = '' or return_pv = '0.00000', 0, return_pv) as return_pv,
            if(isnull(return_pv_cost) or return_pv_cost = '' or return_pv_cost = '0.00000', 0, cast(return_pv_cost as decimal(20,5))) as return_pv_cost,
            if(isnull(roi) or roi = '' or roi = '0.00000', 0, cast(roi as decimal(20,5))) as roi,
            if(isnull(search_click_cnt) or search_click_cnt = '' or search_click_cnt = '0.00000', 0, search_click_cnt) as search_click_cnt,
            if(isnull(search_click_cost) or search_click_cost = '' or search_click_cost = '0.00000', 0, cast(search_click_cost as decimal(20,5))) as search_click_cost,
            biz_code,
            offset,
            page_size,
            query_time_dim,
            query_domain,
            group_by_campaign_id,
            group_by_log_date,
            group_by_promotion_entity_id,
            start_time,
            end_time,
            effect_type,
            effect,
            effect_days,
            req_storeId as store_id,
            dw_resource,
            dw_create_time,
            dw_batch_number
        from stg.ylmf_adzone_cumul_daily
        """
    ).withColumn("etl_source_table", F.lit("stg.ylmf_adzone_cumul_daily"))\
        .withColumn("etl_date", F.current_date()).withColumn("etl_create_time", F.current_timestamp())\
        .distinct().write.mode("overwrite")\
        .insertInto("ods.ylmf_adzone_cumul_daily")

    # 输出eab代码 其他TO DO
    # out_df = spark.table("ods.ylmf_adzone_cumul_daily").drop('etl_source_table').drop('etl_date').drop('etl_create_time')
    # out_df.withColumn('category_id',F.lit('')).withColumn('brand_id',F.lit('')).withColumn('data_source',F.lit('tmall')).withColumn("etl_date", F.current_date()).withColumn('dw_batch_id',F.lit(f'{run_id}'))
    # output_to_emedia(out_df,
    #                  f'fetchResultFiles/ALI_days/YLMF_CUMUL/{run_id}',
    #                  f'tmall_ylmf_day_adzone_{date}.csv.gz',
    #                  dict_key='eab', compression='gzip', sep='|')


    tmall_ylmf_adzone_daily_pks = [
        "ad_date",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "adzone_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]

    tmall_ylmf_adzone_daily_df = (
        spark.table("ods.ylmf_adzone_cumul_daily").drop("etl_date").withColumnRenamed('store_id','req_storeId')
        .drop("etl_create_time")
    )
    tmall_ylmf_adzone_daily_fail_df = (
        spark.table("dwd.ylmf_adzone_cumul_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        ylmf_adzone_daily_mapping_success,
        ylmf_adzone_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark,
        tmall_ylmf_adzone_daily_df.union(tmall_ylmf_adzone_daily_fail_df),
        "ylmf",
    )



    ylmf_adzone_daily_mapping_success.dropDuplicates(
        tmall_ylmf_adzone_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.ylmf_adzone_cumul_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [
            f"{dwd_table}.{col} <=> {tmp_table}.{col}"
            for col in tmall_ylmf_adzone_daily_pks
        ]
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
        ylmf_adzone_daily_mapping_fail.dropDuplicates(tmall_ylmf_adzone_daily_pks)
        .write.mode("overwrite")
        .insertInto("dwd.ylmf_adzone_cumul_daily_mapping_fail")
    )

    spark.table("dwd.ylmf_adzone_cumul_daily_mapping_success").union(
        spark.table("dwd.ylmf_adzone_cumul_daily_mapping_fail")
    ).createOrReplaceTempView("ylmf_adzone_cumul_daily")

    ylmf_adzone_daily_res = spark.sql(
        """
        select
          a.*,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from ylmf_adzone_cumul_daily a
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code
        left join ods.media_category_brand_mapping d
            on a.category_id = d.emedia_category_code
        """
    )

    (
        ylmf_adzone_daily_res.distinct()
        .withColumn("etl_source_table", F.lit("ods.ylmf_adzone_cumul_daily"))
        .withColumn("etl_date", F.current_date())
        .withColumn("etl_create_time", F.current_timestamp())
        .write.mode("overwrite")
        .insertInto("dwd.ylmf_adzone_cumul_daily")
    )



    tmall_ylmf_daily_fact_pks = [
        "ad_date",
        "store_id",
        "effect_type",
        "effect",
        "effect_days",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "report_level_id",
    ]
    # dwd.tb_media_emedia_ylmf_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_ylmf_cumul_daily_fact
        where `report_level` = 'adzone'
        """
    )

    spark.sql(
        f"""
      SELECT
          t1.ad_date,
          '引力魔方' as ad_format_lv2,
          t1.req_storeId as store_id,
          t1.effect_type,
          t1.effect,
          t1.effect_days,
          t1.campaign_group_id,
          t1.campaign_group_name,
          t1.campaign_id,
          t1.campaign_name,
          t1.promotion_entity_id,
          t1.promotion_entity_name,
          'adzone' as report_level,
          t1.adzone_id as report_level_id,
          t1.adzone_name as report_level_name,
          '' as sub_crowd_name,
          '' as audience_name,
          t1.emedia_category_id as emedia_category_id,
          t1.emedia_brand_id as emedia_brand_id,
          t1.mdm_category_id as mdm_category_id,
          t1.mdm_brand_id as mdm_brand_id,
          case
            when t2.localProductLineId is null then ''
            when t2.localProductLineId = 'N/A' then ''
            else t2.localProductLineId
          end as mdm_productline_id,
          round(nvl(t1.charge, 0), 4) as cost,
          nvl(t1.click, 0) as click,
          nvl(t1.impression, 0) as impression,
          nvl(t1.alipay_inshop_num, 0) as order_quantity,
          round(nvl(t1.alipay_inshop_amt, 0), 4) as order_amount,
          nvl(t1.cart_num, 0) as cart_quantity,
          nvl(t1.gmv_inshop_num, 0) as gmv_order_quantity,
          t1.dw_resource,
          t1.dw_create_time,
          t1.dw_batch_number,
          'dwd.ylmf_adzone_cumul_daily' as etl_source_table,
          current_timestamp() as etl_create_time,
          current_timestamp() as etl_update_time
      FROM
          dwd.ylmf_adzone_cumul_daily t1
      LEFT JOIN
          stg.media_mdl_douyin_cdl t2
      ON
          t1.promotion_entity_id = t2.numIid
      """
    ).dropDuplicates(tmall_ylmf_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_ylmf_cumul_daily_fact"
    )

    return 0







