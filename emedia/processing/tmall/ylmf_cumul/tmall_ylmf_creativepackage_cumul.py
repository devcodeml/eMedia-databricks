import datetime

from pyspark.sql import functions as F

from emedia import spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.common.emedia_brand_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia


def tmall_ylmf_daliy_creativepackage_cumul_etl(airflow_execution_date, run_id):
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

    mapping_blob_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_blob_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_blob_sas = emedia_conf_dict.get('mapping_blob_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn"
                   , mapping_blob_sas)
    file_date = etl_date - datetime.timedelta(days=1)
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ylmf_cumul_daily_displayreport/aliylmf_day_creativePackageReport_cumul_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    tmall_ylmf_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )
    first_row_data = tmall_ylmf_df.first().asDict()
    dw_batch_number = first_row_data.get('dw_batch_number')

    read_json_content_df = tmall_ylmf_df.select("*", F.json_tuple("rpt_info", "add_new_charge", "add_new_uv",
                                                                  "add_new_uv_cost", "add_new_uv_rate",
                                                                  "alipay_inshop_amt", "alipay_inshop_num",
                                                                  "avg_access_page_num", "avg_deep_access_times",
                                                                  "cart_num", "charge", "click", "cpc", "cpm", "ctr",
                                                                  "cvr", "deep_inshop_pv", "dir_shop_col_num",
                                                                  "gmv_inshop_amt", "gmv_inshop_num", "icvr",
                                                                  "impression", "inshop_item_col_num",
                                                                  "inshop_potential_uv", "inshop_potential_uv_rate",
                                                                  "inshop_pv", "inshop_pv_rate", "inshop_uv",
                                                                  "prepay_inshop_amt", "prepay_inshop_num", "return_pv",
                                                                  "return_pv_cost", "roi", "search_click_cnt",
                                                                  "search_click_cost"
                                                                  ).alias("add_new_charge",
                                                                          "add_new_uv",
                                                                          "add_new_uv_cost",
                                                                          "add_new_uv_rate",
                                                                          "alipay_inshop_amt",
                                                                          "alipay_inshop_num",
                                                                          "avg_access_page_num",
                                                                          "avg_deep_access_times",
                                                                          "cart_num", "charge",
                                                                          "click", "cpc", "cpm", "ctr",
                                                                          "cvr", "deep_inshop_pv",
                                                                          "dir_shop_col_num",
                                                                          "gmv_inshop_amt",
                                                                          "gmv_inshop_num", "icvr",
                                                                          "impression",
                                                                          "inshop_item_col_num",
                                                                          "inshop_potential_uv",
                                                                          "inshop_potential_uv_rate",
                                                                          "inshop_pv",
                                                                          "inshop_pv_rate",
                                                                          "inshop_uv",
                                                                          "prepay_inshop_amt",
                                                                          "prepay_inshop_num",
                                                                          "return_pv",
                                                                          "return_pv_cost", "roi",
                                                                          "search_click_cnt",
                                                                          "search_click_cost")).drop('rpt_info')

    report_df = read_json_content_df.na.fill("").selectExpr("log_data as ad_date", "campaign_group_id",
                                                            "campaign_group_name",
                                                            "campaign_id", "campaign_name",
                                                            "creative_package_id",
                                                            "creative_package_name",
                                                            "promotion_entity_id",
                                                            "promotion_entity_name",
                                                            "add_new_charge", "add_new_uv",
                                                            "add_new_uv_cost", "add_new_uv_rate",
                                                            "alipay_inshop_amt",
                                                            "alipay_inshop_num",
                                                            "avg_access_page_num",
                                                            "avg_deep_access_times",
                                                            "cart_num", "charge", "click", "cpc",
                                                            "cpm", "ctr", "cvr", "deep_inshop_pv", "dir_shop_col_num",
                                                            "gmv_inshop_amt", "gmv_inshop_num", "icvr", "impression",
                                                            "inshop_item_col_num", "inshop_potential_uv",
                                                            "inshop_potential_uv_rate", "inshop_pv", "inshop_pv_rate",
                                                            "inshop_uv", "prepay_inshop_amt", "prepay_inshop_num",
                                                            "return_pv", "return_pv_cost", "roi", "search_click_cnt",
                                                            "search_click_cost",
                                                            "`req_api_service_context.biz_code` as biz_code",
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

    fail_table_exist = spark.sql(
        "show tables in stg like 'media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_fail'").count()
    if fail_table_exist == 0:
        daily_reports = report_df
    else:
        fail_df = spark.table("stg.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_fail") \
            .drop('category_id') \
            .drop('brand_id') \
            .drop('etl_date') \
            .drop('etl_create_time')
        daily_reports = report_df.union(fail_df)

    ad_type = 'ylmf'

    tmall_ylmf_mapping_pks = ['ad_date', 'campaign_group_id', 'campaign_id', "creative_package_id",
                              "promotion_entity_id",
                              'effect_days', 'req_storeId']
    # 引用mapping函数
    res = emedia_brand_mapping(spark, daily_reports, ad_type, etl_date=etl_date, etl_create_time=date_time,
                               mapping_pks=tmall_ylmf_mapping_pks)
    all_success_df = res[0]
    all_success_df.createOrReplaceTempView("all_mapping_success")
    table_exist = spark.sql(
        "show tables in dws like 'media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success'").count()
    if table_exist == 0:
        all_success_df.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .insertInto.saveAsTable("dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success")
    else:
        spark.sql("""
              MERGE INTO dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success
              USING all_mapping_success
              ON dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.ad_date = all_mapping_success.ad_date
                  AND dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.campaign_group_id = all_mapping_success.campaign_group_id
                  AND dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.campaign_id = all_mapping_success.campaign_id
                  AND dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.promotion_entity_id = all_mapping_success.promotion_entity_id
                  AND dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.creative_package_id = all_mapping_success.creative_package_id
                  AND dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.effect_days = all_mapping_success.effect_days
                  AND dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.req_storeId = all_mapping_success.req_storeId
                  AND ((dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.campaign_group_id = all_mapping_success.campaign_group_id)
                          OR
                       (dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success.campaign_group_id IS null and all_mapping_success.campaign_group_id IS null))
              WHEN MATCHED THEN
                  UPDATE SET *
              WHEN NOT MATCHED
                  THEN INSERT *
            """)
    all_fail_df = res[1]
    all_fail_df.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_fail")
    # 增量输出
    out_df = spark.sql(f'''
        select 
            date_format(ad_date,'yyyyMMdd') as ad_date,
            campaign_group_id,
            campaign_group_name,
            campaign_id,
            campaign_name,
            creative_package_id,
            creative_package_name,
            promotion_entity_id,
            promotion_entity_name,
            if(isnull(add_new_charge) or add_new_charge = '' or add_new_charge = 0.00000, 0, cast(add_new_charge as decimal(20,5))) as add_new_charge,
            if(isnull(add_new_uv) or add_new_uv = '' or add_new_uv = 0.00000, 0, add_new_uv) as add_new_uv,
            if(isnull(add_new_uv_cost) or add_new_uv_cost = '' or add_new_uv_cost = 0.00000, 0, cast(add_new_uv_cost as decimal(20,5))) as add_new_uv_cost,
            if(isnull(add_new_uv_rate) or add_new_uv_rate = '' or add_new_uv_rate = 0.00000, 0, cast(add_new_uv_rate as decimal(20,5))) as add_new_uv_rate,
            if(isnull(alipay_inshop_amt) or alipay_inshop_amt = '' or alipay_inshop_amt = 0.00000, 0, cast(alipay_inshop_amt as decimal(20,5))) as alipay_inshop_amt,
            if(isnull(alipay_inshop_num) or alipay_inshop_num = '' or alipay_inshop_num = 0.00000, 0, alipay_inshop_num) as alipay_inshop_num,
            if(isnull(avg_access_page_num) or avg_access_page_num = '' or avg_access_page_num = 0.00000, 0, cast(avg_access_page_num as decimal(20,5))) as avg_access_page_num,
            if(isnull(avg_deep_access_times) or avg_deep_access_times = '' or avg_deep_access_times = 0.00000, 0, cast(avg_deep_access_times as decimal(20,5))) as avg_deep_access_times,
            if(isnull(cart_num) or cart_num = '' or cart_num = 0.00000, 0, cart_num) as cart_num,
            if(isnull(charge) or charge = '' or charge = 0.00000, 0, cast(charge as decimal(20,5))) as charge,
            if(isnull(click) or click = '' or click = 0.00000, 0, click) as click,
            if(isnull(cpc) or cpc = '' or cpc = 0.00000, 0, cast(cpc as decimal(20,5))) as cpc,
            if(isnull(cpm) or cpm = '' or cpm = 0.00000, 0, cast(cpm as decimal(20,5))) as cpm,
            if(isnull(ctr) or ctr = '' or ctr = 0.00000, 0, cast(ctr as decimal(20,5))) as ctr,
            if(isnull(cvr) or cvr = '' or cvr = 0.00000, 0, cast(cvr as decimal(20,5))) as cvr,
            if(isnull(deep_inshop_pv) or deep_inshop_pv = '' or deep_inshop_pv = 0.00000, 0, deep_inshop_pv) as deep_inshop_pv,
            if(isnull(dir_shop_col_num) or dir_shop_col_num = '' or dir_shop_col_num = 0.00000, 0, dir_shop_col_num) as dir_shop_col_num,
            if(isnull(gmv_inshop_amt) or gmv_inshop_amt = '' or gmv_inshop_amt = 0.00000, 0, cast(gmv_inshop_amt as decimal(20,5))) as gmv_inshop_amt,
            if(isnull(gmv_inshop_num) or gmv_inshop_num = '' or gmv_inshop_num = 0.00000, 0, gmv_inshop_num) as gmv_inshop_num,
            if(isnull(icvr) or icvr = '' or icvr = 0.00000, 0, cast(icvr as decimal(20,5))) as icvr,
            if(isnull(impression) or impression = '' or impression = 0.00000, 0, impression) as impression,
            if(isnull(inshop_item_col_num) or inshop_item_col_num = '' or inshop_item_col_num = 0.00000, 0, inshop_item_col_num) as inshop_item_col_num,
            if(isnull(inshop_potential_uv) or inshop_potential_uv = '' or inshop_potential_uv = 0.00000, 0, inshop_potential_uv) as inshop_potential_uv,
            if(isnull(inshop_potential_uv_rate) or inshop_potential_uv_rate = '' or inshop_potential_uv_rate = 0.00000, 0, cast(inshop_potential_uv_rate as decimal(20,5))) as inshop_potential_uv_rate,
            if(isnull(inshop_pv) or inshop_pv = '' or inshop_pv = 0.00000, 0, inshop_pv) as inshop_pv,
            if(isnull(inshop_pv_rate) or inshop_pv_rate = '' or inshop_pv_rate = 0.00000, 0, cast(inshop_pv_rate as decimal(20,5))) as inshop_pv_rate,
            if(isnull(inshop_uv) or inshop_uv = '' or inshop_uv = 0.00000, 0, inshop_uv) as inshop_uv,
            if(isnull(prepay_inshop_amt) or prepay_inshop_amt = '' or prepay_inshop_amt = 0.00000, 0, cast(prepay_inshop_amt as decimal(20,5))) as prepay_inshop_amt,
            if(isnull(prepay_inshop_num) or prepay_inshop_num = '' or prepay_inshop_num = 0.00000, 0, prepay_inshop_num) as prepay_inshop_num,
            if(isnull(return_pv) or return_pv = '' or return_pv = 0.00000, 0, return_pv) as return_pv,
            if(isnull(return_pv_cost) or return_pv_cost = '' or return_pv_cost = 0.00000, 0, cast(return_pv_cost as decimal(20,5))) as return_pv_cost,
            if(isnull(roi) or roi = '' or roi = 0.00000, 0, cast(roi as decimal(20,5))) as roi,
            if(isnull(search_click_cnt) or search_click_cnt = '' or search_click_cnt = 0.00000, 0, search_click_cnt) as search_click_cnt,
            if(isnull(search_click_cost) or search_click_cost = '' or search_click_cost = 0.00000, 0, cast(search_click_cost as decimal(20,5))) as search_click_cost,
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
            dw_batch_number,
            category_id,
            brand_id,
            'tmall' as data_source,
            etl_date as dw_etl_date,
            '{run_id}' as dw_batch_id
        from (
            select * from dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success where dw_batch_number = '{dw_batch_number}'
            union 
            select * from stg.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_fail where dw_batch_number = '{dw_batch_number}'
        )
    ''')

    # GM
    output_to_emedia(out_df, f'{date}/{date_time}/ylmf_cuml',
                     'EMEDIA_TMALL_YLMF_CUML_DAILY_CREATIVEPACKAGE_REPORT_FACT.CSV',dict_key='cumul')
    # eab
    output_to_emedia(out_df,
                     f'fetchResultFiles/ALI_days/YLMF_CUMUL/{run_id}',
                     f'tmall_ylmf_day_creativePackage_{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    spark.sql('optimize dws.media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success')

    return 0
