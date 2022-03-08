# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.functions import lit

from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia


tmall_super_report_mapping_success_tbl = 'dws.tb_emedia_tmall_super_report_mapping_success'
tmall_super_report_mapping_fail_tbl = 'stg.tb_emedia_tmall_super_report_mapping_fail'


tmall_super_report_pks = [
    'req_date'
    , 'clickPv'
    , 'clickUv'
    , 'adzoneName'
    , 'req_group_by_adzone'
]


output_tmall_super_report_pks = [
    'ad_date'
    , 'click_pv'
    , 'click_uv'
    , 'position_name'
    , 'is_position'
]


def tmall_super_report_etl(airflow_execution_date,run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]

    days_ago912 = (etl_date - datetime.timedelta(days=912)).strftime("%Y-%m-%d")

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get('input_blob_account')
    input_container = emedia_conf_dict.get('input_blob_container')
    input_sas = emedia_conf_dict.get('input_blob_sas')
    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    file_date = etl_date - datetime.timedelta(days=1)

    tmall_super_report_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/tmsha_daily_adreport/tmsha_daily_adreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_super_report file: {tmall_super_report_path}')

    tmall_super_report_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_super_report_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    ).dropDuplicates(tmall_super_report_pks)
    tmall_super_report_daily_df.withColumn('data_source',lit("tmall"))\
    .withColumn('dw_etl_date',lit(etl_date))\
    .withColumn('dw_batch_id',lit(run_id))\
    .withColumn("etl_date", current_date()) \
    .withColumn("etl_create_time", current_timestamp()).createOrReplaceTempView("tmall_super_report_daily")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_tmall_super_report_mapping_success

        USING tmall_super_report_daily
        
        ON dws.tb_emedia_tmall_super_report_mapping_success.req_date = tmall_super_report_daily.req_date

        AND dws.tb_emedia_tmall_super_report_mapping_success.clickPv = tmall_super_report_daily.clickPv
        
        AND dws.tb_emedia_tmall_super_report_mapping_success.clickUv = tmall_super_report_daily.clickUv
        
        AND dws.tb_emedia_tmall_super_report_mapping_success.adzoneName = tmall_super_report_daily.adzoneName
        
        AND dws.tb_emedia_tmall_super_report_mapping_success.req_group_by_adzone = tmall_super_report_daily.req_group_by_adzone
        
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)



    # Query output result
    tb_emedia_tmall_super_report_df = spark.sql(f'''
        SELECT
            date_format(req_date, 'yyyyMMdd') as ad_date,
            clickPv as click_pv,
            clickUv as click_uv,
            adzoneName as position_name,
            req_group_by_adzone as is_position,
            dw_create_time as create_time,
            data_source,
            dw_etl_date
        FROM (
            SELECT *
            FROM dws.tb_emedia_tmall_super_report_mapping_success 
                UNION
            SELECT *
            FROM stg.tb_emedia_tmall_super_report_mapping_fail
        )
        WHERE req_date >= '{days_ago912}' AND req_date <= '{etl_date}'
    ''').dropDuplicates(output_tmall_super_report_pks)

    output_to_emedia(tb_emedia_tmall_super_report_df, f'{date}/{date_time}/ha', 'TMALL_SUPER_REPORT_FACT.CSV')

    return 0

