import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict

spark = get_spark()


def tmsha_report_daily_etl(airflow_execution_date):
    # date
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_account")
    input_container = emedia_conf_dict.get("input_container")
    input_sas = emedia_conf_dict.get("input_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )
    # daily report

    tmall_super_report_path = (
        f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall'
        "/tmsha_daily_adreport/tmsha_new_daily_adreport_"
        f'{file_date.strftime("%Y-%m-%d")}.csv.gz'
    )

    (
        spark.read.csv(
            f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
            f"{tmall_super_report_path}",
            header=True,
            multiLine=True,
            sep="|",
            quote='"',
            escape='"',
            inferSchema=True,
        )
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("stg.tmsha_report_daily")
        .insertInto("stg.tmsha_report_daily")
    )
    # ods.tmsha_report_daily
    (
        spark.sql(
            """
            select
                cast(adzoneName as string) as position_name,
                cast(clickUv as bigint) as click_uv,
                cast(clickPv as bigint) as click_pv,
                cast(req_date as date) as ad_date,
                req_group_by_adzone,
                req_storeId,
                cast(dw_resource as string) as dw_source,
                cast(dw_create_time as string) as dw_create_time,
                cast(dw_batch_number as string) as dw_batch_number,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from stg.tmsha_report_daily
            """
        )
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.tmsha_report_daily")
        .insertInto("ods.tmsha_report_daily")
    )
    (
        spark.sql(
            """
            select
                ad_date,
                '猫超硬广' as ad_format_lv2,
                '' as effect,
                '' as effect_days,
                position_name,
                '' as emedia_category_id,
                '' as emedia_brand_id,
                '' as mdm_category_id,
                '' as mdm_brand_id,
                '' as mdm_productline_id,
                '' as ali_brand_name,
                click_uv,
                click_pv,
                cast(null as bigint) as alipay_uv,
                cast(null as bigint) as alipay_num,
                cast(null as decimal(20,4)) as alipay_fee,
                dw_source,
                dw_create_time,
                dw_batch_number,
                req_storeId,
                req_group_by_adzone,
                'ods.tmsha_report_daily' as etl_source_table,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from ods.tmsha_report_daily
            """
        )
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.tmsha_report_daily")
        .insertInto("dwd.tmsha_report_daily")
    )
    tmsha_daily_fact_pks = [
        "ad_date",
        "effect",
        "effect_days",
        "position_name",
        "report_level",
        "report_level_id",
        "ali_brand_name",
    ]

    # dwd.tb_media_emedia_tmsha_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_tmsha_daily_fact
        where `report_level` = 'report' 
        """
    )
    tables = ["dwd.tmsha_report_daily"]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("tmsha_report_daily")

    spark.sql(
        f"""
        SELECT
            ad_date,
            ad_format_lv2,
            effect,
            effect_days,
            position_name,
            'report' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id as emedia_category_id,
            emedia_brand_id as emedia_brand_id,
            mdm_category_id as mdm_category_id,
            mdm_brand_id as mdm_brand_id,
            mdm_productline_id,
            ali_brand_name,
            click_pv,
            click_uv,
            alipay_uv,
            alipay_num,
            alipay_fee,
            dw_source,
            dw_create_time,
            dw_batch_number,
            etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        FROM 
            tmsha_report_daily
        """
    ).dropDuplicates(tmsha_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_tmsha_daily_fact"
    )
