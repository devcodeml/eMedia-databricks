# coding: utf-8
from emedia import get_spark
# from emedia.config.emedia_conf import get_emedia_conf_dict
from mdl_emedia_etl_dbs.config.mdl_conf import get_emedia_conf_dict
from emedia.utils import output_df

spark = get_spark()


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


def push_to_dw(dataframe, dw_table_name, model, table_name):
    project_name = "emedia"
    emedia_conf_dict = get_emedia_conf_dict()
    user = emedia_conf_dict.get("dwwriteuser")
    password = emedia_conf_dict.get("dwwritepassword")
    synapseaccountname = emedia_conf_dict.get("synapseaccountname")
    synapsedirpath = emedia_conf_dict.get("synapsedirpath")
    synapsekey = emedia_conf_dict.get("synapsekey")
    url = emedia_conf_dict.get("dwurl")
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
        "dbTable", dw_table_name
    ).option(
        "tempDir", tempDir
    ).save()

    return 0


def push_table_to_dw():
    # 推送数据到dw

    model = "overwrite"

    gwcd_campaign_df = spark.table("dwd.gwcd_campaign_daily").distinct()
    gwcd_campaign_dw_table = "dbo.tb_emedia_jd_gwcd_campaign_daily_v202209_fact"
    push_to_dw(gwcd_campaign_df, gwcd_campaign_dw_table, model, "gwcd_campaign_daily")
