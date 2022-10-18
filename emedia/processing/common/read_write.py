from emedia.config.emedia_jd_conf import get_emedia_conf_dict


def Read_Synapse(spark, table_name, query):
  # 获取scope相关信息
  project_name = "media_hc"

  user = dbutils.secrets.get("databrick-secret-scope", "synapsedwuser")
  password = dbutils.secrets.get("databrick-secret-scope", "synapsedwpw")

  # 获取key
  blobKey = "fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(
    dbutils.secrets.get("databrick-secret-scope", "synapseaccountname"))
  # 获取然后拼接 blob 临时路径
  tempDir = r"{0}/{1}/{2}/".format(dbutils.secrets.get("databrick-secret-scope", "synapsedirpath"), project_name,
                                   table_name)
  # 将key配置到环境中
  spark.conf.set(blobKey, dbutils.secrets.get("databrick-secret-scope", "synapsekey"))
  # synapse url
  url = "jdbc:sqlserver://b2bqacne2cdlsynapse01.database.chinacloudapi.cn:1433;database=bimart;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;"

  # 读取synapse示例
  df = spark.read.load(spark=spark, url=url, format="com.databricks.spark.sqldw",
                       forwardSparkAzureStorageCredentials="true", query=query, user=user, password=password,
                       tempDir=tempDir)
  return df


# re = Read_Synapse(spark, 'hc_media_emedia_overview_daily_fact', 'select * from dm.hc_media_emedia_overview_daily_fact')


def Write_To_Synapse(spark,df,SynapseTable):
    # 获取scope相关信息
    project_name = "media_hc"
    table_name = SynapseTable.split('.')[1]
    emedia_conf_dict = get_emedia_conf_dict()
    user = emedia_conf_dict.get('synapsedwuser')
    password = emedia_conf_dict.get('synapsedwpw')
    synapseaccountname = emedia_conf_dict.get('synapseaccountname')
    synapsedirpath = emedia_conf_dict.get('synapsedirpath')
    synapsekey = emedia_conf_dict.get('synapsekey')
    url = emedia_conf_dict.get('url')

    # 获取key
    blobKey = "fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(synapseaccountname)
    # 获取然后拼接 blob 临时路径
    tempDir = r"{0}/{1}/{2}/".format(synapsedirpath, project_name,table_name)
    # 将key配置到环境中
    spark.conf.set(blobKey, synapsekey)

    df.distinct().write.mode("append") \
        .format("com.databricks.spark.sqldw") \
        .option("preActions", "truncate table {0}".format(SynapseTable)) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbTable", SynapseTable) \
        .option("tempDir", tempDir) \
        .save()



# Write_To_Synapse(spark,spark.table('ds.hc_media_emedia_new_customer_lineup_fact'),'dm.hc_media_emedia_new_customer_lineup_fact')

