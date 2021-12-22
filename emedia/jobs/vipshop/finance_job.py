from pyspark.sql import SparkSession
import datetime
from emedia.common import read_conf
from databricks_util.data_processing import data_writer

def etl():
    spark = SparkSession.builder.getOrCreate()

    date = datetime.datetime.now().strftime("%Y-%m-%d")
    input_blob_container_name = read_conf.get_conf('vipshop', 'input_blob_container_name')
    input_blob_account_name = read_conf.get_conf('vipshop', 'input_blob_account_name')
    input_blob_sas_token = read_conf.get_conf('vipshop', 'input_blob_sas_token')
    input_blob_endpoint_suffix = read_conf.get_conf('vipshop', 'input_blob_endpoint_suffix')

    finance_input_path = read_conf.get_conf('vipshop', 'finance_input_path')
    finance_output_path = read_conf.get_conf('vipshop', 'finance_output_path')

    target_blob_container_name = read_conf.get_conf('vipshop', 'input_blob_container_name')
    target_blob_account_name = read_conf.get_conf('vipshop', 'input_blob_account_name')
    target_blob_sas_token = read_conf.get_conf('vipshop', 'input_blob_sas_token')
    target_blob_endpoint_suffix = read_conf.get_conf('vipshop', 'input_blob_endpoint_suffix')

    spark.conf.set(f"fs.azure.sas.{input_blob_container_name}.{input_blob_account_name}.blob.{input_blob_endpoint_suffix}",f"?{input_blob_sas_token}")

    soureDf = spark.read.csv(
        f"wasbs://{input_blob_container_name}@{input_blob_account_name}.blob.{input_blob_endpoint_suffix}/{finance_input_path}",
        header=True, multiLine=True, sep="|")
    soureDf.createOrReplaceTempView("finance_reports")

    financeReportsDf = spark.sql("select *,current_date() as etl_date,now() as etl_create_time from finance_reports")

    spark.sql("delete from stg.e_media_vip_otd_finance_record where etl_date = '{}'".format(date))

    financeReportsDf.write.format("delta").insertInto("stg.e_media_vip_otd_finance_record")

    result_df = spark.sql("""select
      t.*,
      t2.category,
      t2.brand,
      t2.category_id,
      t2.brand_id
    from
      stg.e_media_vip_otd_finance_record t
      inner join stg.e_media_brand_mapping t2 on t.req_advertiser_id = t2.advertiser_id """)

    result_df.write.insertInto("dws.e_media_vip_otd_finance_record")

    # data_writer.write_to_blob(result_df,"wasbs://{}@{}.blob.{}/"
    #                           .format(target_blob_container_name, target_blob_account_name, target_blob_endpoint_suffix) + date
    #                           + "/DOUYIN/report/ad/",mode="overwrite", format="csv", header=True, sep="|")
    #
    data_writer.delete_blob_mark_file(date + "/DOUYIN/report/ad/", target_blob_container_name, target_blob_account_name,
                                      target_blob_sas_token, target_blob_endpoint_suffix)
    #
    # data_writer.rename_blob_file(date + "/DOUYIN/report/ad/", date + "/DOUYIN/report/ad/daily_phase_origin.csv",
    #                  target_blob_container_name, target_blob_account_name, target_blob_sas_token,
    #                  target_blob_endpoint_suffix)


if __name__ == '__main__':
    etl()
