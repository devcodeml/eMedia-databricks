import datetime
import sys
from pyspark.sql import SparkSession
from emedia.common import read_conf
from pyspark.sql.functions import current_date,current_timestamp
from databricks_util.data_processing import data_writer
from azure.storage.blob import BlockBlobService


def create_blob_by_text(full_file_path,text, container_name, account_name, sas_token, endpoint_suffix,encoding = 'utf-8'):
    blob_service = BlockBlobService(account_name=account_name,sas_token=sas_token,endpoint_suffix=endpoint_suffix)
    blob_service.create_blob_from_text(container_name, full_file_path,text,encoding=encoding)

def etl():
    spark = SparkSession.builder.getOrCreate()
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    days_ago912 = (datetime.datetime.now() - datetime.timedelta(days=912)).strftime("%Y-%m-%d")
    date_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    input_blob_container_name = read_conf.get_conf('vipshop', 'input_blob_container_name')
    input_blob_account_name = read_conf.get_conf('vipshop', 'input_blob_account_name')
    input_blob_sas_token = read_conf.get_conf('vipshop', 'input_blob_sas_token')
    input_blob_endpoint_suffix = read_conf.get_conf('vipshop', 'input_blob_endpoint_suffix')

    otd_vip_input_path = read_conf.get_conf('vipshop', 'otd_vip_input_path')
    otd_vip_output_path = read_conf.get_conf('vipshop', 'otd_vip_output_path').format(date, date_time)
    output_temp_path = read_conf.get_conf('vipshop', 'output_temp_path')

    mapping_blob_container_name = read_conf.get_conf('vipshop', 'mapping_blob_container_name')
    mapping_blob_account_name = read_conf.get_conf('vipshop', 'mapping_blob_account_name')
    mapping_blob_sas_token = read_conf.get_conf('vipshop', 'mapping_blob_sas_token')
    mapping_blob_endpoint_suffix = read_conf.get_conf('vipshop', 'mapping_blob_endpoint_suffix')

    otd_vip_mapping1_path = read_conf.get_conf('vipshop', 'otd_vip_mapping1_path')
    otd_vip_mapping2_path = read_conf.get_conf('vipshop', 'otd_vip_mapping2_path')
    otd_vip_mapping3_path = read_conf.get_conf('vipshop', 'otd_vip_mapping3_path')

    target_blob_container_name = read_conf.get_conf('vipshop', 'target_blob_container_name')
    target_blob_account_name = read_conf.get_conf('vipshop', 'target_blob_account_name')
    target_blob_sas_token = read_conf.get_conf('vipshop', 'target_blob_sas_token')
    target_blob_endpoint_suffix = read_conf.get_conf('vipshop', 'target_blob_endpoint_suffix')

    spark.conf.set(f"fs.azure.sas.{input_blob_container_name}.{input_blob_account_name}.blob.{input_blob_endpoint_suffix}",input_blob_sas_token)
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container_name}.{mapping_blob_account_name}.blob.{mapping_blob_endpoint_suffix}",mapping_blob_sas_token)
    spark.conf.set(f"fs.azure.sas.{target_blob_container_name}.{target_blob_account_name}.blob.{target_blob_endpoint_suffix}",target_blob_sas_token)

    vip_report_df = spark.read.csv(f"wasbs://{input_blob_container_name}@{input_blob_account_name}.blob.{input_blob_endpoint_suffix}/{otd_vip_input_path}",
        header=True, multiLine=True, sep="|")
    fail_df = spark.table("stg.tb_emedia_vip_otd_mapping_fail")
    vip_report_df.union(fail_df).createOrReplaceTempView("daily_reports")
    vip_report_df.cache()

    mapping1_df = spark.read.csv(
        f"wasbs://{mapping_blob_container_name}@{mapping_blob_account_name}.blob.{mapping_blob_endpoint_suffix}/{otd_vip_mapping1_path}",
        header=True, multiLine=True, sep="=")
    mapping1_df.createOrReplaceTempView("mapping1")

    mapping2_df = spark.read.csv(
        f"wasbs://{mapping_blob_container_name}@{mapping_blob_account_name}.blob.{mapping_blob_endpoint_suffix}/{otd_vip_mapping2_path}",
        header=True, multiLine=True, sep="=")
    mapping2_df.createOrReplaceTempView("mapping2")

    mapping3_df = spark.read.csv(
        f"wasbs://{mapping_blob_container_name}@{mapping_blob_account_name}.blob.{mapping_blob_endpoint_suffix}/{otd_vip_mapping3_path}",
        header=True, multiLine=True, sep="=")
    mapping3_df.createOrReplaceTempView("mapping3")

    mappint1_result_df = spark.sql(
        "select dr.*,m1.category_id,m1.brand_id from daily_reports dr left join mapping1 m1 on dr.req_advertiser_id = m1.account_id")
    mappint1_result_df.cache()

    mappint1_result_df.filter("category_id is null and brand_id is null").drop("category_id").drop(
        "brand_id").createOrReplaceTempView("mappint_fail_1")
    mappint1_result_df.filter("category_id is not null or brand_id is not null").createOrReplaceTempView(
        "mappint_success_1")

    mappint2_result_df = spark.sql(
        "select mfr1.*,m2.category_id,m2.brand_id from mappint_fail_1 mfr1 left join mapping2 m2 on mfr1.req_advertiser_id = m2.account_id and  instr(mfr1.campaign_title, m2.keyword) > 0")

    mappint2_result_df.filter("category_id is null and brand_id is null").drop("category_id").drop(
        "brand_id").createOrReplaceTempView("mappint_fail_2")
    mappint2_result_df.filter("category_id is not null or brand_id is not null").createOrReplaceTempView(
        "mappint_success_2")

    mappint3_result_df = spark.sql(
        "select mfr2.*,m3.category_id,m3.brand_id from mappint_fail_2 mfr2 left join mapping3 m3 on mfr2.req_advertiser_id = m3.account_id and instr(mfr2.campaign_title, m3.keyword) > 0")

    mappint3_result_df.filter("category_id is null and brand_id is null").drop("category_id").drop(
        "brand_id").createOrReplaceTempView("mappint_fail_3")
    mappint3_result_df.filter("category_id is not null or brand_id is not null").createOrReplaceTempView(
        "mappint_success_3")

    out1 = spark.table("mappint_success_1").union(spark.table("mappint_success_2")).union(
        spark.table("mappint_success_3")).withColumn("etl_date", current_date()).withColumn("etl_create_time",
                                                                                            current_timestamp())
    out1.createOrReplaceTempView("all_mappint_success")

    spark.sql("""
    MERGE INTO dws.tb_emedia_vip_otd_mapping_success
    USING all_mappint_success
    ON dws.tb_emedia_vip_otd_mapping_success.date = all_mappint_success.date 
        and dws.tb_emedia_vip_otd_mapping_success.req_advertiser_id = all_mappint_success.req_advertiser_id 
        and dws.tb_emedia_vip_otd_mapping_success.campaign_id = all_mappint_success.campaign_id and (dws.tb_emedia_vip_otd_mapping_success.ad_id = all_mappint_success.ad_id or (dws.tb_emedia_vip_otd_mapping_success.ad_id is null and  all_mappint_success.ad_id is null ))
        and dws.tb_emedia_vip_otd_mapping_success.effect = all_mappint_success.effect 
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED
      THEN INSERT *
    """)

    spark.table("mappint_fail_3").write.mode("overwrite").insertInto("stg.tb_emedia_vip_otd_mapping_fail")

    tb_emedia_vip_otd_ad_fact_df = spark.sql(f"""select date_format(date, 'yyyyMMdd') as ad_date ,
                        req_advertiser_id as store_id,
                        '' as account_name ,
                        campaign_id,
                        campaign_title as campaign_name,
                        ad_id as adgroup_id,
                        ad_title as adgroup_name,
                        category_id,
                        brand_id,
                        impression as impressions,
                        click as clicks,
                        cost,
                        app_waken_uv,
                        cost_per_app_waken_uv,
                        app_waken_pv,
                        app_waken_rate,
                        miniapp_uv,
                        app_uv,
                        cost_per_app_uv,
                        cost_per_miniapp_uv,
                        general_uv,
                        product_uv,
                        special_uv,
                        effect,
                        case effect  when '1' then book_customer_in_24hour when '14' then book_customer_in_14day end as book_customer,
                        case effect  when '1' then new_customer_in_24hour when '14' then new_customer_in_14day end as new_customer,
                        case effect  when '1' then customer_in_24hour when '14' then customer_in_14day end as customer,
                        case effect  when '1' then book_sales_in_24hour when '14' then book_sales_in_14day end as book_sales,
                        case effect  when '1' then sales_in_24hour when '14' then sales_in_14day end as order_value,
                        case effect  when '1' then book_orders_in_24hour when '14' then book_orders_in_14day end as book_orders,
                        case effect  when '1' then orders_in_24hour when '14' then orders_in_14day end as order_quantity,
                        dw_resource as data_source,
                        dw_create_time as dw_etl_date,
                        dw_batch_number as dw_batch_id,
                        channel from dws.tb_emedia_vip_otd_mapping_success where req_level = "REPORT_LEVEL_AD" and date >= '{days_ago912}' and date <= '{date}'""").coalesce(1)
    ad_output_temp_path = output_temp_path + "ad"

    tb_emedia_vip_otd_campaign_fact_df = spark.sql(f"""select 
                        date_format(date, 'yyyyMMdd') as ad_date ,
                        req_advertiser_id as store_id,
                        '' as account_name,
                        campaign_id as campaign_id,
                        campaign_title as campaign_name,
                        category_id,
                        brand_id,
                        impression as impressions,
                        click as clicks,
                        cost,
                        app_waken_uv,
                        cost_per_app_waken_uv,
                        app_waken_pv,
                        app_waken_rate,
                        miniapp_uv,
                        app_uv,
                        cost_per_app_uv,
                        cost_per_miniapp_uv,
                        general_uv,
                        product_uv,
                        special_uv,
                        effect,
                        case effect  when '1' then book_customer_in_24hour when '14' then book_customer_in_14day end as book_customer,
                        case effect  when '1' then new_customer_in_24hour when '14' then new_customer_in_14day end as new_customer,
                        case effect  when '1' then customer_in_24hour when '14' then customer_in_14day end as customer,
                        case effect  when '1' then book_sales_in_24hour when '14' then book_sales_in_14day end as book_sales,
                        case effect  when '1' then sales_in_24hour when '14' then sales_in_14day end as order_value,
                        case effect  when '1' then book_orders_in_24hour when '14' then book_orders_in_14day end as book_orders,
                        case effect  when '1' then orders_in_24hour when '14' then orders_in_14day end as order_quantity,
                        dw_resource as data_source,
                        dw_create_time as dw_etl_date,
                        dw_batch_number as dw_batch_id,
                        channel from dws.tb_emedia_vip_otd_mapping_success where req_level = "REPORT_LEVEL_CAMPAIGN" and date >= '{days_ago912}' and date <= '{date}'""").coalesce(1)
    campaign_output_temp_path = output_temp_path + "campaign"

    #ad_output_temp_path write
    data_writer.write_to_blob(tb_emedia_vip_otd_ad_fact_df,
                              "wasbs://{}@{}.blob.{}/{}".format(target_blob_container_name, target_blob_account_name,
                                                                target_blob_endpoint_suffix, ad_output_temp_path),
                              mode="overwrite", format="csv", header=True, sep="\001")
    data_writer.delete_blob_mark_file(ad_output_temp_path, target_blob_container_name, target_blob_account_name,
                                      target_blob_sas_token, target_blob_endpoint_suffix)
    ad_flag = data_writer.rename_blob_file(ad_output_temp_path, "part", otd_vip_output_path, "TB_EMEDIA_VIP_OTD_AD_FACT.CSV",
                                 target_blob_container_name, target_blob_account_name, target_blob_sas_token,
                                 target_blob_endpoint_suffix)

    # campaign_output_temp_path write
    data_writer.write_to_blob(tb_emedia_vip_otd_campaign_fact_df,
                              "wasbs://{}@{}.blob.{}/{}".format(target_blob_container_name, target_blob_account_name,
                                                                target_blob_endpoint_suffix, campaign_output_temp_path),
                              mode="overwrite", format="csv", header=True, sep="\001")
    data_writer.delete_blob_mark_file(campaign_output_temp_path, target_blob_container_name, target_blob_account_name,
                                      target_blob_sas_token, target_blob_endpoint_suffix)
    campaign_flag = data_writer.rename_blob_file(campaign_output_temp_path, "part", otd_vip_output_path,
                                 "TB_EMEDIA_VIP_OTD_CAMPAIGN_FACT.CSV",
                                 target_blob_container_name, target_blob_account_name, target_blob_sas_token,
                                 target_blob_endpoint_suffix)
    if ad_flag and campaign_flag:
        create_blob_by_text("dbr_etl_brand_mapping/flag.txt",date_time,target_blob_container_name,target_blob_account_name,target_blob_sas_token,target_blob_endpoint_suffix)

if __name__ == '__main__':
    etl()
