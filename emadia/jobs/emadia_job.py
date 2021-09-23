from pyspark.sql import SparkSession
import datetime
import sys

def etl():
    spark = SparkSession.builder.getOrCreate()

    date = datetime.datetime.now().strftime("%Y-%m-%d")

    test = spark.read.csv("dbfs:/test/vip-otd_getDailyReports_2021-09-10.csv", header=True, sep="|")
    test.createOrReplaceTempView("daily_reports")

    table1 = spark.sql("select *,current_date() as etl_date,now() as etl_create_time from daily_reports")
    spark.sql("delete from stg.e_media_vip_otd_daily_reports where etl_date = '{}'".format(date))
    table1.write.format("delta").insertInto("stg.e_media_vip_otd_daily_reports")

    result_df = spark.sql("""select t.*,t2.category,t2.brand,t2.category_id,t2.brand_id from stg.e_media_vip_otd_daily_reports t inner join stg.e_media_brand_mapping t2 on t.req_advertiser_id = t2.advertiser_id
    where t2.advertiser_type = "eMedia-单品类单品牌店" or (t2.advertiser_type = "eMedia-单品类多品牌店" and instr(t.campaign_title, t2.campaign_title) = 1)""")

    result_df.write.insertInto("dws.e_media_vip_otd_mappint_reports")

    result_df.write.csv("dbfs:/test/mapping_reports.csv", header=True, sep="|")


if __name__ == '__main__':
    etl()