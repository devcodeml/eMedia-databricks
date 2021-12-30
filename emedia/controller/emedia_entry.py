# coding: utf-8


from emedia.processing.vip.otd_vip_job import vip_etl

from emedia.processing.jd.jd_gwcd import jd_gwcd_campaign_etl

from emedia.processing.jd.jd_ht import jd_ht_campaign_etl

from emedia.processing.jd.jd_jst import jd_jst_campaign_etl

def emedia_etl(etl_action, airflow_execution_date):

    if etl_action == 'vip_etl':
        vip_etl()

    elif etl_action == 'jd_gwcd_campaign_etl':
        jd_gwcd_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_ht_campaign_etl':
        jd_ht_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_jst_campaign_etl':
        jd_jst_campaign_etl(airflow_execution_date)

    return 0

