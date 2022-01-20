# coding: utf-8


from emedia.processing.vip.otd_vip_job import vip_etl

from emedia.processing.jd.jd_gwcd import jd_gwcd_campaign_etl

from emedia.processing.jd.jd_ht import jd_ht_campaign_etl

from emedia.processing.jd.jd_jst import jd_jst_campaign_etl

from emedia.processing.jd.jd_ha import jd_ha_campaign_etl

from emedia.processing.jd.jd_dmp import jd_dmp_campaign_etl

from emedia.processing.jd.jd_sem_adgroup import jd_sem_adgroup_etl

from emedia.processing.jd.jd_sem_keyword import jd_sem_keyword_etl

from emedia.processing.jd.jd_zt import jd_zt_campaign_etl

from emedia.processing.jd.jd_sem_creative import jd_sem_creative_etl

from emedia.processing.jd.jd_sem_target import jd_sem_target_etl

from emedia.processing.vip.vip_finance import vip_finance_etl



def emedia_etl(etl_action, airflow_execution_date):

    if etl_action == 'vip_etl':
        vip_etl()

    elif etl_action == 'vip_finance_etl':
        vip_finance_etl(airflow_execution_date)

    elif etl_action == 'jd_gwcd_campaign_etl':
        jd_gwcd_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_ht_campaign_etl':
        jd_ht_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_jst_campaign_etl':
        jd_jst_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_ha_campaign_etl':
        jd_ha_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_dmp_campaign_etl':
        jd_dmp_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_sem_adgroup_etl':
        jd_sem_adgroup_etl(airflow_execution_date)

    elif etl_action == 'jd_sem_keyword_etl':
        jd_sem_keyword_etl(airflow_execution_date)

    elif etl_action == 'jd_sem_creative_etl':
        jd_sem_creative_etl(airflow_execution_date)

    elif etl_action == 'jd_sem_target_etl':
        jd_sem_target_etl(airflow_execution_date)

    elif etl_action == 'jd_zt_campaign_etl':
        jd_zt_campaign_etl(airflow_execution_date)

    return 0

