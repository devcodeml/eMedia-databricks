# coding: utf-8

import sys
from emedia.utils import output_df
from emedia.processing.vip.otd_vip_job import vip_etl

from emedia.processing.jd.jd_gwcd import jd_gwcd_campaign_etl

from emedia.processing.jd.jd_ht import jd_ht_campaign_etl

from emedia.processing.jd.jd_jst import jd_jst_campaign_etl

from emedia.processing.jd.jd_ha import jd_ha_campaign_etl

from emedia.processing.jd.jd_dmp import jd_dmp_campaign_etl

from emedia.processing.jd.jd_sem_adgroup import jd_sem_adgroup_etl

from emedia.processing.jd.jd_sem_keyword import jd_sem_keyword_etl

from emedia.processing.jd.jd_sem_creative import jd_sem_creative_etl

from emedia.processing.jd.jd_sem_campaign import jd_sem_campaign_etl

from emedia.processing.jd.jd_sem_target import jd_sem_target_etl

from emedia.processing.jd.jd_zt import jd_zt_campaign_etl

from emedia.processing.jd.jd_zt_adgroup import jd_zt_adgroup_campaign_etl

from emedia.processing.jd.jd_finance import jd_finance_campaign_etl

from emedia.processing.vip.vip_finance import vip_finance_etl



def emedia_etl(etl_action, airflow_execution_date,run_id):

    # vip 部分
    if etl_action == 'vip_etl':
        vip_etl(airflow_execution_date)

    elif etl_action == 'vip_finance_etl':
        vip_finance_etl(airflow_execution_date)

    # JD 部分
    elif etl_action == 'jd_dmp_campaign_etl':
        jd_dmp_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_finance_campaign_etl':
        jd_finance_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_gwcd_campaign_etl':
        jd_gwcd_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_ha_campaign_etl':
        jd_ha_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_ht_campaign_etl':
        jd_ht_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_jst_campaign_etl':
        jd_jst_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_sem_adgroup_etl':
        jd_sem_adgroup_etl(airflow_execution_date,run_id)

    elif etl_action == 'jd_sem_campaign_etl':
        jd_sem_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_sem_creative_etl':
        jd_sem_creative_etl(airflow_execution_date,run_id)

    elif etl_action == 'jd_sem_keyword_etl':
        jd_sem_keyword_etl(airflow_execution_date,run_id)

    elif etl_action == 'jd_sem_target_etl':
        jd_sem_target_etl(airflow_execution_date,run_id)

    elif etl_action == 'jd_zt_campaign_etl':
        jd_zt_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_zt_adgroup_campaign_etl':
        jd_zt_adgroup_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == 'update_flag':
        # "2022-01-23 22:15:00+00:00"
        output_date = airflow_execution_date[0:10]
        output_date_time = output_date + "T" + airflow_execution_date[11:19]
        output_df.create_blob_by_text(f"{output_date}/flag.txt", output_date_time)
    return 0

if __name__ == '__main__':

    emedia_etl(sys.argv[1],sys.argv[2],sys.argv[3].replace(":", "").replace("+", ""))
