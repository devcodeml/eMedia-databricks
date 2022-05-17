# coding: utf-8

import sys

from emedia.processing.jd.jd_jst_daily_search import jd_jst_daily_search_etl
from emedia.processing.tmall.tmall_super_new_effect import tmall_super_new_effect_etl
from emedia.processing.tmall.tmall_super_new_report import tmall_super_new_report_etl
from emedia.processing.tmall.tmall_ylmf_adzone import tmall_ylmf_daliy_adzone_etl
from emedia.processing.tmall.tmall_ylmf_campaign import tmall_ylmf_campaign_etl
from emedia.processing.tmall.tmall_ylmf_campaign_group import tmall_ylmf_campaign_group_etl
from emedia.processing.tmall.tmall_ylmf_creativepackage import tmall_ylmf_daliy_creativepackage_etl
from emedia.processing.tmall.tmall_ylmf_crowd import tmall_ylmf_daliy_crowd_etl
from emedia.processing.tmall.tmall_ylmf_promotion import tmall_ylmf_daliy_promotion_etl
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

from emedia.processing.tmall.tmall_super_effect import tmall_super_effect_etl
from emedia.processing.tmall.tmall_super_report import tmall_super_report_etl
from emedia.processing.tmall.tmall_mxdp import tmall_mxdp_etl
from emedia.processing.tmall.tmall_pptx import tmall_pptx_etl
from emedia.processing.tmall.tmall_ppzq import tmall_ppzq_etl

from emedia.processing.tmall.tmall_ztc_account import tmall_ztc_account_etl
from emedia.processing.tmall.tmall_ztc_adgroup import tmall_ztc_adgroup_etl
from emedia.processing.tmall.tmall_ztc_campaign import tmall_ztc_campaign_etl
from emedia.processing.tmall.tmall_ztc_creative import tmall_ztc_creative_etl
from emedia.processing.tmall.tmall_ztc_keyword import tmall_ztc_keyword_etl
from emedia.processing.tmall.tmall_ztc_target import tmall_ztc_target_etl

from emedia.processing.tmall.tmall_ztc_cumul_adgroup import tmall_ztc_cumul_adgroup_etl
from emedia.processing.tmall.tmall_ztc_cumul_campaign import tmall_ztc_cumul_campaign_etl
from emedia.processing.tmall.tmall_ztc_cumul_creative import tmall_ztc_cumul_creative_etl
from emedia.processing.tmall.tmall_ztc_cumul_keyword import tmall_ztc_cumul_keyword_etl
from emedia.processing.tmall.tmall_ztc_cumul_target import tmall_ztc_cumul_target_etl


def emedia_etl(etl_action, airflow_execution_date, run_id):
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
        jd_sem_adgroup_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_sem_campaign_etl':
        jd_sem_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_sem_creative_etl':
        jd_sem_creative_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_sem_keyword_etl':
        jd_sem_keyword_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_sem_target_etl':
        jd_sem_target_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_zt_campaign_etl':
        jd_zt_campaign_etl(airflow_execution_date)

    elif etl_action == 'jd_zt_adgroup_campaign_etl':
        jd_zt_adgroup_campaign_etl(airflow_execution_date)

    # tmall 部分
    elif etl_action == 'tmall_super_effect_etl':
        tmall_super_effect_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_super_report_etl':
        tmall_super_report_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_mxdp_etl':
        tmall_mxdp_etl(airflow_execution_date)

    elif etl_action == 'tmall_pptx_etl':
        tmall_pptx_etl(airflow_execution_date)

    elif etl_action == 'tmall_ppzq_etl':
        tmall_ppzq_etl(airflow_execution_date)

    elif etl_action == 'tmall_ztc_account_etl':
        tmall_ztc_account_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_adgroup_etl':
        tmall_ztc_adgroup_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_campaign_etl':
        tmall_ztc_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_creative_etl':
        tmall_ztc_creative_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_keyword_etl':
        tmall_ztc_keyword_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_target_etl':
        tmall_ztc_target_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_cumul_adgroup_etl':
        tmall_ztc_cumul_adgroup_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_cumul_campaign_etl':
        tmall_ztc_cumul_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_cumul_creative_etl':
        tmall_ztc_cumul_creative_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_cumul_keyword_etl':
        tmall_ztc_cumul_keyword_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ztc_cumul_target_etl':
        tmall_ztc_cumul_target_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ylmf_daliy_adzone_etl':
        tmall_ylmf_daliy_adzone_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ylmf_campaign_etl':
        tmall_ylmf_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ylmf_campaign_group_etl':
        tmall_ylmf_campaign_group_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ylmf_daliy_creativepackage_etl':
        tmall_ylmf_daliy_creativepackage_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ylmf_daliy_crowd_etl':
        tmall_ylmf_daliy_crowd_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_ylmf_daliy_promotion_etl':
        tmall_ylmf_daliy_promotion_etl(airflow_execution_date, run_id)

    elif etl_action == 'jd_jst_daily_search_etl':
        jd_jst_daily_search_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_super_new_effect_etl':
        tmall_super_new_effect_etl(airflow_execution_date, run_id)

    elif etl_action == 'tmall_super_new_report_etl':
        tmall_super_new_report_etl(airflow_execution_date, run_id)

    elif etl_action == 'update_cumul_flag':
        # "2022-01-23 22:15:00+00:00"
        output_date = airflow_execution_date[0:10]
        output_date_time = output_date + "T" + airflow_execution_date[11:19]
        output_df.create_blob_by_text(f"{output_date}/flag.txt", output_date_time, 'cumul')

    elif etl_action == 'update_target_flag':
        # "2022-01-23 22:15:00+00:00"
        output_date = airflow_execution_date[0:10]
        output_date_time = output_date + "T" + airflow_execution_date[11:19]
        output_df.create_blob_by_text(f"{output_date}/flag.txt", output_date_time, 'target')
    return 0


if __name__ == '__main__':
    emedia_etl(sys.argv[1], sys.argv[2], sys.argv[3].replace(":", "").replace("+", "").replace(".", ""))
