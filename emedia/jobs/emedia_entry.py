# coding: utf-8

import sys
import datetime
from emedia.processing.jd.jd_dmp import jd_dmp_campaign_etl
from emedia.processing.jd.jd_finance import jd_finance_campaign_etl
from emedia.processing.jd.jd_gwcd import jd_gwcd_campaign_etl
from emedia.processing.jd.jd_ha import jd_ha_campaign_etl
from emedia.processing.jd.jd_ht import jd_ht_campaign_etl
from emedia.processing.jd.jd_jst import jd_jst_campaign_etl
from emedia.processing.jd.jd_jst_daily_search import jd_jst_daily_search_etl
from emedia.processing.jd.jd_sem_adgroup import jd_sem_adgroup_etl
from emedia.processing.jd.jd_sem_campaign import jd_sem_campaign_etl
from emedia.processing.jd.jd_sem_creative import jd_sem_creative_etl
from emedia.processing.jd.jd_sem_keyword import jd_sem_keyword_etl
from emedia.processing.jd.jd_sem_target import jd_sem_target_etl
from emedia.processing.jd.jd_zt import jd_zt_campaign_etl
from emedia.processing.jd.jd_zt_adgroup import jd_zt_adgroup_campaign_etl
from emedia.processing.jd_new.jd_gwcd_adgroup import jd_gwcd_adgroup_etl
from emedia.processing.jd_new.jd_gwcd_campaign import jd_gwcd_campaign_etl_new
from emedia.processing.jd_new.jd_jst_campaign import jd_jst_campaign_etl_new
from emedia.processing.jd_new.jd_jst_search import jd_jst_search_etl_new
from emedia.processing.jd_new.jd_sem_adgroup_daily import jdkc_adgroup_daily_etl
from emedia.processing.jd_new.jd_sem_campaign_daily import jdkc_campaign_daily_etl
from emedia.processing.jd_new.jd_sem_creative_daily import jdkc_creative_daily_etl
from emedia.processing.jd_new.jd_sem_keyword_daily import jdkc_keyword_daily_etl
from emedia.processing.jd_new.jd_zt_account_daily import jdzt_account_daily_etl
from emedia.processing.jd_new.jd_zt_adgroup_daily import jdzt_adgroup_daily_etl
from emedia.processing.jd_new.jd_zw_campaign import jd_zw_campaign_etl
from emedia.processing.jd_new.jd_zw_creative import jd_zw_creative_etl
from emedia.processing.jd_new.mapping import emedia_sem_audience_mapping
from emedia.processing.jd_new.push_data_synapse import push_data_synapse
from emedia.processing.jd_new.push_to_dw import push_table_to_dw
from emedia.processing.jd_new.tb_media_emedia_jdkc_daily import jdkc_daily_fact
from emedia.processing.tmall.tmail_wxt_day_campaign import tamll_wxt_day_campaign_etl
from emedia.processing.tmall.tmall_mxdp import tmall_mxdp_etl
from emedia.processing.tmall.tmall_pptx import tmall_pptx_etl
from emedia.processing.tmall.tmall_ppzq import tmall_ppzq_etl
from emedia.processing.tmall.tmall_super_effect import tmall_super_effect_etl
from emedia.processing.tmall.tmall_super_new_effect import tmall_super_new_effect_etl
from emedia.processing.tmall.tmall_super_new_report import tmall_super_new_report_etl
from emedia.processing.tmall.tmall_super_report import tmall_super_report_etl
from emedia.processing.tmall.tmall_ylmf_adzone import tmall_ylmf_daliy_adzone_etl
from emedia.processing.tmall.tmall_ylmf_campaign import tmall_ylmf_campaign_etl
from emedia.processing.tmall.tmall_ylmf_campaign_group import (
    tmall_ylmf_campaign_group_etl,
)
from emedia.processing.tmall.tmall_ylmf_creativepackage import (
    tmall_ylmf_daliy_creativepackage_etl,
)
from emedia.processing.tmall.tmall_ylmf_crowd import tmall_ylmf_daliy_crowd_etl
from emedia.processing.tmall.tmall_ylmf_promotion import tmall_ylmf_daliy_promotion_etl
from emedia.processing.tmall.tmall_ztc_account import tmall_ztc_account_etl
from emedia.processing.tmall.tmall_ztc_adgroup import tmall_ztc_adgroup_etl
from emedia.processing.tmall.tmall_ztc_campaign import tmall_ztc_campaign_etl
from emedia.processing.tmall.tmall_ztc_creative import tmall_ztc_creative_etl
from emedia.processing.tmall.tmall_ztc_cumul_adgroup import tmall_ztc_cumul_adgroup_etl
from emedia.processing.tmall.tmall_ztc_cumul_campaign import (
    tmall_ztc_cumul_campaign_etl,
)
from emedia.processing.tmall.tmall_ztc_cumul_creative import (
    tmall_ztc_cumul_creative_etl,
)
from emedia.processing.tmall.tmall_ztc_cumul_keyword import tmall_ztc_cumul_keyword_etl
from emedia.processing.tmall.tmall_ztc_cumul_target import tmall_ztc_cumul_target_etl
from emedia.processing.tmall.tmall_ztc_keyword import tmall_ztc_keyword_etl
from emedia.processing.tmall.tmall_ztc_target import tmall_ztc_target_etl
from emedia.processing.tmall.ylmf_cumul.tamll_ylmf_campaign_cumul import (
    tmall_ylmf_campaign_cumul_etl,
)
from emedia.processing.tmall.ylmf_cumul.tmall_ylmf_adzone_cumul import (
    tmall_ylmf_daliy_adzone_cumul_etl,
)
from emedia.processing.tmall.ylmf_cumul.tmall_ylmf_campaign_group_cumul import (
    tmall_ylmf_campaign_group_cumul_etl,
)
from emedia.processing.tmall.ylmf_cumul.tmall_ylmf_creativepackage_cumul import (
    tmall_ylmf_daliy_creativepackage_cumul_etl,
)
from emedia.processing.tmall.ylmf_cumul.tmall_ylmf_crowd_cumul import (
    tmall_ylmf_daliy_crowd_cumul_etl,
)
from emedia.processing.tmall.ylmf_cumul.tmall_ylmf_promotion_cumul import (
    tmall_ylmf_daliy_promotion_cumul_etl,
)
from emedia.processing.vip.otd_vip_job import vip_etl
from emedia.processing.vip.vip_finance import vip_finance_etl
from emedia.utils import output_df
from emedia.processing.jd_new.jd_act_channel_daily import jd_act_channel_daily_etl
from emedia.processing.jd_new.jd_act_flow_source_daily import jd_act_flow_source_daily_etl
from emedia.processing.jd_new.jd_pit_data_daily import jd_pit_data_daily_etl
from emedia.processing.jd_new.jd_propit_daily import jd_propit_daily_etl
from emedia.processing.jd_new.jd_seckill_daily import jd_seckill_daily_etl
from emedia.processing.jd_new.jd_ticket_daily import jd_ticket_daily_etl
from emedia.processing.jd_new.tb_media_emedia_jd_seckill_daily_fact import jd_seckill_daily_fact
from emedia.processing.jd_new.tb_media_emedia_jst_daily_fact import jd_jst_daily_fact
from emedia.processing.jd_new.tb_media_emedia_gwcd_daily_fact import jd_gwcd_daily_fact


def emedia_etl(etl_action, airflow_execution_date, run_id):
    # vip 部分
    if etl_action == "vip_etl":
        vip_etl(airflow_execution_date)

    elif etl_action == "vip_finance_etl":
        vip_finance_etl(airflow_execution_date)

    # JD 部分
    # elif etl_action == "jd_dmp_campaign_etl":
        # jd_dmp_campaign_etl(airflow_execution_date)
    elif etl_action == "jd_zw_campaign_etl":
        jd_zw_campaign_etl(airflow_execution_date, run_id)
    elif etl_action == "jd_zw_creative_etl":
        emedia_sem_audience_mapping()
        # jd_zw_creative_etl(airflow_execution_date, run_id)
        jd_dmp_campaign_etl(airflow_execution_date)

    elif etl_action == "jd_finance_campaign_etl":
        jd_finance_campaign_etl(airflow_execution_date)

    elif etl_action == "jd_gwcd_adgroup_etl":
        # jd_gwcd_campaign_etl(airflow_execution_date)
        jd_gwcd_adgroup_etl(airflow_execution_date, run_id)
    elif etl_action == "jd_gwcd_campaign_etl":
        jd_gwcd_campaign_etl_new(airflow_execution_date, run_id)

    elif etl_action == "jd_ha_campaign_etl":
        jd_ha_campaign_etl(airflow_execution_date)

    # elif etl_action == "jd_ht_campaign_etl":
    #     jd_ht_campaign_etl(airflow_execution_date)

    elif etl_action == "jd_jst_campaign_etl":
        # jd_jst_campaign_etl(airflow_execution_date)
        jd_jst_campaign_etl_new(airflow_execution_date, run_id)

    elif etl_action == "jd_jst_daily_search_etl":
        # jd_jst_daily_search_etl(airflow_execution_date, run_id)
        jd_jst_search_etl_new(airflow_execution_date, run_id)

    elif etl_action == "jd_sem_adgroup_etl":
        # jd_sem_adgroup_etl(airflow_execution_date, run_id)
        jdkc_adgroup_daily_etl(airflow_execution_date, run_id)
    elif etl_action == "jd_sem_campaign_etl":
        # jd_sem_campaign_etl(airflow_execution_date, run_id)
        jdkc_campaign_daily_etl(airflow_execution_date, run_id)
    elif etl_action == "jd_sem_creative_etl":
        # jd_sem_creative_etl(airflow_execution_date, run_id)
        jdkc_creative_daily_etl(airflow_execution_date, run_id)

    elif etl_action == "jd_sem_keyword_etl":
        # jd_sem_keyword_etl(airflow_execution_date, run_id)
        jdkc_keyword_daily_etl(airflow_execution_date, run_id)

    elif etl_action == "jd_sem_target_etl":
        jd_sem_target_etl(airflow_execution_date, run_id)

    elif etl_action == "jdkc_daily_fact":
        jdkc_daily_fact()
    elif etl_action == "push_data_synapse":
        push_data_synapse()
        # day_date = airflow_execution_date[0:10]
        # time_flag = datetime.datetime.strptime(
        #     day_date + ' 14:30:00', "%Y-%m-%d %H:%M:%S"
        # )
        # time_airflow = datetime.datetime.strptime(
        #     day_date + ' ' + airflow_execution_date[11:19], "%Y-%m-%d %H:%M:%S"
        # )
        # if time_airflow >= time_flag:
        #     push_data_synapse()
    # elif etl_action == "jd_zt_campaign_etl":
    #     jd_zt_campaign_etl(airflow_execution_date)
    #
    # elif etl_action == "jd_zt_adgroup_campaign_etl":
    #     jd_zt_adgroup_campaign_etl(airflow_execution_date)

    elif etl_action == "jdzt_account_daily_etl":
        jdzt_account_daily_etl(airflow_execution_date, run_id)
    elif etl_action == "jdzt_adgroup_daily_etl":
        jdzt_adgroup_daily_etl(airflow_execution_date, run_id)

    # tmall 部分
    elif etl_action == "tmall_super_effect_etl":
        tmall_super_effect_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_super_report_etl":
        tmall_super_report_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_mxdp_etl":
        tmall_mxdp_etl(airflow_execution_date)

    elif etl_action == "tmall_pptx_etl":
        tmall_pptx_etl(airflow_execution_date)

    elif etl_action == "tmall_ppzq_etl":
        tmall_ppzq_etl(airflow_execution_date)

    elif etl_action == "tmall_ztc_account_etl":
        tmall_ztc_account_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_adgroup_etl":
        tmall_ztc_adgroup_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_campaign_etl":
        tmall_ztc_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_creative_etl":
        tmall_ztc_creative_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_keyword_etl":
        tmall_ztc_keyword_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_target_etl":
        tmall_ztc_target_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_cumul_adgroup_etl":
        tmall_ztc_cumul_adgroup_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_cumul_campaign_etl":
        tmall_ztc_cumul_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_cumul_creative_etl":
        tmall_ztc_cumul_creative_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_cumul_keyword_etl":
        tmall_ztc_cumul_keyword_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ztc_cumul_target_etl":
        tmall_ztc_cumul_target_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_adzone_etl":
        tmall_ylmf_daliy_adzone_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_campaign_etl":
        tmall_ylmf_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_campaign_group_etl":
        tmall_ylmf_campaign_group_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_creativepackage_etl":
        tmall_ylmf_daliy_creativepackage_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_crowd_etl":
        tmall_ylmf_daliy_crowd_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_promotion_etl":
        tmall_ylmf_daliy_promotion_etl(airflow_execution_date, run_id)


    elif etl_action == "tmall_super_new_effect_etl":
        tmall_super_new_effect_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_super_new_report_etl":
        tmall_super_new_report_etl(airflow_execution_date, run_id)

    elif etl_action == "tamll_wxt_day_campaign_etl":
        tamll_wxt_day_campaign_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_campaign_cumul_etl":
        tmall_ylmf_campaign_cumul_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_adzone_cumul_etl":
        tmall_ylmf_daliy_adzone_cumul_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_campaign_group_cumul_etl":
        tmall_ylmf_campaign_group_cumul_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_creativepackage_cumul_etl":
        tmall_ylmf_daliy_creativepackage_cumul_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_crowd_cumul_etl":
        tmall_ylmf_daliy_crowd_cumul_etl(airflow_execution_date, run_id)

    elif etl_action == "tmall_ylmf_daliy_promotion_cumul_etl":
        tmall_ylmf_daliy_promotion_cumul_etl(airflow_execution_date, run_id)

    elif etl_action == "update_cumul_flag":
        # "2022-01-23 22:15:00+00:00"
        output_date = airflow_execution_date[0:10]
        output_date_time = output_date + "T" + airflow_execution_date[11:19]
        output_df.create_blob_by_text(
            f"{output_date}/flag.txt", output_date_time, "cumul"
        )

    elif etl_action == "update_target_flag":
        # "2022-01-23 22:15:00+00:00"
        output_date = airflow_execution_date[0:10]
        output_date_time = output_date + "T" + airflow_execution_date[11:19]
        output_df.create_blob_by_text(
            f"{output_date}/flag.txt", output_date_time, "target"
        )


    elif etl_action == "jd_gwcd_etl":
        jd_gwcd_adgroup_etl(airflow_execution_date, run_id)
        jd_gwcd_campaign_etl_new(airflow_execution_date, run_id)
    elif etl_action == "jd_jst_etl":
        jd_jst_campaign_etl_new(airflow_execution_date, run_id)
        jd_jst_search_etl_new(airflow_execution_date, run_id)

    elif etl_action == "jd_sem_etl":
        jdkc_adgroup_daily_etl(airflow_execution_date, run_id)
        jdkc_campaign_daily_etl(airflow_execution_date, run_id)
        jdkc_creative_daily_etl(airflow_execution_date, run_id)
        jdkc_keyword_daily_etl(airflow_execution_date, run_id)

    elif etl_action == "jd_zt_etl":
        jdzt_account_daily_etl(airflow_execution_date, run_id)
        jdzt_adgroup_daily_etl(airflow_execution_date, run_id)

    elif etl_action == "jd_zw_etl":
        emedia_sem_audience_mapping()
        jd_zw_campaign_etl(airflow_execution_date, run_id)
        jd_zw_creative_etl(airflow_execution_date, run_id)

    elif etl_action == "push_table_to_dw":
        push_table_to_dw()

    elif etl_action == "migration_etl_zf":
        jd_gwcd_daily_fact()
        jd_jst_daily_fact()
        # 以下是运行一次的历史数据处理
        # jd_gwcd_campaign_etl_old
        # jd_ht_campaign_etl_old
        # jd_jst_campaign_etl_old

    elif etl_action == "migration_etl_zw":
        print("---")

    elif etl_action == "migration_etl_yw":
        print("---")

    elif etl_action == "jd_tier_packet":
        jd_act_channel_daily_etl(airflow_execution_date, run_id)
        jd_act_flow_source_daily_etl(airflow_execution_date, run_id)
        jd_pit_data_daily_etl(airflow_execution_date, run_id)
        jd_propit_daily_etl(airflow_execution_date, run_id)
        jd_seckill_daily_etl(airflow_execution_date, run_id)
        jd_ticket_daily_etl(airflow_execution_date, run_id)
        jd_seckill_daily_fact()

    return 0


if __name__ == "__main__":
    emedia_etl(
        sys.argv[1],
        sys.argv[2],
        sys.argv[3].replace(":", "").replace("+", "").replace(".", ""),
    )
