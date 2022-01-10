<!-- TOC -->

- [Airflow Schedule](#airflow-schedule)
- [VIPSHOP](#vipshop)
    - [OTD](#otd)
    - [Finance](#finance)
- [JD](#jd)
    - [GWCD](#gwcd)
    - [HT](#ht)
    - [JST](#jst)
    - [SEM](#sem)
        - [Adgroup](#adgroup)
    - [DMP](#dmp)
- [ALI](#ali)
    - [YLMF](#ylmf)

<!-- /TOC -->

---

# Airflow Schedule
emedia 每天调度 2 次，airflow 传参使用 `{{ next_execution_date }}`，入参的值为调度的当前时间（T）。


# VIPSHOP
## OTD

## Finance


# JD
## GWCD
- 购物触点

- [jd_gwcd_campaign](emedia/processing/jd/jd_gwcd.py)


## HT
- 海投

- [jd_ht](emedia/processing/jd/jd_ht.py)


## JST
- 京速推

- API 输入字段: https://confluence-wiki.pg.com.cn/pages/viewpage.action?pageId=85570745

- DP 输出字段：https://confluence-wiki.pg.com.cn/display/MD/JD+JST

- [jd_jst](emedia/processing/jd/jd_jst.py)


## SEM
- 京东快车


### Adgroup
- API 输入字段：

- DP 输出字段：https://confluence-wiki.pg.com.cn/display/MD/JD+SEM+Adgroup

## DMP




# ALI
## YLMF
- 引力魔方


