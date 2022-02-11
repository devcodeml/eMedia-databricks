# coding: utf-8


from emedia.jobs.emedia_entry import emedia_etl


if __name__ == '__main__':

    next_execution_date = '2021-12-30T10:00:00+08:00'
    
    emedia_etl('jd_sem_keyword_etl', next_execution_date)

    