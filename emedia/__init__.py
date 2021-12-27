# coding: utf-8

import os, sys, logging, configparser

log = logging.getLogger("log")
log.setLevel(logging.INFO)

_formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")

# console
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
log.addHandler(_console_handler)


def _get_dbr_env():
    # Local dev
    if not os.path.exists('/dbfs/mnt/databricks_conf.ini'):
        return 'qa'
    
    dbfsCfgParser = configparser.ConfigParser()
    dbfsCfgParser.read('/dbfs/mnt/databricks_conf.ini')

    if dbfsCfgParser['common']['env'] == 'qa':
        return 'qa'
    else:
        return 'prod'

dbr_env = _get_dbr_env()
