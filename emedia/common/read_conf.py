import configparser

databricks_conf_path = "/dbfs/mnt/databricks_conf.ini"

conf_path = "/dbfs/etl-projects/"
# conf_path = "/Users/yaosheng/workspace/eMedia-databricks/"

def get_env():
    configParser = cf = configparser.RawConfigParser()
    # ini 文件路径
    databricks_conf_path = "/dbfs/mnt/databricks_conf.ini"
    # 读取 ini 文件
    # configParser.read(databricks_conf_path)
    # env = configParser.get("common", "env")
    env = "qa"
    return env



def get_conf(path,key):
    configParser = configparser.RawConfigParser()

    # ini 文件路径
    databricks_conf_path = conf_path + f"/emedia/config/{path}/etl.conf"
    env = get_env().upper()
    configParser.read(databricks_conf_path)
    if env == "PROD":
        value = configParser.get(env, key)
    else:
        value = configParser.get("QA", key)
    return value


if __name__ == '__main__':
    from emedia.common import read_conf
    print(read_conf.get_conf('vipshop', 'input_blob_sas_token'))



