import configparser

databricks_conf_path = "/dbfs/mnt/databricks_conf.ini"

# conf_path = "/dbfs/etl-projects/eMedia-databricks/"
conf_path = "/Users/yaosheng/workspace/eMedia-databricks/"

def get_env():
    configParser = configparser.ConfigParser()
    # ini 文件路径
    databricks_conf_path = "/dbfs/mnt/databricks_conf.ini"
    # 读取 ini 文件
    # configParser.read(databricks_conf_path)
    # env = configParser.get("common", "env")
    env = "qa"
    return env



def get_conf(path,key):
    configParser = configparser.ConfigParser()
    # ini 文件路径
    databricks_conf_path = conf_path + f"/emadia/config/{path}/etl.conf"
    env = get_env().isupper()
    configParser.read(databricks_conf_path)
    if env == "PROD":
        value = configParser.get(env, key)
    else:
        value = configParser.get("QA", key)
    return value


if __name__ == '__main__':
    value = get_conf("vipshop","input_blob_container_name")
    print(value)



