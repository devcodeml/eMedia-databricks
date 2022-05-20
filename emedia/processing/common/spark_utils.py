import pyspark.sql.functions as F
import pyspark.sql.types as T

@F.udf(T.StringType())
def replace_bank(row):
    """
    替换字段换行符等
    :param row:
    :return: row
    """
    return row.replace('\n', '').replace('\r', '').strip()