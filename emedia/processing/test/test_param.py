import sys

from emedia.utils import output_df


def run_test(airflow_execution_date, run_id,**param_dics):
    print(airflow_execution_date)
    print(run_id)
    print(param_dics)
    output_df.create_blob_by_text(f"/test/test_param_.txt", param_dics)

if __name__ == '__main__':
    run_test(sys.argv[1], sys.argv[2], sys.argv[3])