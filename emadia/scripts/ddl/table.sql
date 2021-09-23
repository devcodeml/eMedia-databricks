CREATE TABLE `cdl_dp`.`vacuum_delta_log` (
  table_name String,
  start_date timestamp,
  end_date timestamp,
  trigger_date date
) USING delta LOCATION 'dbfs:/mnt/qa/data_warehouse/cdl_dp.db/vacuum_delta_log';

CREATE TABLE `cdl_dp`.`vacuum_delta_err_log` (
  table_name String,
  exception_info String,
  exception_date timestamp
) USING delta LOCATION 'dbfs:/mnt/qa/data_warehouse/cdl_dp.db/vacuum_delta_err_log';