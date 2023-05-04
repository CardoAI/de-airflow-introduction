from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
import pandas as pd
import os 
import san
from pendulum import datetime

SAN_API_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PGSQL_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
CSVFILE_TS_FORMAT = "%Y%m%d%H%M%S"

import logging

@task
def extract(coin_id: str, execution_date: datetime = None) -> str:
  to_date = execution_date.in_tz("UTC")
  from_date = to_date.subtract(hours=1)

  logging.info(f"Extracting data for '{coin_id}'.")

  crypto_info = san.get(
    "prices/" + coin_id,
    from_date = from_date.strftime(SAN_API_TS_FORMAT),
    to_date = to_date.strftime(SAN_API_TS_FORMAT),
    interval = "1s"
  )
  
  cwd = os.getcwd()
  extract_folder = f"{cwd}/extract"
  if not os.path.exists(extract_folder):
    os.makedirs(extract_folder)
  output_filename = f"{extract_folder}/{coin_id}_{to_date.strftime(CSVFILE_TS_FORMAT)}.csv"

  crypto_info.reset_index(inplace=True)
  crypto_info.to_csv(output_filename, index=False)
  return output_filename


@task
def transform(coin_id: str, csv_path: str, execution_date: datetime = None) -> dict:
  logging.info(f"Transforming data for '{coin_id}'.")
  df = pd.read_csv(csv_path)
  if df.empty:
    raise AirflowSkipException("No data to transform!")

  df_mean = df[["marketcap", "priceBtc", "priceUsd", "volume"]].mean()
  df_mean["refTime"] = execution_date.strftime(PGSQL_TS_FORMAT)

  return df_mean.to_dict()


@task
def load(coin_id: str, data: dict, postgres_hook: PostgresHook = PostgresHook(postgres_conn_id="postgresql_database")) -> None:
  if not data:
    raise AirflowSkipException("No data to load!")

  dst_table_name = f"{coin_id}_data"
  logging.info(f"Going to create the destinatin table '{dst_table_name}' if it doesn't exist.")

  create_table_sql=f"""
    CREATE TABLE IF NOT EXISTS {dst_table_name}
    (
      refTime TIMESTAMP,
      marketcap DOUBLE PRECISION,
      priceBtc DOUBLE PRECISION,
      priceUsd DOUBLE PRECISION,
      volume DOUBLE PRECISION
    );
  """
  postgres_hook.run(create_table_sql)

  insert_data_sql=f"""
  INSERT INTO {dst_table_name} (refTime, marketcap, priceBtc, priceUsd, volume)
  VALUES ('{data["refTime"]}', {data["marketcap"]}, {data["priceBtc"]}, {data["priceUsd"]}, {data["volume"]});
  """
  postgres_hook.run(insert_data_sql)
