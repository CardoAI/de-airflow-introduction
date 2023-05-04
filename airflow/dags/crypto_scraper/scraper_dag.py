from airflow.decorators import dag
from crypto_scraper.tasks import extract, transform, load
from pendulum import datetime

start_date = datetime(2023, 5, 3, 22, 0, 0)

@dag(
  dag_id="scraper_dag",
  start_date=start_date,
  schedule="@hourly",
)
def scraper_dag():
  coin_id = "bitcoin"
  csv_path = extract(coin_id=coin_id)
  avg_data = transform(coin_id=coin_id, csv_path=csv_path)
  load(coin_id=coin_id, data=avg_data)  

scraper_dag()

coins_list = [
  "bitcoin",
  "ethereum",
  "litecoin",
  "cardano",
  "dogecoin",
]

for coin_id in coins_list:
  @dag(
    dag_id=f"{coin_id}_scraper_dag",
    start_date=start_date,
    schedule="@hourly",
  )
  def crypto_scraper_dag():
    csv_path = extract(coin_id=coin_id)
    avg_data = transform(coin_id=coin_id, csv_path=csv_path)
    load(coin_id=coin_id, data=avg_data)  

  crypto_scraper_dag()


