# How to change the parameter

## lib/step1_db_to_csv.py:
### change day range -> how many days raw data you want to download
DAY_RANGE = 3
### put the address in the SITES list, filter the DB -> which site you want
SITES = [
    "233-255 Botany Road, Waterloo",
]

## lib/step3_get_processed_data.py  -> how many days NEM12.csv you want to generate
start_test_day = today - timedelta(days=2)

- finally: "NEM12#{unique_id}#X4MDP#Evergy.csv"

## master_daily.py -> set every-day/one-time execution time
next_run = current_time.replace(hour=16, minute=3, second=0, microsecond=0)