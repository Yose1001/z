import os
import json
import time
import redis
import pandas as pd
import datetime

REDIS_HOST = '172.16.1.210'
REDIS_PORT = 6379
REDIS_KEY = 'wwkfsE4pGRb4/aZ3V6FY+pFMlTFpq1OkK3XnazLwV/jRxAHr4zrmkG2X9moY88R8mpT6ez3ivKEYGLnca'


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_KEY, db=0)


redis_client = get_redis_client()


def process_file(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-16',
                         delimiter='\t', on_bad_lines='skip')
        df = df.dropna(axis=1, how='all')
        df.columns = [col.lower() for col in df.columns]
        print(f"Processing file: {file_path}")

        if 'time' not in df.columns:
            print(f"Column 'time' not found in file: {file_path}")
            return None

        df['time'] = pd.to_datetime(
            df['time'], format='%Y.%m.%d %H:%M:%S', errors='coerce')
        data_json = df.to_json(orient='records')
        return json.loads(data_json)

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def process_files_in_directory(directory, target_date):
    all_processed_data = []
    target_date_str = target_date.strftime('%Y.%m.%d')

    for dirpath, _, filenames in os.walk(directory):
        symbol = os.path.basename(dirpath)
        for filename in filenames:
            if filename.lower().endswith('.csv') and target_date_str in filename:
                full_file_path = os.path.join(dirpath, filename)
                processed_data = process_file(full_file_path)
                if processed_data:
                    for entry in processed_data:
                        entry['symbol'] = symbol
                    all_processed_data.extend(processed_data)
    return all_processed_data


def add_data_to_redis(redis_client, key, sub_key, results_json, date):
    while True:
        try:
            redis_client.hset(key, sub_key, results_json)
            return {'message': 'successfully!', 'code': True, 'status': 200}

        except redis.exceptions.ConnectionError as e:
            # print(f"Redis is loading the dataset in memory...")
            #time.sleep(30)
            return
        except redis.exceptions.ResponseError as e:
            if "MISCONF" in str(e):
                # print(f"Retrying in seconds...")
                #time.sleep(30)
                continue
            else:
                return

def load_and_process_data(directory, target_date):
    start_time = time.time()
    print(f"Process started at: {
          datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    processed_data = process_files_in_directory(directory, target_date)

    if not processed_data:
        print("No data found to process.")
        return

    for entry in processed_data:
        symbol = entry.get('symbol')
        if not symbol:
            continue

        main_key = f"symbol:{symbol}"
        sub_key = f"date:{target_date}:data"
        results_json = json.dumps(entry)

        response = add_data_to_redis(
            redis_client, main_key, sub_key, results_json, target_date)
        print(f"Symbol for {symbol} on {
              target_date} added to Redis: {response}")

    end_time = time.time()
    print(f"\nProcess ended at: {
          datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    print(f"Total duration: {end_time - start_time:.2f} seconds\n")


if __name__ == "__main__":
    directory = r'Z:/'
    target_date = datetime.datetime(
        2024, 11, 18).date()
    load_and_process_data(directory, target_date)
