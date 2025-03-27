import requests
import time
from kbcstorage.client import Client
import csv

KEBOOLA_TOKEN = "your_token"
BASE_QUEUE_URL = "https://queue.europe-west3.gcp.keboola.com"
CONFIG_ID = "11232241"
COMPONENT_ID = "kds-team.ex-shopify"
TABLE_ID = "in.c-kds-team-ex-shopify-11232241.order"

headers = {
    "X-StorageApi-Token": KEBOOLA_TOKEN,
    "Content-Type": "application/json"
}


# trigger job
run_body = {
    "component": COMPONENT_ID,
    "config": CONFIG_ID,
    "mode": "run"
}

# make api call to keboola queue api
print("Triggering job...")
run_res = requests.post(f"{BASE_QUEUE_URL}/jobs", headers=headers, json=run_body)
run_res.raise_for_status()
job_id = run_res.json()["id"]
print(f"Triggered Job ID: {job_id}")

# check job status every 30 seconds
print("Job processing...")
job_url = f"{BASE_QUEUE_URL}/jobs/{job_id}"
for i in range(600):
    status_res = requests.get(job_url, headers=headers).json()
    status = status_res.get("status")
    print(f"   Job Status: {status}")
    if status in ["success", "error", "cancelled"]:
        break
    time.sleep(30)

if status == "success":
    print("Job completed successfully")

    # export results to csv
    client = Client('https://connection.europe-west3.gcp.keboola.com', KEBOOLA_TOKEN)
    client.tables.export_to_file(table_id=TABLE_ID, path_name='.')
    print(f"Exporting table {TABLE_ID}")

    with open('./order', mode='rt', encoding='utf-8') as in_file:
        lazy_lines = (line.replace('\0', '') for line in in_file)
        reader = csv.reader(lazy_lines, lineterminator='\n')
    print("Export complete")
else:
    print("Job failed")
    print(status_res.get("error", {}).get("message", "No error message found"))

