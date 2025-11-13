import os
from datetime import datetime, date

base_url = "https://raw.githubusercontent.com/PacktPublishing/Data-Engineering-with-Azure-Databricks/refs/heads/main/Chapter02/raw_data/"
target_dir = "/Volumes/analytics_dev/sales_raw/staging"
today = date.today().isoformat()
file_format = "csv"
files = ["customers", "orders", "products", "order_items"]

for f in files:
    try:
        target_folder_path = f"{target_dir}/{f}/{today}"
        os.makedirs(target_folder_path, exist_ok=True)
        
        source = f"{base_url}/{f}.{file_format}"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        target = f"{target_folder_path}/{f}_{timestamp}.{file_format}"
        
        print(f"Copying {f}...")
        os.system(f"curl -L {source} -o {target}")
    except Exception as e:
        print(f"Error copying file {f}: {e}")
