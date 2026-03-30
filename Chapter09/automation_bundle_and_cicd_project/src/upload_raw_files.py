# Databricks notebook source
# Upload Raw Files to Volume
# Copies CSV files from the bundle's raw_files/ directory into the raw volume.
# Only runs for dev and staging environments. Exits early for prod.


import os
import glob
import shutil

environment = dbutils.widgets.get("environment")
raw_volume_path = dbutils.widgets.get("raw_volume_path")

if environment not in ("dev", "staging"):
    print(f"Environment '{environment}' does not require file upload. Skipping.")
    dbutils.notebook.exit("skipped")

# Bundle files are deployed relative to the notebook location
notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_root = "/Workspace" + notebook_path.rsplit("/", 2)[0]
raw_files_dir = os.path.join(workspace_root, "raw_files")

csv_files = glob.glob(os.path.join(raw_files_dir, "*.csv"))

if not csv_files:
    print(f"No CSV files found in {raw_files_dir}")
    dbutils.notebook.exit("no_files")

for csv_file in csv_files:
    file_name = os.path.basename(csv_file)
    dataset_name = os.path.splitext(file_name)[0]
    dest_dir = f"{raw_volume_path}/{dataset_name}"
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = f"{dest_dir}/{file_name}"
    shutil.copy2(csv_file, dest_path)
    print(f"Copied {file_name} -> {dest_path}")

print(f"Uploaded {len(csv_files)} files to {raw_volume_path}")
