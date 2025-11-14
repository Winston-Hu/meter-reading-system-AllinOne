"""
Overlay 300 rows on the trigger date
Change the Overlaid 300 rows flag N to F
Check and modify line 300 and the corresponding line 610 in the file content
    - total consumption of one day should be 0
    - 300, 96 intervals should be all 0.000
"""
import logging
import os
import shutil
from datetime import datetime
import csv


logging.basicConfig(
    filename='log/daily_monitor.log',
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def copy_folder(src_folder, dest_folder):
    """
    Copy the entire folder and its contents.
    """
    if os.path.exists(dest_folder):
        shutil.rmtree(dest_folder)  # 如果目标文件夹已存在，先删除
    shutil.copytree(src_folder, dest_folder)
    print(f"文件夹 {src_folder} 已复制到 {dest_folder}")


def load_csv_file(file_path):
    """
    read one csv.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return f.readlines()


def save_csv_file(file_path, lines):
    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def update_flag_to_f(line):
    """
    Change flag N to F。
    """
    parts = line.strip().split(",")
    if "N" in parts[-4]:  # 检查倒数第四列是否为 N
        parts[-4] = parts[-4].replace("N", "F")
    return ",".join(parts)


def insert_trigger_data(folder_path, backtracked_data):
    """
    Insert rows into the files in the copy folder NMI12_modified.
    """
    # Group data by meter ID and trigger_date
    grouped_data = {}
    for record in backtracked_data:
        key = (record["meter_serial"], record["trigger_date"])
        grouped_data.setdefault(key, []).append(record)

    # Traverse each set of data and perform insertion operations
    for (meter_serial, trigger_date), records in grouped_data.items():
        # Construct the corresponding file name
        trigger_date_obj = datetime.strptime(trigger_date, "%Y-%m-%d")
        trigger_date_str = trigger_date_obj.strftime('%Y%m%d')
        file_name = next(
            (f for f in os.listdir(folder_path) if trigger_date_str in f and f.endswith(".csv")),
            None
        )
        if not file_name:
            print(f"No file found for date {trigger_date_str}, skipping...")
            continue

        file_path = os.path.join(folder_path, file_name)

        # Reading file contents
        lines = load_csv_file(file_path)

        # Find the 610 rows corresponding to the water meter number and its associated 300 rows
        for idx, line in enumerate(lines):
            if line.startswith("610") and meter_serial in line:
                # Find the associated 300 rows upwards
                for j in range(idx - 1, -1, -1):
                    if lines[j].startswith("300"):
                        insertion_index = j
                        break
                    elif not lines[j].startswith("400"):
                        break  # If it is not 300 or 400 rows, stop searching
                else:
                    print(f"Line 300 not found for {meter_serial}, skipping...")
                    continue

                # Filter out 300 rows of trigger_date and change the flag of the inserted record to F
                insertion_lines = [
                    update_flag_to_f(rec["line"]) + "\n" for rec in records if rec["file_date"] != trigger_date
                ]
                if not insertion_lines:
                    print(f"The traceback for {meter_serial} contains only 300 for trigger_date, skipping...")
                    continue

                # insert 300
                lines = lines[:insertion_index] + insertion_lines + lines[insertion_index:]

                save_csv_file(file_path, lines)
                logging.info(f"Inserted traceback for {meter_serial} into {file_name} and changed flag to F")
                print(f"Inserted traceback for {meter_serial} into {file_name} and changed flag to F")
                break


def modify_300_and_610(lines):
    """
    Check and modify line 300 and the corresponding line 610 in the file content。
    """
    modified_lines = lines[:]
    for idx, line in enumerate(lines):
        if line.startswith("300"):
            parts = line.strip().split(",")
            if "N" in parts[-4]:  # flag is N
                # 300, 96 intervals should be all 0.000
                parts[2:98] = ["0.000"] * 96
                modified_lines[idx] = ",".join(parts) + "\n"

                # Find the corresponding 610 rows
                for j in range(idx + 1, len(lines)):
                    if lines[j].startswith("610"):
                        parts_610 = lines[j].strip().split(",")
                        # Change the first number after ‘W1’ in line 610 to 0.00000
                        for k in range(len(parts_610)):
                            if parts_610[k] == "W1" and k + 1 < len(parts_610):
                                parts_610[k + 1] = "0.000"
                                modified_lines[j] = ",".join(parts_610) + "\n"
                                break
                        break
    return modified_lines


def process_all_files(folder_path):
    """
    Traverse all files in the folder and modify the contents of lines 300 and 610.
    """
    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        lines = load_csv_file(file_path)
        modified_lines = modify_300_and_610(lines)
        save_csv_file(file_path, modified_lines)
        logging.info(f"Modified file {file_name}")
        print(f"Modified file {file_name}")


def step5_main(src_folder="data_merged/NMI12",
               dest_folder="data_merged/NMI12_modified",
               backtracked_csv_path="data_merged/output_meter_data_backtracked_sorted.csv"):

    # copy folder
    copy_folder(src_folder, dest_folder)

    # Loading backtrace data
    with open(backtracked_csv_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        backtracked_data = list(reader)

    # Insert backtrace records into replica folder
    insert_trigger_data(dest_folder, backtracked_data)

    # Modify all files in the replica folder
    process_all_files(dest_folder)


if __name__ == "__main__":
    src_folder = "data_merged/NMI12"
    dest_folder = "data_merged/NMI12_modified"
    backtracked_csv_path = "data_merged/output_meter_data_backtracked_sorted.csv"

    step5_main(src_folder, dest_folder, backtracked_csv_path)