"""
Change 610,710 line last number: total index reading
- total Index reading should be consistent with the non-N of the previous date.
"""

import os
import csv
from datetime import datetime
import logging


logging.basicConfig(
    filename='log/daily_monitor.log',
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def parse_csv_lines(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.readlines()


def find_previous_valid_610(file_lines, meter_serial, prev_files):
    """
    Go back and find the last value and date of 610 rows
    corresponding to the 300 rows of non-N status for the specified water meter number.

    If the status of the previous day is N, continue to go back to the earlier files.

    parameter:
        file_lines (list): List of lines of the current file.
        meter_serial (str): Meter serial number.
        prev_files (list): List of paths to all previous files, sorted in reverse order by date.
    return:
        tuple or None: (last value, date) if found, or None if not found.
    """
    for i, line in enumerate(file_lines):
        if line.startswith("610") and meter_serial in line:
            # Find the corresponding 300 rows from the 610 row upwards
            for j in range(i - 1, -1, -1):  # 向上查找
                if file_lines[j].startswith("300"):
                    parts = file_lines[j].strip().split(",")
                    flag = parts[-4].strip()
                    if flag == "N":  # If the current state is N, continue to backtrack to earlier files
                        if not prev_files:
                            return None  # No more files to go back to
                        # Recursively trace back to earlier files
                        next_file_path = prev_files.pop(0)
                        next_file_lines = parse_csv_lines(next_file_path)
                        return find_previous_valid_610(next_file_lines, meter_serial, prev_files)
                    else:  # Find 300 rows with non-N status
                        date = parts[1]  # Get the date
                        last_value = line.strip().split(",")[-1]  # Get the last value of row 610
                        try:
                            last_value = float(last_value)
                        except ValueError:
                            last_value = None
                        return last_value, date
    return None


def count_n_flags_with_backtracking(folder_path):
    """
    Count the water meter numbers in each CSV file whose status is `N` in `300` row and the last value in `610` row,
    and trace back to find the 300 rows with non-`N` status corresponding to the water meter numbers
    and their corresponding 610 row values and dates.

    return:
        dict: A dictionary containing all water meter numbers, current values, and backdated values.
    """
    # Get a list of files and sort them by date
    files = sorted(
        [f for f in os.listdir(folder_path) if f.endswith(".csv")],
        key=lambda x: datetime.strptime(x.split("#")[1][:8], "%Y%m%d")
    )

    global_meter_dict = {}

    for i, current_file in enumerate(files):
        current_file_path = os.path.join(folder_path, current_file)
        current_lines = parse_csv_lines(current_file_path)

        # Create a dictionary
        # Count the water meter numbers in the current file
        # with status N in the 300th line and the last value in the 610th line
        meter_dict = {}

        for j, line in enumerate(current_lines):
            if line.startswith("300"):
                # Check the status of the current 300 rows
                parts = line.strip().split(",")
                flag = parts[-4].strip()
                if flag == "N":
                    # Find the corresponding 610 rows
                    for k in range(j + 1, len(current_lines)):
                        if current_lines[k].startswith("610"):
                            meter_serial = current_lines[k].split(",")[2].strip()  # Water meter number
                            last_value = current_lines[k].strip().split(",")[-1]  # The last value of line 610
                            try:
                                last_value = float(last_value)
                            except ValueError:
                                last_value = None

                            # Initialize dictionary entries
                            if meter_serial not in meter_dict:
                                meter_dict[meter_serial] = {"current_value": last_value, "previous": None}

                            # Backtrack the previous file and find the non-N state
                            prev_files = [os.path.join(folder_path, f) for f in files[:i][::-1]]  # Earlier file lists
                            result = find_previous_valid_610(current_lines, meter_serial, prev_files)
                            if result:
                                meter_dict[meter_serial]["previous"] = {
                                    "value": result[0],
                                    "date": result[1]
                                }
                            break

        global_meter_dict[current_file] = meter_dict

        logging.info(f"file: {current_file}")
        logging.info(f'dict: {meter_dict}')
        logging.info(f"nums of meters: {len(meter_dict)}\n")
        print(f"file: {current_file}")
        print(f"Dictionary: {meter_dict}")
        print(f"nums of meters: {len(meter_dict)}\n")

    return global_meter_dict


def modify_csv_files(folder_path, meter_dict):
    """
    for N
    Modify the CSV file based on the dictionary contents,
    replacing the current day's value for each water meter with the previous value.
    """
    modification_records = []

    for current_file, file_meter_dict in meter_dict.items():
        current_file_path = os.path.join(folder_path, current_file)
        current_lines = parse_csv_lines(current_file_path)
        modified_lines = current_lines.copy()

        for meter_serial, values in file_meter_dict.items():
            current_value = values["current_value"]
            previous_value = values["previous"]

            for i, line in enumerate(modified_lines):
                if line.startswith("610") and meter_serial in line:
                    # Modify the last value of line 610
                    parts = line.strip().split(",")
                    if previous_value and previous_value["value"] is not None:
                        modification_records.append({
                            "file": current_file,
                            "meter_serial": meter_serial,
                            "date": previous_value["date"],
                            "old_value": parts[-1],
                            "new_value": f"{previous_value['value']:.3f}"
                        })
                        logging.info(
                            f"Modify file: {current_file}, meter number: {meter_serial}, date: {previous_value['date']},"
                            f"old_value: {parts[-1]}, new_value: {previous_value['value']:.3f}"
                        )
                        print(
                            f"Modify file: {current_file}, meter number: {meter_serial}, date: {previous_value['date']},"
                            f"old_value: {parts[-1]}, new_value: {previous_value['value']:.3f}"
                        )
                        parts[-1] = f"{previous_value['value']:.3f}"
                        modified_lines[i] = ",".join(parts) + "\n"

                    # 710
                    for j in range(i + 1, len(modified_lines)):
                        if modified_lines[j].startswith("710"):
                            parts_710 = modified_lines[j].strip().split(",")
                            parts_710[-1] = f"{previous_value['value']:.3f}"
                            modified_lines[j] = ",".join(parts_710) + "\n"
                            break

        # 保存修改后的文件
        with open(current_file_path, "w", encoding="utf-8") as f:
            f.writelines(modified_lines)

    output_dir = "data_merged"
    os.makedirs(output_dir, exist_ok=True)  # 确保目录存在
    output_file = os.path.join(output_dir, "modification_records.csv")

    # 保存修改记录为 CSV
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["file", "meter_serial", "date", "old_value", "new_value"])
        writer.writeheader()
        writer.writerows(modification_records)


def save_meter_dict_to_csv(meter_dict, output_file):
    """将字典保存为 CSV 文件。"""
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file", "meter_serial", "current_value", "previous_value", "previous_date"])
        for file, meters in meter_dict.items():
            for meter_serial, values in meters.items():
                writer.writerow([
                    file,
                    meter_serial,
                    values["current_value"],
                    values["previous"]["value"] if values["previous"] else None,
                    values["previous"]["date"] if values["previous"] else None
                ])


def step6_main(folder_path="data_merged/NMI12_modified",
               meter_dict_output="data_merged/meter_dict.csv",
               modification_record_output="data_merged/modification_records.csv"):

    meter_dict = count_n_flags_with_backtracking(folder_path)
    save_meter_dict_to_csv(meter_dict, meter_dict_output)
    modify_csv_files(folder_path, meter_dict)

    logging.info(f"Step 6 is completed, the results have been saved to {meter_dict_output} and {modification_record_output}")
    print(f"Step 6 is completed, the results have been saved to {meter_dict_output} and {modification_record_output}")


if __name__ == "__main__":
    # 调用 Step6 主函数
    folder_path = "data_merged/NMI12_modified"
    meter_dict_output = "data_merged/meter_dict.csv"
    modification_record_output = "data_merged/modification_records.csv"

    step6_main(folder_path, meter_dict_output, modification_record_output)
