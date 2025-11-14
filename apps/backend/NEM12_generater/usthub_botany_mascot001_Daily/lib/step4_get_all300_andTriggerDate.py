"""
For the contents of the NMI12 folder,
extract 300 lines of content. Sort out the consecutive N,
and the corresponding dates that should be used as the superposition of 300 rows
"""

import logging
import os
import csv
from datetime import datetime


logging.basicConfig(
    filename='log/daily_monitor.log',
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_meter_data(lines):
    """
    Extract each water meter number and its associated 300 lines

    Parameters:
        lines (list): List of all lines of the file.
    Returns:
        dict: A dictionary containing each water meter number and its associated 300 lines of data.
    Format:
        {water meter number: {"line": 300 lines of content, "status": 300 lines of status}}
    """
    meter_data = {}
    current_meter = None

    for idx, line in enumerate(lines):
        if line.startswith("610"):  # first look for 610 -> meter serial
            current_meter = line.split(",")[2].strip()  # Extract water meter number

            # Find the 300 rows associated with the meter number, ignoring the 400 rows in between
            for j in range(idx - 1, -1, -1):  # Backtrack upward
                if lines[j].startswith("300"):
                    meter_data[current_meter] = {
                        "line": lines[j],
                        "status": lines[j].split(",")[-4].strip()
                    }
                    break
                elif not lines[j].startswith("400"):
                    break  # If it is not 300 or 400 lines, stop backtracking

    return meter_data


def process_nem12_files(folder_path):
    """
    Process all CSV files in the NEM12 folder and extract 300 rows of data corresponding to all water meter numbers.

    parameter:
        folder_path (str)
    return:
        list: A list containing 300 rows of data records for each water meter number.
    Format:
        [{"meter_serial": water meter number, "file_date": file date, "line": 300 lines of content, "status": status}, ...]
    """
    files = sorted([f for f in os.listdir(folder_path) if f.startswith("NEM12") and f.endswith(".csv")])
    all_meter_data = []

    for file_name in files:
        date_str = file_name.split("#")[1][:8]  # 提取文件中的日期
        date = datetime.strptime(date_str, "%Y%m%d")
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Extract water meter data from current file
        file_meter_data = extract_meter_data(lines)

        # Add the data of each water meter number to the global list
        for meter_serial, data in file_meter_data.items():
            all_meter_data.append({
                "meter_serial": meter_serial,
                "file_date": date.strftime("%Y-%m-%d"),
                "line": data["line"].strip(),
                "status": data["status"]
            })

    # Sort by water meter number and date
    all_meter_data.sort(key=lambda x: (x["meter_serial"], datetime.strptime(x["file_date"], "%Y-%m-%d")))

    return all_meter_data


def backtrack_meter_data(data):
    """
    Perform back-tracking on the water meter data and record the continuous N states.

    parameter:
        data (list): A list containing 300 rows of data records for each water meter number.
    return:
        list: A list of traceback results.
    Format:
        [{"meter_serial": 水表编号, "file_date": 日期, "line": 300行内容, "status": 状态, "trigger_date": 触发回溯的日期}, ...]
    """
    results = []

    # Group by water meter number
    meter_serials = sorted(set(record["meter_serial"] for record in data))
    for meter_serial in meter_serials:
        # Extract the records of the current water meter number and sort them by date
        meter_records = sorted(
            [record for record in data if record["meter_serial"] == meter_serial],
            key=lambda x: datetime.strptime(x["file_date"], "%Y-%m-%d")
        )

        # Traverse the records of the current water meter number
        # N, N, V, N, A -> [N,N,V],[N,A]. so the V and A are trigger dates
        for i in range(len(meter_records)):
            if meter_records[i]["status"] in {"A", "V"}:  # if the current state is A or V
                backtracked = []
                has_n = False  # Marks whether the traceback contains at least one N
                trigger_date = meter_records[i]["file_date"]  # start date
                for j in range(i - 1, -1, -1):  # backtrack upward
                    if meter_records[j]["status"] == "N":  # if the 300 is N
                        backtracked.append({
                            **meter_records[j],
                            "trigger_date": trigger_date
                        })
                        has_n = True
                    else:
                        break  # if 300 is not N, then we have get a period of N

                # If the traceback contains at least one N, join the current A or V record
                if has_n:
                    backtracked.append({
                        **meter_records[i],
                        "trigger_date": trigger_date
                    })

                # Sort by date and join results
                results.extend(sorted(
                    backtracked,
                    key=lambda x: datetime.strptime(x["file_date"], "%Y-%m-%d")
                ))

    # Sort the results by meter number and date
    results = sorted(
        results,
        key=lambda x: (x["meter_serial"], datetime.strptime(x["file_date"], "%Y-%m-%d"))
    )

    return results


def save_to_csv(data, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["meter_serial", "file_date", "line", "status", "trigger_date"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def step4_main(folder_path="data_merged/NMI12",
               output_sorted_csv="data_merged/output_meter_data_sorted.csv",
               output_backtracked_csv="data_merged/output_meter_data_backtracked_sorted.csv"):
    """
    output_sorted_csv (str): The path to the sorted output file.
    output_backtracked_csv (str): Output file path after backtrace.
    if there is no trigger date, the output_backtracked_csv will be empty.
    """
    print("Step 4: Processing files...")
    sorted_data = process_nem12_files(folder_path)
    save_to_csv(sorted_data, output_sorted_csv)
    logging.info(f"Sorted data saved to {output_sorted_csv}")
    print(f"Sorted data saved to {output_sorted_csv}")

    print("Step 4: Backtracking data...")
    backtracked_data = backtrack_meter_data(sorted_data)
    save_to_csv(backtracked_data, output_backtracked_csv)
    logging.info(f"Backtracked data saved to {output_backtracked_csv}")
    print(f"Backtracked data saved to {output_backtracked_csv}")


if __name__ == "__main__":
    # 示例运行
    folder_path = "data_merged/NMI12"
    output_sorted_csv = "data_merged/output_meter_data_sorted.csv"
    output_backtracked_csv = "data_merged/output_meter_data_backtracked_sorted.csv"

    step4_main(folder_path, output_sorted_csv, output_backtracked_csv)