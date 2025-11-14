"""
If there is no corresponding file in the entire range, that is,
the entire range is powered off or disconnected,
the entire "Daily" program is meaningless (the daily report is full of #, and no valid data can be found).
Ignore the error or the error in the log, and rerun master_daily.py on the first day after recovery.
"""


from lib.step1_db_to_csv import step1_main
from lib.step2_get_processed_data import step2_main
from lib.step3_get_NMI12_ori import step3_main
from lib.step4_get_all300_andTriggerDate import step4_main
from lib.step5_multiple300_clear610 import step5_main
from lib.step6_clear610and710 import step6_main
from lib.step7_sendemail import step7_send_email_with_attachment

import time
import gc
import logging
import os
from datetime import datetime, timedelta

os.makedirs('log', exist_ok=True)

logging.basicConfig(
    filename="log/daily_monitor",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Delete all contents of the folder
def clear_directory(parent_dir, sub_dir):
    directory = os.path.join(parent_dir, sub_dir)
    try:
        if os.path.exists(directory):
            for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    os.rmdir(file_path)
            logging.info(f"Cleared contents of directory: {directory}")
        else:
            logging.warning(f"Directory does not exist: {directory}")
    except Exception as e:
        logging.error(f"Error clearing directory {directory}: {e}")


# Check log file size and trim
def check_and_trim_log(log_file, max_size_mb=10, trim_size_mb=2):
    max_size_bytes = max_size_mb * 1024 * 1024
    trim_size_bytes = trim_size_mb * 1024 * 1024

    try:
        if os.path.exists(log_file):
            file_size = os.path.getsize(log_file)
            if file_size > max_size_bytes:
                logging.info(f"Trimming log file: {log_file}. Current size: {file_size} bytes.")

                # Read subsequent parts of the log file (keep newer parts)
                with open(log_file, 'rb') as f:
                    f.seek(file_size - (max_size_bytes - trim_size_bytes))
                    data = f.read()

                # Write back the trimmed content
                with open(log_file, 'wb') as f:
                    f.write(data)
                logging.info(f"Log file trimmed. New size: {os.path.getsize(log_file)} bytes.")
    except Exception as e:
        logging.error(f"Error while trimming log file: {e}")


def master_main():
    logging.info("Starting execution of master workflow...")

    steps = [
        # ("step1_db_to_csv", step1_main),
        # ("step2_get_processed_data", step2_main),
        # ("step3_get_NMI12_ori", step3_main),
        # ("step4_get_all300_andTiggerDate", step4_main),
        # ("step5_multiple300_clear610", step5_main),
        # ("step6_clear610and710", step6_main),
        ("step7_SendEmail", step7_send_email_with_attachment)
    ]

    for step_name, step_function in steps:
        try:
            logging.info(f"Executing {step_name}...")
            print(f"Executing {step_name}...")
            step_function()
        except Exception as e:
            logging.error(f"Error in {step_name}: {e}")
            if step_name == "Send Email":
                logging.error("Email sending failed, skipping cleanup.")
                # If the send fails, no cleanup is performed
                return

    # # Clean up folder contents
    # parent_dir = "data_merged"
    # directories_to_clear = ["NMI12", "NMI12_modified"]
    # for sub_dir in directories_to_clear:
    #     clear_directory(parent_dir, sub_dir)

    logging.info("Master workflow execution completed.")
    print("Master workflow execution completed.")

    # Check and prune log files, the maximum size is 10M
    check_and_trim_log("log/daily_monitor.log")


if __name__ == "__main__":
    try:
        while True:
            current_time = datetime.now()
            next_run = current_time.replace(hour=9, minute=0, second=0, microsecond=0)

            # If the current time is past 8 o'clock today, set it to 8 o'clock tomorrow
            if current_time >= next_run:
                next_run += timedelta(days=1)

            # Calculate the required sleep time (seconds)
            sleep_time = (next_run - current_time).total_seconds()
            logging.info(f"Next run scheduled at {next_run}. Sleeping for {sleep_time} seconds.")
            print(f"Next run scheduled at {next_run}. Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)

            master_main()

            # Force triggering garbage collection to save memory
            gc.collect()

    except KeyboardInterrupt:
        logging.info("Terminating the workflow gracefully...")
        print("Terminating the workflow gracefully...")
