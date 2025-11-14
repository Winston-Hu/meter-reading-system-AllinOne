#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import smtplib
import tempfile
import re
import csv
from collections import defaultdict
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import paramiko  # pip install paramiko

# ─────────────────────────── log config ────────────────────────────
logging.basicConfig(
    filename="log/daily_monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

T_dailyConsumption = 1


def is_number(s: str) -> bool:
    s = s.strip()
    # Integers/decimals/positive and negative signs are allowed
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?", s))


def extract_date_from_filename(filename: str) -> str | None:
    """
    Adapting to filenames like NEM12#2025082700025#X4MDP#Evergy.csv:
    - Prioritize the first 8 characters of the second segment separated by # (e.g., 20yyyyMMdd)
    - As a fallback: Search the filename for the first 8-character string starting with '20'.
    """
    name = os.path.basename(filename)
    parts = name.split("#")
    if len(parts) >= 2 and len(parts[1]) >= 8 and parts[1][:2] == "20":
        return parts[1][:8]
    m = re.search(r"(20\d{6})", name)
    return m.group(1) if m else None


def scan_csv_for_negatives_and_610(folder_path: str):
    negative_count = 0
    exceed_count = 0
    Fin300_count = 0
    Fin400_count = 0
    scanned_rows = 0

    # meter_serial -> set([date1, date2, ...])
    meter_to_dates: dict[str, set[str]] = defaultdict(set)

    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, filename)
        file_date = extract_date_from_filename(filename)  # e.g. 20250827

        try:
            with open(file_path, newline='', encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                for row_num, row in enumerate(reader, start=1):
                    scanned_rows += 1
                    if not row:
                        continue

                    # === negative checking ===
                    has_negative = False
                    for col in row:
                        if is_number(col) and float(col) < 0:
                            has_negative = True
                            break
                    if has_negative:
                        negative_count += 1
                        logging.info(f"[Negative Number] File {filename} Line {row_num}: {row}")
                        print(f"[Negative Number] File {filename} Line {row_num}: {row}")

                    # === 300 lines of F-test ===
                    if row[0].strip() == "300":
                        if "F" in row:
                            Fin300_count += 1
                            logging.info(f"[300 including F] File {filename} Line {row_num}: {row}")
                            print(f"[300 including F] File {filename} Line {row_num}: {row}")

                    # === 400 lines of F-test===
                    if row[0].strip() == "400":
                        if "F" in row:
                            Fin400_count += 1
                            logging.info(f"[400 including F] File {filename} Line {row_num}: {row}")
                            print(f"[400 including F] File {filename} Line {row_num}: {row}")

                    # === Row 610 exceeds limit: Second to last column > 2 ===
                    if row[0].strip() == "610" and len(row) >= 2 and is_number(row[-2]):
                        val = float(row[-2])
                        if val > T_dailyConsumption:
                            exceed_count += 1
                            logging.info(f"[610 Exceeded Limit] File {filename} Line {row_num}: {row}")
                            print(f"[610 Exceeded Limit] File {filename} Line {row_num}: {row}")
                            # Summary and organization: index=2 is the water meter number; date comes from file name.
                            if len(row) >= 3 and file_date:
                                meter_serial = row[2].strip()
                                meter_to_dates[meter_serial].add(file_date)

        except Exception as e:
            logging.error(f"Error reading {filename}: {e}")
            print(f"Error reading {filename}: {e}")

    # === Summary ===
    print("==============================================")
    print("\nScan ended:")
    print(f"A total of {scanned_rows} rows were scanned")
    print(f"A total of {negative_count} rows contained negative numbers")
    print(f"A total of {Fin300_count} rows contained an F at row 300")
    print(f"A total of {Fin400_count} rows contained an F at row 400")
    print(f"A total of {exceed_count} rows contained the second-to-last value of 610 > {T_dailyConsumption}")
    print("\n")
    print("=============================================")

    logging.info("==============================================")
    logging.info("\nScan ended:")
    logging.info(f"A total of {scanned_rows} rows were scanned")
    logging.info(f"A total of {negative_count} rows contained negative numbers")
    logging.info(f"A total of {Fin300_count} rows contained an F at row 300")
    logging.info(f"A total of {Fin400_count} rows contained an F at row 400")
    logging.info(f"A total of {exceed_count} rows contained the second-to-last value of 610 > {T_dailyConsumption}")
    logging.info("\n")
    logging.info("=============================================")

    flag = int(negative_count or Fin300_count or Fin400_count or exceed_count)

    # ===
    # Summary of "Water Meter Number -> Date List" for 610 exceeding limits
    # (sorted by water meter number; date ascending order)
    # ===
    if meter_to_dates:
        logging.info("\n[Summary of 610 Over-Limit Water Meter Numbers -> Date List]")
        print("\n[Summary of 610 Over-Limit Water Meter Numbers -> Date List]")
        for meter in sorted(meter_to_dates):                                  # order 1：index=2（meter_serial）
            dates = sorted(meter_to_dates[meter])                              # order 2: datetime
            logging.info(f"  {meter}: {', '.join(dates)}")
            print(f"  {meter}: {', '.join(dates)}")
    else:
        logging.info("\n[610 Over-Limit Summary] No records found.")
        print("\n[610 Over-Limit Summary] No records found.")

    return flag


# ─────────────────────── main function: email + SFTP ───────────────────────
# Read file
# Standardize line breaks
# Correct line endings at 300/400 characters
# Send email/upload


def step7_send_email_with_attachment(
    # ---------- SMTP ----------
    smtp_server="mail.jdktech.com.au",
    smtp_port=465,
    sender_email="support@jdktech.com.au",
    sender_password="3.1415926Pi",
    recipient_email="winston@jdktech.com.au",
    cc_emails: list[str] = None,
    # ---------- attachment folder ----------
    attachment_folder="data_merged/NMI12_modified",
    # ---------- SFTP(The procedure will only be executed if all conditions are met) ----------
    sftp_host="54.206.186.32",
    sftp_port=22,
    sftp_username="evergy",
    sftp_password="LjQqtGSfD95FH9Kn",
    sftp_remote_path="/nem12"
):
    """
    Send an email with a NEM12 attachment; if SFTP 5 parameters are also provided,
    upload the same file to the remote server. Other cases send only an email.
    """
    if cc_emails is None:
        cc_emails = ["winston.hu2143@hotmail.com"]

    # 1. Find the latest *.csv file.
    latest_file = None
    latest_dt = None
    for fname in os.listdir(attachment_folder):
        if fname.endswith(".csv") and "#" in fname:
            try:
                dt = datetime.strptime(fname.split("#")[1][:8], "%Y%m%d")
                if latest_dt is None or dt > latest_dt:
                    latest_dt, latest_file = dt, fname
            except (ValueError, IndexError):
                continue
    if latest_file is None:
        raise FileNotFoundError("No matching CSV file found")
    attachment_path = os.path.join(attachment_folder, latest_file)

    with open(attachment_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 2. change line to CRLF
    content = content.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n")

    # 3. Additional processing: Change 'F' to 'S' in line 400.
    fixed_lines = []
    for line in content.splitlines():
        if line.startswith("400"):
            parts = line.split(",")
            parts = ["S" if field == "F" else field for field in parts]
            line = ",".join(parts)
        fixed_lines.append(line)
    content = "\r\n".join(fixed_lines)

    # 4. Corrected the use of one comma at the end of line 300 and two commas at the end of line 400.
    new_lines = []
    for line in content.splitlines():
        if line.startswith("300") and not line.endswith(","):
            line += ","
        elif line.startswith("400"):
            missing = 2 - (len(line) - len(line.rstrip(",")))
            if missing > 0:
                line += "," * missing
        new_lines.append(line)
    content = "\r\n".join(new_lines)

    folder = "data_merged/NMI12_modified"
    logging.info(f"Start scanning the directory: {os.path.abspath(folder)}\n")
    print(f"Start scanning the directory: {os.path.abspath(folder)}\n")
    flag = scan_csv_for_negatives_and_610(folder)
    logging.info(f"flag = {flag}")

    if flag == 0:

        # 5. Assemble and send emails
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = recipient_email
        if cc_emails:
            msg["Cc"] = ", ".join(cc_emails)
        msg["Subject"] = f"Daily NEM12: {latest_file}"
        msg.attach(MIMEText(f"Please see the attachment: {latest_file}", "plain"))

        part = MIMEBase("application", "octet-stream")
        part.set_payload(content.encode("utf-8"))
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={latest_file}")
        msg.attach(part)

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            recipients = [recipient_email] + cc_emails
            server.sendmail(sender_email, recipients, msg.as_string())

        logging.info("Email sent to %s with %s", recipients, latest_file)
        print(f"邮件已发送：{latest_file}")

        # # SFTP upload (only if all 5 parameters are given)
        # if all([sftp_host, sftp_username, sftp_password, sftp_remote_path]):
        #     with tempfile.NamedTemporaryFile(
        #         mode="w", encoding="utf-8", delete=False, suffix=".csv"
        #     ) as tmp:
        #         tmp.write(content)
        #         tmp_path = tmp.name
        #
        #     transport = paramiko.Transport((sftp_host, sftp_port))
        #     transport.connect(username=sftp_username, password=sftp_password)
        #     with paramiko.SFTPClient.from_transport(transport) as sftp:
        #         remote_file = os.path.join(sftp_remote_path, latest_file)
        #         sftp.put(tmp_path, remote_file)
        #
        #     os.unlink(tmp_path)
        #     transport.close()
        #     logging.info(
        #         "File %s uploaded to %s:%s", latest_file, sftp_host, sftp_remote_path
        #     )
        #     print(f"SFTP 上传完成：{latest_file} → {sftp_host}:{sftp_remote_path}")
    elif flag == 1:
        try:
            # 5. Assemble and send emails
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = recipient_email
            if cc_emails:
                msg["Cc"] = ", ".join(cc_emails)
            msg["Subject"] = f"Daily NEM12: {latest_file}"
            msg.attach(MIMEText(f"Something Error(Big/Neg/Fin300,400 data) in attachment: {latest_file}", "plain"))

            part = MIMEBase("application", "octet-stream")
            part.set_payload(content.encode("utf-8"))
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={latest_file}")
            msg.attach(part)

            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                server.login(sender_email, sender_password)
                recipients = [recipient_email] + cc_emails
                server.sendmail(sender_email, recipients, msg.as_string())

            logging.info("Email sent to %s with %s", recipients, latest_file)
            print(f"邮件已发送：{latest_file}")

            # # SFTP upload (only if all 5 parameters are given)
            # if all([sftp_host, sftp_username, sftp_password, sftp_remote_path]):
            #     with tempfile.NamedTemporaryFile(
            #         mode="w", encoding="utf-8", delete=False, suffix=".csv"
            #     ) as tmp:
            #         tmp.write(content)
            #         tmp_path = tmp.name
            #
            #     transport = paramiko.Transport((sftp_host, sftp_port))
            #     transport.connect(username=sftp_username, password=sftp_password)
            #     with paramiko.SFTPClient.from_transport(transport) as sftp:
            #         remote_file = os.path.join(sftp_remote_path, latest_file)
            #         sftp.put(tmp_path, remote_file)
            #
            #     os.unlink(tmp_path)
            #     transport.close()
            #     logging.info(
            #         "File %s uploaded to %s:%s", latest_file, sftp_host, sftp_remote_path
            #     )
            #     print(f"SFTP 上传完成：{latest_file} → {sftp_host}:{sftp_remote_path}")
        except Exception as e:
            print(f"error happened: {e}")


# ──────────────────────── 示例调用（可删） ────────────────────────
if __name__ == "__main__":
    # ----- SMTP 参数 -----
    smtp_server = "mail.jdktech.com.au"
    smtp_port = 465
    sender_email = "support@jdktech.com.au"
    sender_password = "3.1415926Pi"
    recipient_email = "winston@jdktech.com.au"
    cc_emails = ["winston.hu2143@hotmail.com"]

    # ----- SFTP 参数（如不需要 SFTP，可全部置 None） -----
    sftp_host = "54.206.186.32"
    sftp_port = 22
    sftp_username = "evergy"
    sftp_password = "LjQqtGSfD95FH9Kn"
    sftp_remote_path = "/nem12"

    # ----- 附件目录 -----
    attachment_folder = "data_merged/NMI12_modified"

    step7_send_email_with_attachment(
        smtp_server,
        smtp_port,
        sender_email,
        sender_password,
        recipient_email,
        cc_emails,
        attachment_folder,
        sftp_host,
        sftp_port,
        sftp_username,
        sftp_password,
        sftp_remote_path,
    )
