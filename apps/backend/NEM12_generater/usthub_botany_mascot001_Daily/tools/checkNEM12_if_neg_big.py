#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
from collections import defaultdict


T_dailyConsumption = 0.5


def is_number(s: str) -> bool:
    s = s.strip()
    # 允许整数/小数/正负号
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?", s))


def extract_date_from_filename(filename: str) -> str | None:
    """
    适配 NEM12#2025082700025#X4MDP#Evergy.csv 这类文件名：
    - 优先取 # 分隔的第二段的前8位（20yyyyMMdd）
    - 兜底：在文件名中搜索首个 8 位以 '20' 开头的数字串
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

    # 汇总： meter_serial -> set([date1, date2, ...])
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

                    # === 负数检测 ===
                    has_negative = False
                    for col in row:
                        if is_number(col) and float(col) < 0:
                            has_negative = True
                            break
                    if has_negative:
                        negative_count += 1
                        print(f"[负数] 文件 {filename} 第{row_num}行: {row}")

                    # === 300 行 F 检测 ===
                    if row[0].strip() == "300":
                        if "F" in row:
                            Fin300_count += 1
                            print(f"[300含F] 文件 {filename} 第{row_num}行: {row}")

                    # === 400 行 F 检测 ===
                    if row[0].strip() == "400":
                        if "F" in row:
                            Fin400_count += 1
                            print(f"[400含F] 文件 {filename} 第{row_num}行: {row}")

                    # === 610 行超限：倒数第二列 > 2 ===
                    if row[0].strip() == "610" and len(row) >= 2 and is_number(row[-2]):
                        val = float(row[-2])
                        if val > T_dailyConsumption:
                            exceed_count += 1
                            print(f"[610超限] 文件 {filename} 第{row_num}行: {row}")
                            # 汇总整理：index=2 为水表号；日期来自文件名
                            if len(row) >= 3 and file_date:
                                meter_serial = row[2].strip()
                                meter_to_dates[meter_serial].add(file_date)

        except Exception as e:
            print(f"读取 {filename} 出错: {e}")

    # === 汇总输出 ===
    print("=============================================")
    print("\n扫描结束：")
    print(f"  共扫描 {scanned_rows} 行")
    print(f"  共 {negative_count} 行出现负数")
    print(f"  共 {Fin300_count} 行在 300 出现F")
    print(f"  共 {Fin400_count} 行在 400 出现F")
    print(f"  共 {exceed_count} 行满足 610 倒数第二个数值 > {T_dailyConsumption}")
    print("\n")
    print("=============================================")

    # === 610超限的“水表号→日期列表”汇总（按水表号排序；日期升序）===
    if meter_to_dates:
        print("\n[610超限汇总：水表号 → 日期列表]")
        for meter in sorted(meter_to_dates):                                  # 排序1：index=2（水表号）
            dates = sorted(meter_to_dates[meter])                              # 排序2：日期
            print(f"  {meter}: {', '.join(dates)}")
    else:
        print("\n[610超限汇总] 无记录")


if __name__ == "__main__":
    # folder = "../ready_to_send"
    folder = "manual_file"
    print(f"开始扫描目录: {os.path.abspath(folder)}\n")
    scan_csv_for_negatives_and_610(folder)
