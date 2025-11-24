#!/usr/bin/env python3
# replace_csv_inplace.py

import sys
import argparse
from pathlib import Path
import os
import re


def replace_in_file(filepath: str):
    # 1. 读入全文
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    content = content.replace("\r\n", "\n").replace("\r", "\n")

    # === 4. 新增：将 400 行中的 'F' 改为 'S' ===
    fixed_lines = []
    for line in content.split("\n"):
        if line.startswith("400"):
            parts = line.split(",")
            parts = ["S" if field == "F" else field for field in parts]
            line = ",".join(parts)
        fixed_lines.append(line)
    content = "\n".join(fixed_lines)

    new_lines = []
    for line in content.split("\n"):
        if line.startswith("300") and not line.endswith(","):
            line += ","
        elif line.startswith("400"):
            # 确保至少两个逗号结尾
            missing = 2 - (len(line) - len(line.rstrip(",")))
            if missing > 0:
                line += "," * missing
        new_lines.append(line)

    # # 3. 写回原文件（如果想保留备份，可先复制一份）
    # with open(filepath, 'w', encoding='utf-8') as f:
    #     f.write(content)
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        f.write("\r\n".join(new_lines))


def process_directory(directory: str):
    # 遍历目录中的所有文件
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    replace_in_file(file_path)
                    print(f"✅ 处理成功: {file_path}")
                except Exception as e:
                    print(f"❌ 处理失败 {file_path}: {str(e)}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"用法: python {sys.argv[0]} path/to/your/file_or_directory")
        sys.exit(1)

    target_path = sys.argv[1]

    if os.path.isfile(target_path):
        # 如果是文件，直接处理
        if target_path.lower().endswith('.csv'):
            replace_in_file(target_path)
            print(f"✅ 替换完成: {target_path}")
        else:
            print("⚠️ 警告: 提供的文件不是CSV文件")
    elif os.path.isdir(target_path):
        # 如果是目录，处理目录中的所有CSV文件
        process_directory(target_path)
        print(f"✅ 已完成目录中所有CSV文件的替换: {target_path}")
    else:
        print("❌ 错误: 提供的路径既不是文件也不是目录")

