import pandas as pd
import os
import re
import logging
from logic.utils import read_file, export_match_results as utils_export_match_results

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_unique_values(file_a_path, is_dir_mode, header_row, col_a):
    """
    从文件a或目录中读取指定列，并返回去重后的值。
    """
    unique_values = set()
    files_to_process = []

    if is_dir_mode:
        files_to_process = [os.path.join(file_a_path, f) for f in os.listdir(file_a_path) if
                            f.endswith(('.csv', '.xlsx', '.xls'))]
    else:
        files_to_process = [file_a_path]

    if not files_to_process:
        raise ValueError("没有找到需要处理的文件！")

    print("正在从源文件中读取并去重指定列...")
    for full_path_a in files_to_process:
        try:
            df_a = read_file(full_path_a, header_row=header_row)
            if col_a not in df_a.columns:
                print(f"警告: 文件 '{os.path.basename(full_path_a)}' 中找不到列: '{col_a}'。跳过此文件。")
                continue

            # 使用 .astype(str) 防止数据类型问题
            unique_values.update(df_a[col_a].dropna().astype(str).unique())
            print(f"  - 从文件 '{os.path.basename(full_path_a)}' 中提取 {len(unique_values)} 条唯一值。")
        except Exception as e:
            print(f"处理文件 '{os.path.basename(full_path_a)}' 失败：{e}")

    if not unique_values:
        raise ValueError("去重后的内容为空，请检查文件和列名。")

    return unique_values


def fuzzy_match_and_fill(source_values, mapping_file_path, old_separator=None, new_separator=None):
    """
    根据映射文件对去重后的源值进行模糊匹配和填充。
    新增了分隔符替换功能。
    **已优化为右模糊匹配（前缀匹配）**
    """
    try:
        df_b = read_file(mapping_file_path, header_row=None)

        if df_b.shape[1] < 2:
            raise ValueError("匹配关系文件（文件b）至少需要两列：第一列为资源组，第二列为匹配值。")

        # 将第一列和第二列转换为字符串并去除首尾空格
        df_b.iloc[:, 0] = df_b.iloc[:, 0].astype(str).str.strip()
        df_b.iloc[:, 1] = df_b.iloc[:, 1].astype(str).str.strip()

        # **新增逻辑：处理分隔符替换**
        if old_separator and new_separator and old_separator != new_separator:
            # 处理转义字符
            if old_separator == '\\':
                old_separator = r'\\'
            logging.info(f"正在将文件B第一列中的分隔符 '{old_separator}' 替换为 '{new_separator}'")
            df_b.iloc[:, 0] = df_b.iloc[:, 0].str.replace(old_separator, new_separator, regex=False)

        # 构建匹配字典
        mapping_dict = dict(zip(df_b.iloc[:, 0], df_b.iloc[:, 1]))

        # **优化点：对 mapping_dict 的键按长度进行降序排序**
        sorted_keys = sorted(mapping_dict.keys(), key=len, reverse=True)

        matched_results = {}
        unmatched = set()
        match_count = 0

        logging.info("开始进行右模糊匹配...")
        for value in source_values:
            matched = False
            # 将源值转换为字符串并转为小写
            value_str = str(value).strip().lower()

            # **核心优化：遍历排序后的键进行前缀匹配**
            for pattern_key in sorted_keys:
                if pd.isna(pattern_key) or not isinstance(pattern_key, str) or not pattern_key:
                    continue

                # 转换为小写进行匹配，并使用 re.match 确保是从开头进行匹配
                # re.escape() 用于处理模式中的特殊字符
                if re.match(re.escape(pattern_key.lower()), value_str):
                    matched_results[value] = mapping_dict[pattern_key]
                    matched = True
                    match_count += 1
                    logging.info(f"成功匹配: '{value}' -> '{mapping_dict[pattern_key]}'")
                    break  # 找到最长匹配后立即停止

            if not matched:
                unmatched.add(value)
                logging.info(f"未匹配: '{value}'")

        print("\n--- 匹配结果总结 ---")
        print(f"总计资源组数量: {len(source_values)}")
        print(f"成功匹配数量: {match_count}")
        print(f"无法匹配数量: {len(unmatched)}")

        if unmatched:
            print("\n无法匹配的资源组:")
            for item in unmatched:
                print(f"- {item}")

        return matched_results, unmatched

    except Exception as e:
        logging.error(f"处理文件b失败: {e}")
        raise


def export_match_results(matched_results, unmatched_values, output_dir, output_format):
    """
    将匹配结果和未匹配结果分别导出为文件。
    该函数作为中间层，实际导出逻辑已转移至 utils.py。
    """
    try:
        utils_export_match_results(matched_results, unmatched_values, output_dir, output_format)
    except Exception as e:
        logging.error(f"导出匹配结果失败: {e}")
        raise