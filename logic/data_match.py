import pandas as pd
import os
import re
import chardet


def read_file(file_path, header_row):
    """
    根据文件扩展名读取CSV或Excel文件，并自动尝试解析编码。
    返回DataFrame。
    """
    try:
        if file_path.endswith('.csv'):
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read())
                encoding = result['encoding']

            if not encoding or result['confidence'] < 0.8:
                common_encodings = ['utf-8', 'gbk', 'gb18030', 'latin-1', 'iso-8859-1']
                for enc in common_encodings:
                    try:
                        df = pd.read_csv(file_path, header=header_row, dtype='str', encoding=enc)
                        print(f"成功使用 '{enc}' 编码读取文件：{os.path.basename(file_path)}")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError("无法自动识别文件编码，请确保文件格式正确。")
            else:
                df = pd.read_csv(file_path, header=header_row, dtype='str', encoding=encoding)
                print(f"成功使用 '{encoding}' 编码读取文件：{os.path.basename(file_path)}")
        else:
            df = pd.read_excel(file_path, header=header_row)
            for col in df.columns:
                df[col] = df[col].astype(str)

        df.columns = [str(col) for col in df.columns]
        return df
    except Exception as e:
        raise ValueError(f"读取文件失败：{file_path} - {e}")


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

            unique_values.update(df_a[col_a].dropna().unique())
            print(f"  - 从文件 '{os.path.basename(full_path_a)}' 中提取 {len(unique_values)} 条唯一值。")
        except Exception as e:
            print(f"处理文件 '{os.path.basename(full_path_a)}' 失败：{e}")

    if not unique_values:
        raise ValueError("去重后的内容为空，请检查文件和列名。")

    return unique_values


def fuzzy_match_and_fill(source_values, mapping_file_path):
    """
    根据映射文件对去重后的源值进行模糊匹配和填充。
    """
    try:
        if mapping_file_path.endswith('.csv'):
            common_encodings = ['utf-8', 'gbk', 'gb18030', 'latin-1', 'iso-8859-1']
            df_b = None
            for encoding in common_encodings:
                try:
                    df_b = pd.read_csv(mapping_file_path, header=None, dtype=str, encoding=encoding)
                    print(f"成功使用 '{encoding}' 编码读取文件b")
                    break
                except UnicodeDecodeError:
                    continue
            if df_b is None:
                raise ValueError("无法自动识别文件b的编码。")
        else:
            df_b = pd.read_excel(mapping_file_path, header=None)

        if df_b.shape[1] < 2:
            raise ValueError("匹配关系文件（文件b）至少需要两列：第一列为资源组，第二列为匹配值。")

        # **改进点1：直接使用 B 文件的第一列作为匹配模式，第二列作为结果**
        # 这里的 key 是匹配模式，value 是你最终想要的结果
        mapping_dict = dict(zip(df_b.iloc[:, 0], df_b.iloc[:, 1]))

        # **改进点2：不再需要反转字典**
        # 直接使用 mapping_dict 进行匹配

        matched_results = {}
        unmatched = set()
        match_count = 0

        for resource_group in source_values:
            matched = False
            # **改进点3：循环遍历 mapping_dict**
            for pattern_value, result_key in mapping_dict.items():
                if pd.isna(pattern_value) or pd.isna(result_key):
                    continue

                # 模糊匹配
                # 使用 re.search 在 resource_group 中搜索 pattern_value
                # 推荐使用 ^ 锚点，确保 pattern_value 位于 resource_group 的开头
                # 比如 '中国移动...' 才能匹配到 '中国移动.../专网...'
                # 这比简单的 'in' 检查更精确，也比不带 ^ 的 re.search 更能防止误匹配
                # re.escape() 用于处理 pattern_value 中可能存在的特殊字符

                # 对两个字符串进行预处理，去除首尾空格，转换为字符串
                resource_group_str = str(resource_group).strip()
                pattern_value_str = str(pattern_value).strip()

                if re.search(f'^{re.escape(pattern_value_str)}', resource_group_str, re.IGNORECASE):
                    # **改进点4：将 B 文件的第二列（即 result_key）作为结果**
                    # 这与你的注释是吻合的
                    matched_results[resource_group] = result_key
                    match_count += 1
                    matched = True
                    break

            if not matched:
                unmatched.add(resource_group)

        print("\n--- 匹配结果总结 ---")
        print(f"总计资源组数量: {len(source_values)}")
        print(f"成功匹配数量: {match_count}")
        print(f"无法匹配数量: {len(unmatched)}")
        print("\n无法匹配的资源组:")
        for item in unmatched:
            print(f"- {item}")

        return matched_results, unmatched

    except Exception as e:
        raise ValueError(f"处理文件b失败: {e}")


def export_match_results(matched_results, unmatched_values, output_dir):
    """
    将匹配结果和未匹配结果分别导出为Excel文件。
    """
    try:
        # 构建包含源值和匹配值的DataFrame
        if matched_results:
            df_matched = pd.DataFrame({
                '文件a去重结果': list(matched_results.keys()),
                '文件b匹配结果': list(matched_results.values())
            })
            matched_file_path = os.path.join(output_dir, 'matched_results.xlsx')
            df_matched.to_excel(matched_file_path, index=False)
            print(f"\n匹配结果已成功导出至: {matched_file_path}")
        else:
            print("\n没有成功匹配的结果，不生成匹配结果文件。")

        # 导出未匹配的结果
        if unmatched_values:
            df_unmatched = pd.DataFrame(list(unmatched_values), columns=['无法匹配的资源组'])
            unmatched_file_path = os.path.join(output_dir, 'unmatched_values.xlsx')
            df_unmatched.to_excel(unmatched_file_path, index=False)
            print(f"无法匹配的资源组已导出至: {unmatched_file_path}")
        else:
            print("所有资源组均已匹配，不生成未匹配结果文件。")

    except Exception as e:
        raise ValueError(f"导出匹配结果失败: {e}")