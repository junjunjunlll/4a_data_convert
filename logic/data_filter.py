import pandas as pd
import os
import re


def read_file(file_path, header_row):
    """
    根据文件扩展名读取CSV或Excel文件，并自动尝试解析编码。
    返回DataFrame。
    """
    try:
        if file_path.endswith('.csv'):
            # 常见编码列表，可以根据需要调整顺序
            common_encodings = ['utf-8', 'gbk', 'gb18030', 'latin-1', 'iso-8859-1']

            for encoding in common_encodings:
                try:
                    # 尝试用当前编码读取CSV文件
                    df = pd.read_csv(file_path, header=header_row, dtype='str', encoding=encoding)
                    print(f"成功使用 '{encoding}' 编码读取文件：{os.path.basename(file_path)}")
                    break  # 成功读取，跳出循环
                except UnicodeDecodeError:
                    print(f"尝试使用 '{encoding}' 编码失败，继续尝试...")
                    continue  # 失败，继续下一次循环
            else:
                # 如果所有编码都尝试失败
                raise ValueError("无法自动识别文件编码，请确保文件格式正确。")
        else:
            # 对于Excel文件，编码通常不是问题
            df = pd.read_excel(file_path, header=header_row)
            for col in df.columns:
                df[col] = df[col].astype(str)

        # 统一将所有列名转为字符串，避免类型不一致导致的问题
        df.columns = [str(col) for col in df.columns]

        return df
    except Exception as e:
        raise ValueError(f"读取文件失败：{file_path} - {e}")


def read_file_b_criteria(file_b, header_row):
    """读取文件b的第一列作为筛选条件。"""
    try:
        # 使用自动处理编码的 read_file 函数
        df_b = read_file(file_b, header_row=header_row)

        if df_b.empty or len(df_b.columns) == 0:
            raise ValueError("文件b内容为空，无法进行筛选。")

        col_b_name = df_b.columns[0]
        filter_values = set(df_b[col_b_name].dropna())

        if not filter_values:
            raise ValueError("文件b中用于筛选的列内容为空，无法进行筛选。")
        return filter_values
    except Exception as e:
        raise Exception(f"读取文件b筛选条件失败：{e}")


def read_and_filter(file_a, filter_criteria, col_a, match_mode, header_row):
    """
    读取文件a并进行筛选，返回筛选后的DataFrame。
    此函数不负责文件写入。
    """
    try:
        # 使用自动处理编码的 read_file 函数
        df_a = read_file(file_a, header_row=header_row)

        if col_a not in df_a.columns:
            raise ValueError(f"文件a '{os.path.basename(file_a)}' 中找不到列：'{col_a}'")

        if match_mode == "精确匹配":
            df_filtered = df_a[df_a[col_a].isin(filter_criteria)]
        elif match_mode == "包含匹配":
            escaped_values = [re.escape(str(val)) for val in filter_criteria if pd.notna(val)]
            if not escaped_values:
                return pd.DataFrame()
            pattern = '|'.join(escaped_values)
            df_filtered = df_a[df_a[col_a].str.contains(pattern, case=False, na=False)]
        # === 更改：新增前缀和后缀匹配逻辑 ===
        elif match_mode == "前缀匹配":
            escaped_values = [f"^{re.escape(str(val))}" for val in filter_criteria if pd.notna(val)]
            if not escaped_values:
                return pd.DataFrame()
            pattern = '|'.join(escaped_values)
            df_filtered = df_a[df_a[col_a].str.contains(pattern, case=False, na=False)]
        elif match_mode == "后缀匹配":
            escaped_values = [f"{re.escape(str(val))}$" for val in filter_criteria if pd.notna(val)]
            if not escaped_values:
                return pd.DataFrame()
            pattern = '|'.join(escaped_values)
            df_filtered = df_a[df_a[col_a].str.contains(pattern, case=False, na=False)]
        # === 更改结束 ===
        else:
            raise ValueError("不支持的匹配模式")

        return df_filtered

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"处理文件 '{os.path.basename(file_a)}' 失败：{e}")