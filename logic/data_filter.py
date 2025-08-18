import pandas as pd
import os
import re


def read_file(file_path, header_row):
    """根据文件扩展名读取CSV或Excel文件，并返回DataFrame。"""
    try:
        if file_path.endswith('.csv'):
            # 使用 dtype='str' 参数来确保所有列都以字符串类型读取
            df = pd.read_csv(file_path, header=header_row, dtype='str')
        else:
            # 对于Excel，无法在读取时直接设置所有列为字符串
            df = pd.read_excel(file_path, header=header_row)
            # 遍历所有列，将它们转换为字符串类型
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
        # 使用 read_file 函数
        df_b = read_file(file_b, header_row=header_row)

        if df_b.empty or len(df_b.columns) == 0:
            raise ValueError("文件b内容为空，无法进行筛选。")

        # 获取第一列的列名
        col_b_name = df_b.columns[0]

        # 使用第一列的数据作为筛选值
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
        # 使用 read_file 函数
        df_a = read_file(file_a, header_row=header_row)

        if col_a not in df_a.columns:
            # 可以在这里选择跳过文件或报错，目前选择报错
            raise ValueError(f"文件a '{os.path.basename(file_a)}' 中找不到列：'{col_a}'")

        if match_mode == "精确匹配":
            # 将文件a中用于匹配的列也转换为字符串
            df_filtered = df_a[df_a[col_a].isin(filter_criteria)]
        elif match_mode == "模糊匹配":
            escaped_values = [re.escape(val) for val in filter_criteria]
            pattern = '|'.join(escaped_values)
            df_filtered = df_a[df_a[col_a].str.contains(pattern, case=False, na=False)]
        else:
            raise ValueError("不支持的匹配模式")

        return df_filtered

    except Exception as e:
        # 为了调试方便，捕获并打印完整的 traceback
        import traceback
        traceback.print_exc()
        raise Exception(f"处理文件 '{os.path.basename(file_a)}' 失败：{e}")