import os
import pandas as pd
import chardet
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
import logging


def read_file(file_path, header_row=0):
    """
    根据文件扩展名读取CSV或Excel文件，并自动尝试解析编码。
    返回DataFrame。
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")

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
                        logging.info(f"成功使用 '{enc}' 编码读取文件：{os.path.basename(file_path)}")
                        return df
                    except (UnicodeDecodeError, pd.errors.ParserError):
                        continue
                raise ValueError("无法自动识别文件编码，请确保文件格式正确。")
            else:
                df = pd.read_csv(file_path, header=header_row, dtype='str', encoding=encoding)
                logging.info(f"成功使用 '{encoding}' 编码读取文件：{os.path.basename(file_path)}")
                return df
        else:  # xls, xlsx
            df = pd.read_excel(file_path, header=header_row)
            for col in df.columns:
                df[col] = df[col].astype(str)
            return df
    except Exception as e:
        raise ValueError(f"读取文件失败：{file_path} - {e}")


def get_file_list(path, file_types=['.csv', '.xls', '.xlsx']):
    """
    如果路径是目录，则获取目录中所有符合类型的文件列表。
    """
    if os.path.isdir(path):
        return [os.path.join(path, f) for f in os.listdir(path) if any(f.endswith(ft) for ft in file_types)]
    elif os.path.isfile(path):
        return [path]
    else:
        return []


def stream_to_excel(df_generator, output_file, sheet_name='Sheet1'):
    """
    流式写入Excel文件，避免内存溢出。
    接收一个 DataFrame 的生成器。
    """
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(sheet_name)

    first_chunk = True
    for df_chunk in df_generator:
        if first_chunk:
            header = list(df_chunk.columns)
            ws.append(header)
            first_chunk = False

        for r in dataframe_to_rows(df_chunk, index=False, header=False):
            ws.append(r)

    wb.save(output_file)


def get_excel_row_limit():
    return 1048576


def get_csv_row_limit():
    return float('inf')