import pandas as pd
import os
import logging
from openpyxl import load_workbook

# 在 logic/utils.py 中添加这个辅助函数
def get_excel_row_limit():
    """获取 Excel 文件的行数限制"""
    return 1048576

def read_file(file_path, header_row=0, nrows=None):
    """
    智能读取CSV或Excel文件，并处理常见的中文编码问题。
    :param file_path: 文件路径
    :param header_row: 标题行索引（从0开始）
    :param nrows: 要读取的行数，用于优化大文件读取
    :return: Pandas DataFrame
    """
    logging.info(f"正在读取文件: {os.path.basename(file_path)}")
    file_extension = os.path.splitext(file_path)[1].lower()

    if file_extension == '.csv':
        encodings = ['utf-8', 'gbk', 'gb18030', 'ansi']
        for encoding in encodings:
            try:
                logging.info(f"尝试使用 {encoding} 编码读取...")
                # 传入 nrows 参数
                df = pd.read_csv(file_path, header=header_row, nrows=nrows, dtype=str, encoding=encoding)
                logging.info(f"文件成功使用 {encoding} 编码读取。")
                return df
            except UnicodeDecodeError as e:
                logging.warning(f"使用 {encoding} 编码失败: {e}")
            except Exception as e:
                logging.error(f"读取文件 {file_path} 时发生非编码错误: {e}")
                raise
        raise UnicodeDecodeError("所有尝试的编码均无法正确读取CSV文件。请检查文件编码。")

    elif file_extension in ['.xlsx', '.xls']:
        try:
            # 传入 nrows 参数
            return pd.read_excel(file_path, header=header_row, nrows=nrows)
        except Exception as e:
            logging.error(f"读取Excel文件失败: {e}")
            raise

    else:
        raise ValueError("不支持的文件格式。请选择 .csv, .xlsx 或 .xls 文件。")

def get_file_list(path):
    """获取目录下的所有csv和excel文件列表"""
    if os.path.isdir(path):
        return [os.path.join(path, f) for f in os.listdir(path) if f.endswith(('.csv', '.xlsx', '.xls'))]
    else:
        return [path]