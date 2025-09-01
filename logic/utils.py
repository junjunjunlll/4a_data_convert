import pandas as pd
import os
import logging
import re

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
        encodings = ['utf-8', 'gbk', 'gb18030', 'ansi', 'latin1','gb2312']
        for encoding in encodings:
            try:
                logging.info(f"尝试使用 {encoding} 编码读取...")
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


def export_dataframe_to_file(df, output_dir, file_name, output_format='xlsx'):
    """
    统一的 DataFrame 导出函数。根据指定的格式导出文件。
    :param df: 要导出的 DataFrame
    :param output_dir: 输出目录
    :param file_name: 输出文件名（不含扩展名）
    :param output_format: 输出格式 ('xlsx' 或 'csv')，默认为 'xlsx'
    """
    if df.empty:
        logging.info(f"DataFrame 为空，跳过导出: {file_name}")
        return

    # 清理文件名中的非法字符
    safe_file_name = re.sub(r'[\\/:*?"<>|]', '_', file_name)
    output_path = os.path.join(output_dir, f"{safe_file_name}.{output_format}")

    try:
        if output_format == 'xlsx':
            if len(df) > get_excel_row_limit():
                logging.error(f"行数 {len(df)} 超出 XLSX 文件格式限制。请尝试分流模式。")
                raise ValueError("行数超出 XLSX 文件格式限制")
            df.to_excel(output_path, index=False)
        elif output_format == 'csv':
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        else:
            raise ValueError("不支持的输出格式。请选择 'xlsx' 或 'csv'。")

        logging.info(f"成功导出文件至: {output_path}")
    except Exception as e:
        logging.error(f"导出文件失败: {e}")
        raise


def export_single_file(df, output_dir, file_prefix, output_format):
    """
    导出单个文件（用于 match_and_split 的单个文件模式）。
    :param df: 要导出的 DataFrame
    :param output_dir: 输出目录
    :param file_prefix: 文件名前缀，如 'match_and_split'
    :param output_format: 输出格式 ('xlsx' 或 'csv')
    """
    export_dataframe_to_file(df, output_dir, file_prefix, output_format)


def export_split_files(df, output_dir, split_row_count, output_format):
    """
    导出分割文件（用于 match_and_split 的分割模式）。
    :param df: 要导出的 DataFrame
    :param output_dir: 输出目录
    :param split_row_count: 每个文件的最大行数
    :param output_format: 输出格式 ('xlsx' 或 'csv')
    """
    grouped_dataframes = df.groupby('所属')

    for group_name, group_df in grouped_dataframes:
        total_rows = len(group_df)
        num_pages = (total_rows + split_row_count - 1) // split_row_count

        for page in range(num_pages):
            start_row = page * split_row_count
            end_row = min((page + 1) * split_row_count, total_rows)
            page_df = group_df.iloc[start_row:end_row]

            # 清理组名中的非法字符
            safe_group_name = re.sub(r'[\\/:*?"<>|]', '_', str(group_name))
            file_name = f"{safe_group_name}_match_and_split_{page + 1}" if num_pages > 1 else f"{safe_group_name}_match_and_split"

            export_dataframe_to_file(page_df, output_dir, file_name, output_format)


def export_unmatched_file(df, output_dir, output_format):
    """
    导出无匹配文件（用于 match_and_split）。
    :param df: 要导出的 DataFrame
    :param output_dir: 输出目录
    :param output_format: 输出格式 ('xlsx' 或 'csv')
    """
    export_dataframe_to_file(df, output_dir, "无匹配_match_and_split", output_format)


def export_match_results(matched_results, unmatched_values, output_dir, output_format):
    """
    将匹配结果和未匹配结果分别导出为文件（用于 data_match）。
    :param matched_results: 匹配成功的字典
    :param unmatched_values: 未匹配成功的集合
    :param output_dir: 输出目录
    :param output_format: 输出格式 ('xlsx' 或 'csv')
    """
    try:
        if matched_results:
            df_matched = pd.DataFrame({
                '文件a去重结果': list(matched_results.keys()),
                '文件b匹配结果': list(matched_results.values())
            })
            export_dataframe_to_file(df_matched, output_dir, 'matched_results', output_format)
        else:
            logging.info("没有成功匹配的结果，不生成匹配结果文件。")

        if unmatched_values:
            df_unmatched = pd.DataFrame(list(unmatched_values), columns=['无法匹配的资源组'])
            export_dataframe_to_file(df_unmatched, output_dir, 'unmatched_values', output_format)
        else:
            logging.info("所有资源组均已匹配，不生成未匹配结果文件。")

    except Exception as e:
        logging.error(f"导出匹配结果失败: {e}")
        raise