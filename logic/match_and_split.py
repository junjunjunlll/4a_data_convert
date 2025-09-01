import pandas as pd
import os
import re
from collections import defaultdict
import logging
import time
from logic.utils import read_file, get_file_list, get_excel_row_limit, \
    export_single_file, export_split_files, export_unmatched_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MatchAndSplitProcessor:
    def __init__(self):
        self.mapping_dict = {}
        self.column_headers = []
        self.all_file_paths = []
        self.output_dir = os.path.join(os.getcwd(), 'output')

    def set_output_dir(self, directory):
        self.output_dir = directory
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def load_source_files(self, path, header_row):
        """加载文件A或目录中的文件，并获取列标题。"""
        self.all_file_paths = get_file_list(path)
        if not self.all_file_paths:
            raise ValueError("没有找到需要处理的文件！")

        try:
            # 使用统一的 read_file 函数
            df = read_file(self.all_file_paths[0], header_row=header_row - 1)
            self.column_headers = list(df.columns)
            logging.info(f"成功读取 {os.path.basename(self.all_file_paths[0])} 的列标题。")
            return self.column_headers
        except Exception as e:
            raise Exception(f"加载源文件或读取列标题失败: {e}")

    def load_mapping_file(self, mapping_file_path):
        """加载文件B，并构建精确匹配的映射字典。"""
        try:
            # 使用统一的 read_file 函数
            df_b = read_file(mapping_file_path, header_row=None)
            if df_b.shape[1] < 2:
                raise ValueError("匹配关系文件（文件B）至少需要两列。")

            self.mapping_dict.clear()

            self.mapping_dict = {
                str(row.iloc[0]).strip().lower(): str(row.iloc[1]).strip()
                for _, row in df_b.iterrows()
                if not pd.isna(row.iloc[0]) and not pd.isna(row.iloc[1])
            }

            if not self.mapping_dict:
                raise ValueError("映射字典文件内容为空或格式不正确。")
            logging.info(f"成功加载映射字典，共 {len(self.mapping_dict)} 条记录。")
        except Exception as e:
            raise Exception(f"加载映射文件失败: {e}")

    def process_and_export(self, col_a, output_mode, split_row_count, output_format):
        """
        主处理函数，遍历所有文件，进行匹配、处理并分流输出。
        """
        if not self.all_file_paths:
            raise ValueError("请先加载源文件。")
        if not self.mapping_dict:
            raise ValueError("请先加载映射字典文件。")
        if col_a not in self.column_headers:
            raise ValueError(f"选择的列 '{col_a}' 在文件中不存在。")

        # 1. 统一处理所有源文件
        all_processed_data = pd.DataFrame()
        for file_path in self.all_file_paths:
            start_time = time.time()
            try:
                logging.info(f"开始处理文件: {os.path.basename(file_path)}")
                # 使用统一的 read_file 函数
                df = read_file(file_path, header_row=0)

                if col_a not in df.columns:
                    logging.warning(f"文件 {os.path.basename(file_path)} 中不存在列 '{col_a}'，跳过该文件。")
                    continue

                original_rows = len(df)

                # 新增一列，名为“所属”，并进行映射
                df['所属'] = df[col_a].apply(self._find_match_for_row)

                all_processed_data = pd.concat([all_processed_data, df], ignore_index=True)

                elapsed_time = time.time() - start_time
                logging.info(
                    f"文件 {os.path.basename(file_path)} 处理完成。原行数: {original_rows}, 用时: {elapsed_time:.2f} 秒。")

                del df

            except Exception as e:
                logging.error(f"处理文件 {os.path.basename(file_path)} 失败: {e}")
                raise

        if all_processed_data.empty:
            logging.warning("所有文件处理后均无数据，无法进行导出。")
            return

        # 2. 分离出匹配数据和无匹配数据
        matched_data = all_processed_data[all_processed_data['所属'] != '无匹配'].copy()
        unmatched_data = all_processed_data[all_processed_data['所属'] == '无匹配'].copy()

        logging.info(
            f"所有文件处理完毕。总记录数: {len(all_processed_data)}, 匹配记录数: {len(matched_data)}, 无匹配记录数: {len(unmatched_data)}。")

        if not unmatched_data.empty:
            logging.warning(f"警告: 存在 {len(unmatched_data)} 条记录未能找到匹配项，已单独导出到无匹配文件。")

        # 3. 导出匹配数据
        if not matched_data.empty:
            if output_mode == 'single_file':
                self._export_single_file(matched_data, output_format)
            else:
                self._export_split_files(matched_data, split_row_count, output_format)
        else:
            logging.info("没有找到任何匹配数据，跳过匹配文件导出。")

        # 4. 强制导出无匹配数据
        if not unmatched_data.empty:
            self._export_unmatched_file(unmatched_data, output_format)
        else:
            logging.info("没有无匹配数据，无需导出无匹配文件。")

    def _find_match_for_row(self, value):
        """为单个值查找精确匹配项，并返回映射值或“无匹配”。"""
        if pd.isna(value) or not str(value).strip():
            return "无匹配"

        return self.mapping_dict.get(str(value).strip().lower(), "无匹配")

    def _export_single_file(self, df, output_format):
        """导出单个匹配文件。"""
        # 调用 utils 中的统一导出方法
        export_single_file(df, self.output_dir, "match_and_split", output_format)

    def _export_split_files(self, df, split_row_count, output_format):
        """导出分割匹配文件。"""
        # 调用 utils 中的统一导出方法
        export_split_files(df, self.output_dir, split_row_count, output_format)

    def _export_unmatched_file(self, df, output_format):
        """导出无匹配文件。"""
        # 调用 utils 中的统一导出方法
        export_unmatched_file(df, self.output_dir, output_format)