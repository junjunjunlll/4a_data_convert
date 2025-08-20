import pandas as pd
import os
import re
from collections import defaultdict
import logging
import time
from logic.utils import read_file, get_file_list, get_excel_row_limit

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
            df = read_file(self.all_file_paths[0], header_row=header_row - 1)
            self.column_headers = list(df.columns)
            logging.info(f"成功读取 {os.path.basename(self.all_file_paths[0])} 的列标题。")
            return self.column_headers
        except Exception as e:
            raise Exception(f"加载源文件或读取列标题失败: {e}")

    def load_mapping_file(self, mapping_file_path):
        """加载文件B，并构建精确匹配的映射字典。"""
        try:
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
                raise  # 抛出异常，让主线程处理

        if all_processed_data.empty:
            logging.warning("所有文件处理后均无数据，无法进行导出。")
            return

        # 2. 分离出匹配数据和无匹配数据
        matched_data = all_processed_data[all_processed_data['所属'] != '无匹配'].copy()
        unmatched_data = all_processed_data[all_processed_data['所属'] == '无匹配'].copy()

        logging.info(
            f"所有文件处理完毕。总记录数: {len(all_processed_data)}, 匹配记录数: {len(matched_data)}, 无匹配记录数: {len(unmatched_data)}。")

        # === 核心改动：如果存在无匹配数据，发出警告 ===
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
        output_path = os.path.join(self.output_dir, f"match_and_split.{output_format}")

        # 检查是否超出excel行数限制
        if output_format == 'xlsx' and len(df) > get_excel_row_limit():
            logging.error(f"总行数 {len(df)} 超出 XLSX 文件格式限制，无法以单文件模式导出。")
            raise ValueError(f"总行数 {len(df)} 超出 XLSX 文件格式限制，请切换到分割模式。")

        if output_format == 'xlsx':
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"成功导出单个匹配文件至: {output_path}")

    def _export_split_files(self, df, split_row_count, output_format):
        """导出分割匹配文件。"""
        grouped_dataframes = defaultdict(lambda: pd.DataFrame())
        for group_name, group_df in df.groupby('所属'):
            grouped_dataframes[group_name] = pd.concat([grouped_dataframes[group_name], group_df], ignore_index=True)

        for group_name, final_df in grouped_dataframes.items():
            total_rows = len(final_df)
            num_pages = (total_rows + split_row_count - 1) // split_row_count

            for page in range(num_pages):
                start_row = page * split_row_count
                end_row = min((page + 1) * split_row_count, total_rows)
                page_df = final_df.iloc[start_row:end_row]

                safe_group_name = re.sub(r'[\\/:*?"<>|]', '_', str(group_name))
                file_prefix = f"{safe_group_name}_match_and_split"
                file_name = f"{file_prefix}_{page + 1}.{output_format}" if num_pages > 1 else f"{file_prefix}.{output_format}"
                output_path = os.path.join(self.output_dir, file_name)

                if output_format == 'xlsx':
                    page_df.to_excel(output_path, index=False)
                else:
                    page_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                logging.info(f"导出文件: {output_path} (行数: {len(page_df)})")

    def _export_unmatched_file(self, df, output_format):
        """导出无匹配文件。"""
        output_path = os.path.join(self.output_dir, f"无匹配_match_and_split.{output_format}")

        if output_format == 'xlsx':
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"成功导出无匹配数据文件: {output_path} (总行数: {len(df)})")