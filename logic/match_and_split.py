import pandas as pd
import os
import re
from collections import defaultdict
import logging
from tqdm import tqdm
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
        """加载文件B，并构建映射字典。"""
        try:
            df_b = read_file(mapping_file_path, header_row=None)
            if df_b.shape[1] < 2:
                raise ValueError("匹配关系文件（文件B）至少需要两列。")

            self.mapping_dict = {
                str(row.iloc[0]).strip(): str(row.iloc[1]).strip()
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
        主处理函数，遍历所有文件，进行匹配并输出。
        """
        if not self.all_file_paths:
            raise ValueError("请先加载源文件。")
        if not self.mapping_dict:
            raise ValueError("请先加载映射字典文件。")
        if col_a not in self.column_headers:
            raise ValueError(f"选择的列 '{col_a}' 在文件中不存在。")

        if output_mode == 'single_file':
            self._process_single_file(col_a, output_format)
        else:  # split_output 模式
            self._process_split_files(col_a, split_row_count, output_format)

    def _find_match_for_row(self, resource_group):
        """为单个资源组查找匹配项。"""
        if pd.isna(resource_group) or not resource_group:
            return "无匹配"

        for pattern_value, result_key in self.mapping_dict.items():
            pattern_str = str(pattern_value).strip()
            resource_group_str = str(resource_group).strip()

            if re.search(f'^{re.escape(pattern_str)}', resource_group_str, re.IGNORECASE):
                return result_key

        return "无匹配"

    def _process_single_file(self, col_a, output_format):
        """处理单文件输出模式。"""
        final_df = pd.DataFrame()
        total_rows = 0
        file_limit = get_excel_row_limit() if output_format == 'xlsx' else float('inf')

        for file_path in tqdm(self.all_file_paths, desc="处理文件并新增‘所属’列"):
            try:
                df = read_file(file_path, header_row=0)
                # 检查所选列是否存在于当前文件中
                if col_a not in df.columns:
                    logging.warning(f"文件 {os.path.basename(file_path)} 中不存在列 '{col_a}'，跳过该文件。")
                    continue

                # 新增一列，名为“所属”，并进行映射
                df['所属'] = df[col_a].apply(self._find_match_for_row)

                # 追加到最终的DataFrame中
                final_df = pd.concat([final_df, df], ignore_index=True)
                total_rows = len(final_df)

                if total_rows > file_limit:
                    raise ValueError(f"总行数 {total_rows} 超出 {output_format} 文件格式限制（{file_limit}行）。")

                # 及时释放内存
                del df

            except Exception as e:
                logging.error(f"处理文件 {os.path.basename(file_path)} 失败: {e}")
                # 如果处理失败，则清空已处理的数据，避免部分输出
                final_df = pd.DataFrame()
                break  # 失败则终止所有任务

        if not final_df.empty:
            # 更改文件名生成逻辑以匹配要求
            output_path = os.path.join(self.output_dir, f"match_and_split.{output_format}")
            if output_format == 'xlsx':
                final_df.to_excel(output_path, index=False)
            else:  # csv
                final_df.to_csv(output_path, index=False)
            logging.info(f"成功导出单个文件至: {output_path}")

    def _process_split_files(self, col_a, split_row_count, output_format):
        """处理分割输出模式。"""
        # 使用字典来存储每个分组的数据和行数
        grouped_dataframes = defaultdict(lambda: {'dfs': [], 'row_count': 0})

        for file_path in tqdm(self.all_file_paths, desc="处理文件并分组"):
            try:
                df = read_file(file_path, header_row=0)
                # 检查所选列是否存在于当前文件中
                if col_a not in df.columns:
                    logging.warning(f"文件 {os.path.basename(file_path)} 中不存在列 '{col_a}'，跳过该文件。")
                    continue

                # 新增一列，名为“所属”，并进行映射
                df['所属'] = df[col_a].apply(self._find_match_for_row)

                # 按 '所属' 列进行分组，并将每个分组的数据追加到对应的列表中
                for group_name, group_df in df.groupby('所属'):
                    safe_group_name = group_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace(' ',
                                                                                                                '_')
                    grouped_dataframes[safe_group_name]['dfs'].append(group_df)
                    grouped_dataframes[safe_group_name]['row_count'] += len(group_df)

            except Exception as e:
                logging.error(f"处理文件 {os.path.basename(file_path)} 失败: {e}")

        # 统一处理分组后的数据导出，并进行分页
        for group_name, data in tqdm(grouped_dataframes.items(), desc="导出分组文件"):
            final_df = pd.concat(data['dfs'], ignore_index=True)
            total_rows = len(final_df)

            # 计算需要分割的页数
            num_pages = (total_rows + split_row_count - 1) // split_row_count

            for page in range(num_pages):
                start_row = page * split_row_count
                end_row = min((page + 1) * split_row_count, total_rows)

                page_df = final_df.iloc[start_row:end_row]

                # 更改文件名生成逻辑以匹配要求
                # 文件名前缀为"{所属}_match_and_split"
                file_prefix = f"{group_name}_match_and_split"
                if num_pages > 1:
                    file_name = f"{file_prefix}_{page + 1}.{output_format}"
                else:
                    file_name = f"{file_prefix}.{output_format}"

                output_path = os.path.join(self.output_dir, file_name)

                # 导出文件
                if output_format == 'xlsx':
                    page_df.to_excel(output_path, index=False)
                else:  # csv
                    page_df.to_csv(output_path, index=False)

                logging.info(f"导出文件: {output_path} (行数: {len(page_df)})")