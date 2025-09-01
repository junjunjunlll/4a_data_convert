import pandas as pd
import os
import logging
from logic.utils import read_file, get_file_list, export_dataframe_to_file

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataFilterLogic:
    def __init__(self):
        self.log = logging.getLogger(__name__)

    def get_source_columns(self, file_a_path, is_dir_mode, header_row):
        """
        读取文件a的列标题。
        :param file_a_path: 文件或目录路径
        :param is_dir_mode: 是否为目录模式
        :param header_row: 标题行数（从1开始）
        :return: 列标题列表
        """
        file_to_read = file_a_path
        if is_dir_mode:
            files = get_file_list(file_a_path)
            if not files:
                raise FileNotFoundError("所选目录中没有找到可用的CSV或Excel文件！")
            file_to_read = files[0]

        # 调用 utils 中的 read_file 函数，仅读取标题行
        df = read_file(file_to_read, header_row=header_row - 1, nrows=0)
        return [str(col) for col in df.columns]

    def process_data(self, params):
        """
        根据参数处理数据。
        :param params: 包含所有处理参数的字典
        """
        if params["is_filter_mode"]:
            self._start_filter(params)
        else:
            self._start_pagination_only(params)

    def _start_filter(self, params):
        """筛选模式的业务逻辑"""
        file_a_path = params["file_a_path"]
        is_dir_mode = params["is_dir_mode"]
        file_b_path = params["file_b_path"]
        col_a = params["col_a"]
        match_mode = params["match_mode"]
        header_row = params["header_row"] - 1
        page_size = params["page_size"]
        output_dir = params["output_dir"]
        output_format = params["output_format"]

        # 参数校验
        if not file_a_path or not file_b_path:
            raise ValueError("请选择文件/目录和筛选文件！")
        if not col_a:
            raise ValueError("请选择文件a的筛选列！")
        if page_size <= 0:
            raise ValueError("请填写有效的分页大小！")

        self.log.info("--- 开始筛选过程 ---")
        self.log.info(f"输出目录: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        self._clear_output_dir(output_dir)

        # 读取筛选条件
        filter_criteria = self._read_file_b_criteria(file_b_path, header_row)
        self.log.info(f"筛选条件共 {len(filter_criteria)} 条。")

        # 批量处理文件
        files_to_process = get_file_list(file_a_path)
        if not files_to_process:
            raise FileNotFoundError("没有找到需要处理的文件！")
        self.log.info(f"共找到 {len(files_to_process)} 个文件需要处理。")

        filtered_data_all = pd.DataFrame()
        total_records_processed = 0

        for i, full_path_a in enumerate(files_to_process):
            file_name = os.path.basename(full_path_a)
            self.log.info(f"[{i + 1}/{len(files_to_process)}] 正在处理文件：{file_name}")

            df = read_file(full_path_a, header_row=header_row)
            if col_a not in df.columns:
                self.log.warning(f"文件 {file_name} 中不存在列 '{col_a}'。跳过。")
                continue

            # 将筛选列转为小写处理
            temp_col = df[col_a].astype(str).str.lower().fillna('')

            # 执行筛选
            if match_mode == '精确匹配':
                df_filtered = df[temp_col.isin(filter_criteria)].copy()
            elif match_mode == '包含匹配':
                df_filtered = df[temp_col.apply(lambda x: any(c in x for c in filter_criteria))].copy()
            elif match_mode == '前缀匹配':
                df_filtered = df[temp_col.apply(lambda x: any(x.startswith(c) for c in filter_criteria))].copy()
            elif match_mode == '后缀匹配':
                df_filtered = df[temp_col.apply(lambda x: any(x.endswith(c) for c in filter_criteria))].copy()

            self.log.info(f"  - 原文件记录数：{len(df)}，筛选保留记录数：{len(df_filtered)}")
            total_records_processed += len(df)
            filtered_data_all = pd.concat([filtered_data_all, df_filtered], ignore_index=True)

        self.log.info(f"已加载所有文件，总记录数: {len(filtered_data_all)}")

        # 统一分页输出
        self._export_paged_data(filtered_data_all, page_size, output_dir, output_format, "filtered_part")

        self.log.info("\n--- 筛选过程总结 ---")
        self.log.info(f"处理文件总数: {len(files_to_process)}")
        self.log.info(f"总记录数: {total_records_processed}")
        self.log.info(f"筛选保留总记录数: {len(filtered_data_all)}")
        self.log.info(f"筛选丢弃总记录数: {total_records_processed - len(filtered_data_all)}")

    def _start_pagination_only(self, params):
        """仅分页模式的业务逻辑"""
        file_a_path = params["file_a_path"]
        page_size = params["page_size"]
        output_dir = params["output_dir"]
        output_format = params["output_format"]
        header_row = params["header_row"] - 1

        # 参数校验
        if not file_a_path:
            raise ValueError("请选择文件/目录！")
        if page_size <= 0:
            raise ValueError("请填写有效的分页大小！")

        self.log.info("--- 开始仅分页过程 ---")
        self.log.info(f"输出目录: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        self._clear_output_dir(output_dir)

        # 批量处理文件
        files_to_process = get_file_list(file_a_path)
        if not files_to_process:
            raise FileNotFoundError("没有找到需要处理的文件！")
        self.log.info(f"共找到 {len(files_to_process)} 个文件需要处理。")

        all_data_to_page = pd.DataFrame()
        total_records_processed = 0

        for i, full_path_a in enumerate(files_to_process):
            file_name = os.path.basename(full_path_a)
            self.log.info(f"[{i + 1}/{len(files_to_process)}] 正在处理文件：{file_name}")

            df = read_file(full_path_a, header_row=header_row)
            total_records_processed += len(df)
            all_data_to_page = pd.concat([all_data_to_page, df], ignore_index=True)

        self.log.info(f"已加载所有文件，总记录数: {total_records_processed}")

        # 统一分页输出
        self._export_paged_data(all_data_to_page, page_size, output_dir, output_format, "paged_part")

        self.log.info("\n--- 分页过程总结 ---")
        self.log.info(f"处理文件总数: {len(files_to_process)}")
        self.log.info(f"总记录数: {total_records_processed}")

    def _read_file_b_criteria(self, file_b_path, header_row):
        """读取筛选条件文件，并返回一个包含所有条件的集合。"""
        df_b = read_file(file_b_path, header_row=None)
        if df_b.empty:
            raise ValueError("筛选条件文件为空，请检查文件内容。")
        criteria_set = set(df_b.iloc[:, 0].dropna().astype(str).str.lower().tolist())
        return criteria_set

    def _export_paged_data(self, df, page_size, output_dir, output_format, prefix):
        """通用分页导出逻辑"""
        if df.empty:
            self.log.info("没有数据需要导出，操作跳过。")
            return

        num_chunks = (len(df) + page_size - 1) // page_size
        self.log.info(f"总记录数 {len(df)}，将分为 {num_chunks} 个文件进行导出。")

        for i in range(num_chunks):
            start_index = i * page_size
            end_index = start_index + page_size
            chunk = df.iloc[start_index:end_index]

            file_name = f"{prefix}_{i + 1}"
            export_dataframe_to_file(chunk, output_dir, file_name, output_format)
            self.log.info(f"  - 【输出文件】已输出第 {i + 1} 个分页文件，记录数：{len(chunk)}")

    def _clear_output_dir(self, directory):
        """清空指定目录下的文件"""
        self.log.info("正在清空输出目录...")
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        self.log.info("输出目录已清空。")