from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                             QComboBox, QFileDialog, QLineEdit, QMessageBox, QTextEdit, QApplication)
from PyQt6.QtGui import QIntValidator
from PyQt6.QtCore import QObject, pyqtSignal
from logic.data_filter import read_file_b_criteria, read_and_filter
import os
import sys
import pandas as pd


# 自定义一个流类，用于将stdout重定向到QTextEdit
class Stream(QObject):
    new_text_signal = pyqtSignal(str)

    def __init__(self, new_text_edit):
        super().__init__()
        self.text_edit = new_text_edit
        self.new_text_signal.connect(self.text_edit.append)
        self.current_line = ""

    def write(self, text):
        self.current_line += text
        if '\n' in self.current_line:
            lines = self.current_line.split('\n')
            self.new_text_signal.emit(lines[0])
            self.current_line = '\n'.join(lines[1:])

    def flush(self):
        pass


class DataFilterTab(QWidget):
    def __init__(self):
        super().__init__()
        self.file_a_path = ""
        self.is_dir_mode = False
        self.file_b_path = ""
        self.file_a_cols = []

        # 定义需要动态隐藏的UI元素
        self.file_b_layout = None
        self.col_a_layout = None
        self.match_mode_layout = None

        self.setup_ui()

        self.log_stream = Stream(self.log_output)

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 操作模式选择开关 (新增)
        operation_mode_layout = QHBoxLayout()
        self.operation_mode_label = QLabel("操作模式：")
        self.operation_mode_combo = QComboBox()
        self.operation_mode_combo.addItems(["筛选", "仅分页"])
        # 绑定模式切换事件
        self.operation_mode_combo.currentIndexChanged.connect(self.on_operation_mode_changed)
        operation_mode_layout.addWidget(self.operation_mode_label)
        operation_mode_layout.addWidget(self.operation_mode_combo)
        main_layout.addLayout(operation_mode_layout)

        # 2. 文件/目录模式选择
        mode_layout = QHBoxLayout()
        self.mode_label = QLabel("数据来源模式：")
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["文件", "目录"])
        self.mode_combo.currentIndexChanged.connect(self.on_mode_changed)
        mode_layout.addWidget(self.mode_label)
        mode_layout.addWidget(self.mode_combo)
        main_layout.addLayout(mode_layout)

        # 3. 文件a/目录选择
        a_layout = QHBoxLayout()
        self.file_a_label = QLabel("待处理数据来源：")
        self.file_a_path_label = QLineEdit()
        self.file_a_path_label.setReadOnly(True)
        self.select_source_button = QPushButton("选择文件a")
        self.select_source_button.clicked.connect(self.select_source)
        a_layout.addWidget(self.file_a_label)
        a_layout.addWidget(self.file_a_path_label)
        a_layout.addWidget(self.select_source_button)
        main_layout.addLayout(a_layout)

        # 4. 标题行数
        b_layout = QHBoxLayout()
        self.header_row_label = QLabel("标题行数：")
        self.header_row_combo = QComboBox()
        self.header_row_combo.addItems([str(i) for i in range(1, 6)])
        b_layout.addWidget(self.header_row_label)
        b_layout.addWidget(self.header_row_combo)
        main_layout.addLayout(b_layout)

        # 5. 读取列标题
        c_layout = QHBoxLayout()
        self.read_cols_button = QPushButton("读取标题列")
        self.read_cols_button.clicked.connect(self.read_source_columns)
        c_layout.addWidget(self.read_cols_button)
        main_layout.addLayout(c_layout)

        # 6. 下拉框用于选择文件a的列 (筛选模式下需要)
        self.col_a_layout = QHBoxLayout()
        self.col_label_a = QLabel("文件a筛选列：")
        self.col_a_combo = QComboBox()
        self.col_a_layout.addWidget(self.col_label_a)
        self.col_a_layout.addWidget(self.col_a_combo)
        main_layout.addLayout(self.col_a_layout)

        # 7. 文件b选择 (筛选模式下需要)
        self.file_b_layout = QHBoxLayout()
        self.file_b_label = QLabel("筛选数据来源文件（文件b）：")
        self.file_b_path_label = QLineEdit()
        self.file_b_path_label.setReadOnly(True)
        self.select_file_b_button = QPushButton("选择文件b")
        self.select_file_b_button.clicked.connect(self.select_file_b)
        self.file_b_layout.addWidget(self.file_b_label)
        self.file_b_layout.addWidget(self.file_b_path_label)
        self.file_b_layout.addWidget(self.select_file_b_button)
        main_layout.addLayout(self.file_b_layout)

        # 8. 匹配模式 (筛选模式下需要)
        self.match_mode_layout = QHBoxLayout()
        self.match_mode_label = QLabel("匹配模式：")
        self.match_mode_combo = QComboBox()
        self.match_mode_combo.addItems(["精确匹配", "包含匹配", "前缀匹配", "后缀匹配"])
        self.match_mode_layout.addWidget(self.match_mode_label)
        self.match_mode_layout.addWidget(self.match_mode_combo)
        main_layout.addLayout(self.match_mode_layout)

        # 9. 分页大小配置
        page_layout = QHBoxLayout()
        self.page_size_label = QLabel("分页大小（条）：")
        self.page_size_input = QLineEdit("1000000")
        self.page_size_input.setValidator(QIntValidator())
        page_layout.addWidget(self.page_size_label)
        page_layout.addWidget(self.page_size_input)
        main_layout.addLayout(page_layout)

        # 10. 输出目录配置
        output_dir_layout = QHBoxLayout()
        self.output_dir_label = QLabel("输出目录：")

        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)
        else:
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        default_output_dir = os.path.join(base_path, 'output')

        self.output_dir_path = QLineEdit(default_output_dir)
        self.output_dir_path.setReadOnly(True)
        self.select_output_dir_button = QPushButton("选择目录")
        self.select_output_dir_button.clicked.connect(self.select_output_dir)
        output_dir_layout.addWidget(self.output_dir_label)
        output_dir_layout.addWidget(self.output_dir_path)
        output_dir_layout.addWidget(self.select_output_dir_button)
        main_layout.addLayout(output_dir_layout)

        # 11. 筛选/分页按钮
        g_layout = QHBoxLayout()
        self.filter_button = QPushButton("开始处理")
        self.filter_button.clicked.connect(self.start_processing)
        g_layout.addWidget(self.filter_button)
        main_layout.addLayout(g_layout)

        # 12. 日志输出文本框
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        main_layout.addWidget(self.log_output)

        main_layout.addStretch()

        os.makedirs(self.output_dir_path.text(), exist_ok=True)

        # 默认模式下隐藏部分UI
        self.on_operation_mode_changed(0)  # 初始设置为"筛选"模式

    # 新增方法: 根据操作模式切换UI可见性
    def on_operation_mode_changed(self, index):
        if index == 0:  # "筛选"模式
            self.file_b_label.setVisible(True)
            self.file_b_path_label.setVisible(True)
            self.select_file_b_button.setVisible(True)
            self.col_label_a.setVisible(True)
            self.col_a_combo.setVisible(True)
            self.match_mode_label.setVisible(True)
            self.match_mode_combo.setVisible(True)
            self.filter_button.setText("开始筛选")
        else:  # "仅分页"模式
            self.file_b_label.setVisible(False)
            self.file_b_path_label.setVisible(False)
            self.select_file_b_button.setVisible(False)
            self.col_label_a.setVisible(False)
            self.col_a_combo.setVisible(False)
            self.match_mode_label.setVisible(False)
            self.match_mode_combo.setVisible(False)
            self.filter_button.setText("开始分页")

    def on_mode_changed(self, index):
        if index == 0:
            self.is_dir_mode = False
            self.select_source_button.setText("选择文件a")
            self.file_a_path_label.setText("")
            self.file_a_path = ""
        else:
            self.is_dir_mode = True
            self.select_source_button.setText("选择目录")
            self.file_a_path_label.setText("")
            self.file_a_path = ""

    def select_source(self):
        if self.is_dir_mode:
            dir_path = QFileDialog.getExistingDirectory(self, "选择目录")
            if dir_path:
                self.file_a_path = dir_path
                self.file_a_path_label.setText(dir_path)
                self.col_a_combo.clear()
        else:
            file_path, _ = QFileDialog.getOpenFileName(self, "选择文件a", "", "Files (*.csv *.xlsx *.xls)")
            if file_path:
                self.file_a_path = file_path
                self.file_a_path_label.setText(file_path)
                self.read_source_columns()

    def select_file_b(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "选择文件b", "", "Files (*.csv *.xlsx *.xls)")
        if file_path:
            self.file_b_path = file_path
            self.file_b_path_label.setText(file_path)

    def select_output_dir(self):
        dir_path = QFileDialog.getExistingDirectory(self, "选择输出目录")
        if dir_path:
            self.output_dir_path.setText(dir_path)

    def read_source_columns(self):
        if not self.file_a_path:
            QMessageBox.warning(self, "警告", "请先选择文件或目录！")
            return

        self.log_output.clear()
        print("正在读取文件a的列标题...")
        file_to_read = self.file_a_path
        if self.is_dir_mode:
            files = [f for f in os.listdir(self.file_a_path) if f.endswith(('.csv', '.xlsx', '.xls'))]
            if not files:
                QMessageBox.warning(self, "警告", "所选目录中没有找到可用的CSV或Excel文件！")
                print("未找到有效文件，操作终止。")
                return
            file_to_read = os.path.join(self.file_a_path, files[0])

        try:
            header_row = int(self.header_row_combo.currentText()) - 1
            if file_to_read.endswith('.csv'):
                df = pd.read_csv(file_to_read, header=header_row, nrows=0, dtype=str)
            else:
                df = pd.read_excel(file_to_read, header=header_row, nrows=0)

            self.file_a_cols = [str(col) for col in list(df.columns)]

            self.col_a_combo.clear()
            self.col_a_combo.addItems(self.file_a_cols)
            print("列标题读取成功！")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"读取文件a列标题失败：{e}")
            print(f"读取文件a列标题失败：{e}")

    # 新增统一处理入口，根据操作模式调用不同的方法
    def start_processing(self):
        mode = self.operation_mode_combo.currentText()
        if mode == "筛选":
            self.start_filter()
        elif mode == "仅分页":
            self.start_pagination_only()

    def start_filter(self):
        self.filter_button.setEnabled(False)
        self.log_output.clear()

        if not self.file_a_path or not self.file_b_path:
            QMessageBox.warning(self, "警告", "请选择文件/目录和筛选文件！")
            self.filter_button.setEnabled(True)
            return

        col_a = self.col_a_combo.currentText()
        match_mode = self.match_mode_combo.currentText()
        header_row = int(self.header_row_combo.currentText()) - 1
        page_size_str = self.page_size_input.text()

        if not col_a:
            QMessageBox.warning(self, "警告", "请选择文件a的筛选列！")
            self.filter_button.setEnabled(True)
            return

        if not page_size_str or not page_size_str.isdigit() or int(page_size_str) <= 0:
            QMessageBox.warning(self, "警告", "请填写有效的分页大小！")
            self.filter_button.setEnabled(True)
            return

        page_size = int(page_size_str)
        output_dir = self.output_dir_path.text()

        try:
            print("--- 开始筛选过程 ---")
            print(f"输出目录: {output_dir}")
            os.makedirs(output_dir, exist_ok=True)

            header_row_b = int(self.header_row_combo.currentText()) - 1
            print(f"正在读取筛选条件文件（{self.file_b_path}）...")
            filter_criteria = read_file_b_criteria(self.file_b_path, header_row=header_row_b)
            print(f"筛选条件共 {len(filter_criteria)} 条。")

            print("正在清空输出目录...")
            for filename in os.listdir(output_dir):
                file_path = os.path.join(output_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            print("输出目录已清空。")

            filtered_data_all = pd.DataFrame()
            output_file_index = 1
            total_records_processed = 0
            total_filtered_records = 0

            if self.is_dir_mode:
                files_to_process = [os.path.join(self.file_a_path, f) for f in os.listdir(self.file_a_path) if
                                    f.endswith(('.csv', '.xlsx', '.xls'))]
            else:
                files_to_process = [self.file_a_path]

            if not files_to_process:
                QMessageBox.warning(self, "警告", "没有找到需要处理的文件！")
                self.filter_button.setEnabled(True)
                return

            total_files = len(files_to_process)
            print(f"共找到 {total_files} 个文件需要处理。")

            for i, full_path_a in enumerate(files_to_process):
                file_name = os.path.basename(full_path_a)
                print(f"[{i + 1}/{total_files}] 正在处理文件：{file_name}")

                df_filtered = read_and_filter(
                    file_a=full_path_a,
                    filter_criteria=filter_criteria,
                    col_a=col_a,
                    match_mode=match_mode,
                    header_row=header_row
                )

                if df_filtered is not None and not df_filtered.empty:
                    try:
                        original_df = pd.read_excel(full_path_a, header=None) if full_path_a.endswith(
                            ('.xlsx', '.xls')) else pd.read_csv(full_path_a, header=None)
                        original_rows = len(original_df)
                    except Exception as e:
                        original_rows = "未知"
                        print(f"  - 无法获取文件总行数: {e}")

                    filtered_rows = len(df_filtered)
                    print(f"  - 原文件记录数：{original_rows}，筛选保留记录数：{filtered_rows}")
                    if isinstance(original_rows, int):
                        total_records_processed += original_rows
                    total_filtered_records += filtered_rows

                    filtered_data_all = pd.concat([filtered_data_all, df_filtered], ignore_index=True)

                    while len(filtered_data_all) >= page_size:
                        filtered_chunk = filtered_data_all.iloc[:page_size]
                        filtered_data_all = filtered_data_all.iloc[page_size:]

                        output_file = os.path.join(output_dir, f"filtered_part_{output_file_index}.xlsx")
                        filtered_chunk.to_excel(output_file, index=False)
                        print(f"  - 【输出文件】已输出第 {output_file_index} 个分页文件，记录数：{len(filtered_chunk)}")
                        output_file_index += 1

            if not filtered_data_all.empty:
                output_file = os.path.join(output_dir, f"filtered_part_{output_file_index}.xlsx")
                filtered_data_all.to_excel(output_file, index=False)
                print(
                    f"  - 【输出文件】已输出第 {output_file_index} 个分页文件（剩余数据），记录数：{len(filtered_data_all)}")

            print("\n--- 筛选过程总结 ---")
            print(f"处理文件总数: {total_files}")
            print(f"总记录数: {total_records_processed}")
            print(f"筛选保留总记录数: {total_filtered_records}")
            print(f"筛选丢弃总记录数: {total_records_processed - total_filtered_records}")

            QMessageBox.information(self, "完成", "数据筛选和分页已全部完成！")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"筛选失败：{e}")
            print(f"\n错误：{e}")
        finally:
            self.filter_button.setEnabled(True)

    # 新增方法: 仅分页逻辑
    def start_pagination_only(self):
        self.filter_button.setEnabled(False)
        self.log_output.clear()

        if not self.file_a_path:
            QMessageBox.warning(self, "警告", "请选择文件/目录！")
            self.filter_button.setEnabled(True)
            return

        page_size_str = self.page_size_input.text()
        if not page_size_str or not page_size_str.isdigit() or int(page_size_str) <= 0:
            QMessageBox.warning(self, "警告", "请填写有效的分页大小！")
            self.filter_button.setEnabled(True)
            return

        page_size = int(page_size_str)
        output_dir = self.output_dir_path.text()

        try:
            print("--- 开始仅分页过程 ---")
            print(f"输出目录: {output_dir}")
            os.makedirs(output_dir, exist_ok=True)

            print("正在清空输出目录...")
            for filename in os.listdir(output_dir):
                file_path = os.path.join(output_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            print("输出目录已清空。")

            total_records_processed = 0
            output_file_index = 1
            header_row = int(self.header_row_combo.currentText()) - 1

            if self.is_dir_mode:
                files_to_process = [os.path.join(self.file_a_path, f) for f in os.listdir(self.file_a_path) if
                                    f.endswith(('.csv', '.xlsx', '.xls'))]
            else:
                files_to_process = [self.file_a_path]

            if not files_to_process:
                QMessageBox.warning(self, "警告", "没有找到需要处理的文件！")
                self.filter_button.setEnabled(True)
                return

            total_files = len(files_to_process)
            print(f"共找到 {total_files} 个文件需要处理。")

            # 使用列表来累积数据，确保分页逻辑正确
            all_data_to_page = pd.DataFrame()

            for i, full_path_a in enumerate(files_to_process):
                file_name = os.path.basename(full_path_a)
                print(f"[{i + 1}/{total_files}] 正在处理文件：{file_name}")

                if full_path_a.endswith('.csv'):
                    df = pd.read_csv(full_path_a, header=header_row, dtype='str', encoding='utf-8')
                else:
                    df = pd.read_excel(full_path_a, header=header_row)

                total_records_processed += len(df)
                all_data_to_page = pd.concat([all_data_to_page, df], ignore_index=True)

            print(f"已加载所有文件，总记录数: {total_records_processed}")

            # 统一对所有数据进行分页输出
            if not all_data_to_page.empty:
                num_chunks = (len(all_data_to_page) + page_size - 1) // page_size
                for i in range(num_chunks):
                    start_index = i * page_size
                    end_index = start_index + page_size
                    chunk = all_data_to_page.iloc[start_index:end_index]

                    output_file = os.path.join(output_dir, f"paged_part_{i + 1}.xlsx")
                    chunk.to_excel(output_file, index=False)
                    print(f"  - 【输出文件】已输出第 {i + 1} 个分页文件，记录数：{len(chunk)}")

            print("\n--- 分页过程总结 ---")
            print(f"处理文件总数: {total_files}")
            print(f"总记录数: {total_records_processed}")
            QMessageBox.information(self, "完成", "数据分页已全部完成！")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"分页失败：{e}")
            print(f"\n错误：{e}")
        finally:
            self.filter_button.setEnabled(True)