from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                             QComboBox, QFileDialog, QLineEdit, QTextEdit, QMessageBox, QGroupBox)
from PyQt6.QtCore import QObject, pyqtSignal
# 从 logic.data_match 导入更新后的函数
from logic.data_match import get_unique_values, fuzzy_match_and_fill, export_match_results
import os
import sys
import pandas as pd
from PyQt6.QtWidgets import QApplication
from logic.utils import read_file


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


class DataMatchTab(QWidget):
    def __init__(self):
        super().__init__()
        self.file_a_path = ""
        self.is_dir_mode = False
        self.file_a_cols = []
        self.unique_values = set()
        self.file_b_path = ""
        self.matched_results = {}
        self.unmatched_values = set()

        self.setup_ui()

        # 在这里只实例化 Stream 对象，不进行全局重定向
        # 日志重定向将在主窗口中动态完成
        self.log_stream = Stream(self.log_output)

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 模式选择
        mode_layout = QHBoxLayout()
        self.mode_label = QLabel("模式选择：")
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["文件", "目录"])
        self.mode_combo.currentIndexChanged.connect(self.on_mode_changed)
        mode_layout.addWidget(self.mode_label)
        mode_layout.addWidget(self.mode_combo)
        main_layout.addLayout(mode_layout)

        # a. 文件a/目录选择
        a_layout = QHBoxLayout()
        self.file_a_label = QLabel("数据来源：")
        self.file_a_path_label = QLineEdit()
        self.file_a_path_label.setReadOnly(True)
        self.select_source_button = QPushButton("选择文件a")
        self.select_source_button.clicked.connect(self.select_source)
        a_layout.addWidget(self.file_a_label)
        a_layout.addWidget(self.file_a_path_label)
        a_layout.addWidget(self.select_source_button)
        main_layout.addLayout(a_layout)

        # b. 标题行数
        b_layout = QHBoxLayout()
        self.header_row_label = QLabel("标题行数：")
        self.header_row_combo = QComboBox()
        self.header_row_combo.addItems([str(i) for i in range(1, 6)])
        b_layout.addWidget(self.header_row_label)
        b_layout.addWidget(self.header_row_combo)
        main_layout.addLayout(b_layout)

        # c. 读取列标题
        c_layout = QHBoxLayout()
        self.read_cols_button = QPushButton("读取标题列")
        self.read_cols_button.clicked.connect(self.read_source_columns)
        c_layout.addWidget(self.read_cols_button)
        main_layout.addLayout(c_layout)

        # d. 下拉框用于选择列
        d_layout = QHBoxLayout()
        self.col_label_a = QLabel("匹配列：")
        self.col_a_combo = QComboBox()
        d_layout.addWidget(self.col_label_a)
        d_layout.addWidget(self.col_a_combo)
        main_layout.addLayout(d_layout)

        # e. 读取去重数据到DataFrame
        e_layout = QHBoxLayout()
        self.load_unique_button = QPushButton("加载去重数据")
        self.load_unique_button.clicked.connect(self.load_unique_data)
        e_layout.addWidget(self.load_unique_button)
        main_layout.addLayout(e_layout)

        # f. 模糊匹配文件b选择
        f_layout = QHBoxLayout()
        self.file_b_label = QLabel("匹配关系文件（文件b）：")
        self.file_b_path_label = QLineEdit()
        self.file_b_path_label.setReadOnly(True)
        self.select_file_b_button = QPushButton("选择文件b")
        self.select_file_b_button.clicked.connect(self.select_file_b)
        f_layout.addWidget(self.file_b_label)
        f_layout.addWidget(self.file_b_path_label)
        f_layout.addWidget(self.select_file_b_button)
        main_layout.addLayout(f_layout)

        # === 新增分隔符选择 UI ===
        separator_layout = QHBoxLayout()
        self.old_separator_label = QLabel("原分隔符：")
        self.old_separator_combo = QComboBox()
        self.old_separator_combo.addItems(['/', '&', '|', '\\', '(无)'])
        self.old_separator_combo.setCurrentText('(无)')

        self.new_separator_label = QLabel("目标分隔符：")
        self.new_separator_combo = QComboBox()
        self.new_separator_combo.addItems(['/', '&', '|', '\\', '(无)'])
        self.new_separator_combo.setCurrentText('/')

        separator_layout.addWidget(self.old_separator_label)
        separator_layout.addWidget(self.old_separator_combo)
        separator_layout.addWidget(self.new_separator_label)
        separator_layout.addWidget(self.new_separator_combo)
        main_layout.addLayout(separator_layout)

        # 匹配按钮
        g_layout = QHBoxLayout()
        self.match_button = QPushButton("开始匹配")
        self.match_button.clicked.connect(self.start_match)
        g_layout.addWidget(self.match_button)
        main_layout.addLayout(g_layout)

        # 新增：输出目录和格式配置
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

        self.output_format_label = QLabel("输出格式：")
        self.output_format_combo = QComboBox()
        self.output_format_combo.addItems(["xlsx", "csv"])

        output_dir_layout.addWidget(self.output_dir_label)
        output_dir_layout.addWidget(self.output_dir_path)
        output_dir_layout.addWidget(self.select_output_dir_button)
        output_dir_layout.addWidget(self.output_format_label)
        output_dir_layout.addWidget(self.output_format_combo)
        main_layout.addLayout(output_dir_layout)

        # 新增：输出按钮
        output_button_layout = QHBoxLayout()
        self.export_button = QPushButton("导出匹配结果")
        self.export_button.setEnabled(False)
        self.export_button.clicked.connect(self.export_results)
        output_button_layout.addWidget(self.export_button)
        main_layout.addLayout(output_button_layout)

        # 日志输出文本框
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        main_layout.addWidget(self.log_output)

        main_layout.addStretch()

        os.makedirs(self.output_dir_path.text(), exist_ok=True)

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
            df = read_file(file_to_read, header_row=header_row)

            self.file_a_cols = [str(col) for col in list(df.columns)]
            self.col_a_combo.clear()
            self.col_a_combo.addItems(self.file_a_cols)
            print("列标题读取成功！")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"读取文件a列标题失败：{e}")
            print(f"读取文件a列标题失败：{e}")

    def load_unique_data(self):
        self.log_output.clear()
        self.export_button.setEnabled(False)

        if not self.file_a_path:
            QMessageBox.warning(self, "警告", "请先选择数据来源！")
            return

        col_a = self.col_a_combo.currentText()
        if not col_a:
            QMessageBox.warning(self, "警告", "请选择匹配列！")
            return

        header_row = int(self.header_row_combo.currentText()) - 1

        try:
            self.unique_values = get_unique_values(self.file_a_path, self.is_dir_mode, header_row, col_a)
            print(f"\n成功加载去重数据。总计 {len(self.unique_values)} 条唯一值。")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"加载去重数据失败：{e}")
            print(f"加载去重数据失败：{e}")

    def start_match(self):
        self.log_output.clear()
        self.export_button.setEnabled(False)

        if not self.unique_values:
            QMessageBox.warning(self, "警告", "请先加载去重数据！")
            return

        if not self.file_b_path:
            QMessageBox.warning(self, "警告", "请选择匹配关系文件（文件b）！")
            return

        old_sep = self.old_separator_combo.currentText()
        new_sep = self.new_separator_combo.currentText()

        if old_sep == '(无)':
            old_sep = None
        if new_sep == '(无)':
            new_sep = None

        try:
            print("--- 开始进行数据匹配 ---")
            self.matched_results, self.unmatched_values = fuzzy_match_and_fill(
                self.unique_values, self.file_b_path, old_separator=old_sep, new_separator=new_sep
            )

            if self.matched_results or self.unmatched_values:
                self.export_button.setEnabled(True)

            QMessageBox.information(self, "完成", "数据匹配已完成！请检查日志，或点击导出按钮。")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"匹配失败：{e}")
            print(f"匹配失败：{e}")

    def export_results(self):
        if not self.matched_results and not self.unmatched_values:
            QMessageBox.warning(self, "警告", "没有匹配结果可以导出！")
            return

        output_dir = self.output_dir_path.text()
        output_format = self.output_format_combo.currentText()
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        try:
            export_match_results(self.matched_results, self.unmatched_values, output_dir, output_format)
            QMessageBox.information(self, "成功", f"结果已成功导出到：\n{output_dir}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败：{e}")