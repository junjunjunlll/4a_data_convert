from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                             QComboBox, QFileDialog, QLineEdit, QMessageBox, QTextEdit, QApplication)
from PyQt6.QtGui import QIntValidator
from PyQt6.QtCore import QObject, pyqtSignal
from logic.data_filter import DataFilterLogic
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

        # 初始化业务逻辑层
        self.logic = DataFilterLogic()

        self.setup_ui()

        self.log_stream = Stream(self.log_output)
        sys.stdout = self.log_stream

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 操作模式选择开关
        operation_mode_layout = QHBoxLayout()
        self.operation_mode_label = QLabel("操作模式：")
        self.operation_mode_combo = QComboBox()
        self.operation_mode_combo.addItems(["筛选", "仅分页"])
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

        # 10. 输出目录和格式配置
        output_layout = QHBoxLayout()
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

        output_layout.addWidget(self.output_dir_label)
        output_layout.addWidget(self.output_dir_path)
        output_layout.addWidget(self.select_output_dir_button)
        output_layout.addWidget(self.output_format_label)
        output_layout.addWidget(self.output_format_combo)
        main_layout.addLayout(output_layout)

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
        self.on_operation_mode_changed(0)

    def on_operation_mode_changed(self, index):
        is_filter_mode = (index == 0)
        self.file_b_path_label.setVisible(is_filter_mode)
        self.select_file_b_button.setVisible(is_filter_mode)
        self.col_label_a.setVisible(is_filter_mode)
        self.col_a_combo.setVisible(is_filter_mode)
        self.match_mode_label.setVisible(is_filter_mode)
        self.match_mode_combo.setVisible(is_filter_mode)
        self.file_b_label.setVisible(is_filter_mode)
        self.filter_button.setText("开始筛选" if is_filter_mode else "开始分页")

    def on_mode_changed(self, index):
        self.is_dir_mode = (index == 1)
        self.select_source_button.setText("选择目录" if self.is_dir_mode else "选择文件a")
        self.file_a_path_label.setText("")
        self.file_a_path = ""
        self.col_a_combo.clear()

    def select_source(self):
        if self.is_dir_mode:
            dir_path = QFileDialog.getExistingDirectory(self, "选择目录")
            if dir_path:
                self.file_a_path = dir_path
                self.file_a_path_label.setText(dir_path)
        else:
            file_path, _ = QFileDialog.getOpenFileName(self, "选择文件a", "", "Files (*.csv *.xlsx *.xls)")
            if file_path:
                self.file_a_path = file_path
                self.file_a_path_label.setText(file_path)
        self.col_a_combo.clear()

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
        self.col_a_combo.clear()

        try:
            header_row = int(self.header_row_combo.currentText())
            column_headers = self.logic.get_source_columns(self.file_a_path, self.is_dir_mode, header_row)
            self.col_a_combo.addItems(column_headers)
            print("列标题读取成功！")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"读取文件a列标题失败：{e}")
            print(f"读取文件a列标题失败：{e}")

    def start_processing(self):
        self.filter_button.setEnabled(False)
        self.log_output.clear()

        # 收集所有参数
        params = {
            "is_filter_mode": self.operation_mode_combo.currentText() == "筛选",
            "file_a_path": self.file_a_path,
            "is_dir_mode": self.is_dir_mode,
            "file_b_path": self.file_b_path,
            "col_a": self.col_a_combo.currentText(),
            "match_mode": self.match_mode_combo.currentText(),
            "header_row": int(self.header_row_combo.currentText()),
            "page_size": int(self.page_size_input.text()),
            "output_dir": self.output_dir_path.text(),
            "output_format": self.output_format_combo.currentText()
        }

        try:
            self.logic.process_data(params)
            QMessageBox.information(self, "完成", "处理已全部完成！")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"处理失败：{e}")
            print(f"\n错误：{e}")
        finally:
            self.filter_button.setEnabled(True)