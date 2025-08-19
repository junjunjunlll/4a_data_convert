import os
import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QRadioButton, QFileDialog,
    QPushButton, QComboBox, QLabel, QLineEdit, QHBoxLayout, QMessageBox,
    QProgressBar, QTextEdit
)
from PyQt6.QtCore import QThread, pyqtSignal, QObject

# 从逻辑层导入业务逻辑
from logic.match_and_split import MatchAndSplitProcessor


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


# 后台工作线程，用于执行耗时操作
class WorkerThread(QThread):
    task_finished = pyqtSignal()
    task_error = pyqtSignal(str)

    def __init__(self, processor, params):
        super().__init__()
        self.processor = processor
        self.params = params

    def run(self):
        try:
            self.processor.process_and_export(**self.params)
            self.task_finished.emit()
        except Exception as e:
            self.task_error.emit(str(e))


class MatchAndSplitTab(QWidget):
    def __init__(self):
        super().__init__()
        self.processor = MatchAndSplitProcessor()
        self.worker_thread = None
        self.setup_ui()

        # 实例化 Stream 类，但不再在这里执行重定向
        self.log_stream = Stream(self.log_textedit)

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 布局：源文件和列设置
        source_groupbox = QGroupBox("源文件和列设置")
        source_layout = QVBoxLayout()

        # a. 模式选择（文件/目录）
        mode_layout = QHBoxLayout()
        self.file_mode_radio = QRadioButton("文件")
        self.dir_mode_radio = QRadioButton("目录")
        self.file_mode_radio.setChecked(True)
        self.file_mode_radio.toggled.connect(self.toggle_source_mode)
        self.source_path_lineedit = QLineEdit()
        self.select_source_button = QPushButton("选择文件a")
        self.select_source_button.clicked.connect(self.select_source)

        mode_layout.addWidget(self.file_mode_radio)
        mode_layout.addWidget(self.dir_mode_radio)
        mode_layout.addStretch()
        source_layout.addLayout(mode_layout)
        source_layout.addWidget(QLabel("文件/目录路径:"))
        source_layout.addWidget(self.source_path_lineedit)
        source_layout.addWidget(self.select_source_button)

        # b, c, d. 标题行和列选择
        col_layout = QHBoxLayout()
        col_layout.addWidget(QLabel("标题行数:"))
        self.header_row_combo = QComboBox()
        self.header_row_combo.addItems([str(i) for i in range(1, 6)])
        self.header_row_combo.setCurrentIndex(0)
        self.read_cols_button = QPushButton("读取标题列")
        self.read_cols_button.clicked.connect(self.read_source_columns)
        self.col_label = QLabel("匹配列:")
        self.col_combo = QComboBox()

        col_layout.addWidget(self.header_row_combo)
        col_layout.addWidget(self.read_cols_button)
        col_layout.addWidget(self.col_label)
        col_layout.addWidget(self.col_combo)
        source_layout.addLayout(col_layout)

        source_groupbox.setLayout(source_layout)
        main_layout.addWidget(source_groupbox)

        # 布局：映射和输出设置
        config_groupbox = QGroupBox("映射和输出设置")
        config_layout = QVBoxLayout()

        # e. 映射文件选择
        mapping_layout = QHBoxLayout()
        mapping_layout.addWidget(QLabel("映射文件（文件B）:"))
        self.mapping_path_lineedit = QLineEdit()
        self.select_mapping_button = QPushButton("选择文件b")
        self.select_mapping_button.clicked.connect(self.select_mapping_file)
        mapping_layout.addWidget(self.mapping_path_lineedit)
        mapping_layout.addWidget(self.select_mapping_button)
        config_layout.addLayout(mapping_layout)

        # f. 输出模式和分割行数
        output_mode_layout = QHBoxLayout()
        self.single_output_radio = QRadioButton("单个文件输出")
        self.split_output_radio = QRadioButton("分割输出")
        self.single_output_radio.setChecked(True)
        self.split_output_radio.toggled.connect(self.toggle_split_output)
        self.split_row_count_label = QLabel("单页行数:")
        self.split_row_count_lineedit = QLineEdit("500000")
        self.split_row_count_lineedit.setEnabled(False)

        output_mode_layout.addWidget(self.single_output_radio)
        output_mode_layout.addWidget(self.split_output_radio)
        output_mode_layout.addStretch()
        output_mode_layout.addWidget(self.split_row_count_label)
        output_mode_layout.addWidget(self.split_row_count_lineedit)
        config_layout.addLayout(output_mode_layout)

        # h. 输出文件格式和目录
        output_format_layout = QHBoxLayout()
        output_format_layout.addWidget(QLabel("输出格式:"))
        self.xlsx_radio = QRadioButton("xlsx")
        self.csv_radio = QRadioButton("csv")
        self.xlsx_radio.setChecked(True)
        output_format_layout.addWidget(self.xlsx_radio)
        output_format_layout.addWidget(self.csv_radio)
        output_format_layout.addStretch()

        output_dir_layout = QHBoxLayout()
        output_dir_layout.addWidget(QLabel("输出目录:"))
        default_output_dir = os.path.join(os.getcwd(), 'output')
        self.output_dir_lineedit = QLineEdit(default_output_dir)
        self.select_output_dir_button = QPushButton("选择目录")
        self.select_output_dir_button.clicked.connect(self.select_output_dir)
        output_dir_layout.addWidget(self.output_dir_lineedit)
        output_dir_layout.addWidget(self.select_output_dir_button)

        config_layout.addLayout(output_format_layout)
        config_layout.addLayout(output_dir_layout)

        config_groupbox.setLayout(config_layout)
        main_layout.addWidget(config_groupbox)

        # g. 执行按钮和日志
        self.execute_button = QPushButton("开始匹配和导出")
        self.execute_button.clicked.connect(self.start_process)
        main_layout.addWidget(self.execute_button)

        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        main_layout.addWidget(self.log_textedit)

        main_layout.addStretch()

    def toggle_source_mode(self, checked):
        if checked:
            self.select_source_button.setText("选择文件a")
        else:
            self.select_source_button.setText("选择目录")

    def toggle_split_output(self, checked):
        self.split_row_count_lineedit.setEnabled(checked)

    def select_source(self):
        if self.file_mode_radio.isChecked():
            file_path, _ = QFileDialog.getOpenFileName(self, "选择文件a", "", "Files (*.csv *.xlsx *.xls)")
            if file_path:
                self.source_path_lineedit.setText(file_path)
                self.read_source_columns()
        else:
            dir_path = QFileDialog.getExistingDirectory(self, "选择目录")
            if dir_path:
                self.source_path_lineedit.setText(dir_path)
                self.read_source_columns()

    def select_mapping_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "选择文件b", "", "Files (*.csv *.xlsx *.xls)")
        if file_path:
            self.mapping_path_lineedit.setText(file_path)

    def select_output_dir(self):
        dir_path = QFileDialog.getExistingDirectory(self, "选择输出目录")
        if dir_path:
            self.output_dir_lineedit.setText(dir_path)

    def read_source_columns(self):
        source_path = self.source_path_lineedit.text()
        if not source_path:
            QMessageBox.warning(self, "警告", "请先选择源文件或目录。")
            return

        self.log_textedit.clear()
        try:
            header_row = int(self.header_row_combo.currentText())
            headers = self.processor.load_source_files(source_path, header_row)
            self.col_combo.clear()
            self.col_combo.addItems(headers)
            print("列标题读取成功。")
        except Exception as e:
            QMessageBox.critical(self, "错误", str(e))
            self.col_combo.clear()

    def start_process(self):
        source_path = self.source_path_lineedit.text()
        mapping_path = self.mapping_path_lineedit.text()
        output_dir = self.output_dir_lineedit.text()
        col_a = self.col_combo.currentText()

        if not all([source_path, mapping_path, output_dir, col_a]):
            QMessageBox.warning(self, "警告", "请确保所有必填项都已填写。")
            return

        try:
            self.processor.set_output_dir(output_dir)
            self.processor.load_mapping_file(mapping_path)
        except Exception as e:
            QMessageBox.critical(self, "错误", str(e))
            return

        output_mode = 'single_file' if self.single_output_radio.isChecked() else 'split_output'
        split_row_count = int(self.split_row_count_lineedit.text()) if output_mode == 'split_output' else 0
        output_format = 'xlsx' if self.xlsx_radio.isChecked() else 'csv'

        params = {
            'col_a': col_a,
            'output_mode': output_mode,
            'split_row_count': split_row_count,
            'output_format': output_format
        }

        self.execute_button.setEnabled(False)
        self.log_textedit.clear()
        print("开始执行...")

        self.worker_thread = WorkerThread(self.processor, params)
        self.worker_thread.task_finished.connect(self.process_finished)
        self.worker_thread.task_error.connect(self.process_error)
        self.worker_thread.start()

    def process_finished(self):
        self.execute_button.setEnabled(True)
        print("所有任务已完成！")
        QMessageBox.information(self, "完成", "数据匹配和导出已完成！")

    def process_error(self, message):
        self.execute_button.setEnabled(True)
        print(f"执行失败: {message}")
        QMessageBox.critical(self, "执行错误", message)