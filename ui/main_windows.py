from PyQt6.QtWidgets import QMainWindow, QTabWidget, QVBoxLayout, QWidget
from ui.data_filter_tab import DataFilterTab


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("数据处理")
        self.setGeometry(100, 100, 800, 600)

        # 创建一个 QWidget 作为中央控件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        # 创建一个 QVBoxLayout
        main_layout = QVBoxLayout(main_widget)

        # 创建一个 QTabWidget 来容纳不同的功能标签页
        self.tab_widget = QTabWidget()

        # 添加数据筛选标签页
        self.data_filter_tab = DataFilterTab()
        self.tab_widget.addTab(self.data_filter_tab, "数据筛选")

        # 将 QTabWidget 添加到主布局中
        main_layout.addWidget(self.tab_widget)