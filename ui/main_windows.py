import sys
from PyQt6.QtWidgets import QMainWindow, QTabWidget, QVBoxLayout, QWidget
from ui.data_filter_tab import DataFilterTab
from ui.data_match_tab import DataMatchTab
from ui.match_and_split_tab import MatchAndSplitTab


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("4A数据处理工具")
        self.setGeometry(100, 100, 800, 600)

        # 创建一个 QWidget 作为中央控件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        # 创建一个 QVBoxLayout
        main_layout = QVBoxLayout(main_widget)

        # 创建一个 QTabWidget 来容纳不同的功能标签页
        self.tab_widget = QTabWidget()

        # 关键修改 1: 保存原始的stdout和stderr
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr

        # 添加数据筛选标签页
        self.data_filter_tab = DataFilterTab()
        self.tab_widget.addTab(self.data_filter_tab, "数据筛选")

        # 创建第二个选项卡：数据匹配
        self.match_tab = DataMatchTab()
        self.tab_widget.addTab(self.match_tab, "去重匹配")

        # 添加新的匹配分割标签页
        self.match_and_split_tab = MatchAndSplitTab()
        self.tab_widget.addTab(self.match_and_split_tab, "匹配分割")

        # 关键修改 2: 连接信号到槽函数
        self.tab_widget.currentChanged.connect(self.on_tab_changed)

        # 将 QTabWidget 添加到主布局中
        main_layout.addWidget(self.tab_widget)

        # 初始时，手动调用一次，将日志重定向到默认显示的选项卡
        self.on_tab_changed(self.tab_widget.currentIndex())

    def on_tab_changed(self, index):
        """当Tab切换时，将日志重定向到当前激活Tab的日志框"""
        current_tab = self.tab_widget.widget(index)

        # 恢复原始的stdout和stderr
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

        # 如果当前Tab有log_stream属性，就重定向到它的日志框
        if hasattr(current_tab, 'log_stream'):
            sys.stdout = current_tab.log_stream
            sys.stderr = current_tab.log_stream

        print(f"当前页面已切换至：{self.tab_widget.tabText(index)}")