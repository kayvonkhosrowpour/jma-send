import sys
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from source.shared import common


class Window(QWidget):
    def __init__(self, is_valid, errors, *args, **kwargs):
        QWidget.__init__(self, *args, **kwargs)

        label_str = 'Config is {}.\n'.format('valid. See /logs for more info' if is_valid else 'INVALID')
        if not is_valid:
            label_str += '\n'.join(errors)

        self.label = QLabel(label_str, self)
        self.label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setStyleSheet('QLabel {background-color: %s;}' % ('green' if config else 'red'))

        self.button = QPushButton('OK', self)
        self.button.clicked.connect(self.close)

        self.layout = QGridLayout()
        self.layout.addWidget(self.label, 0, 0)
        self.layout.addWidget(self.button, 1, 0)

        self.setLayout(self.layout)
        self.show()


if __name__ == '__main__':
    # configure logging
    logging = common.setup_logging(__file__, './logs')

    # load config
    errors = []
    config = common.read_config('config.json', logging, errors)

    app = QApplication(sys.argv)
    win = Window(config is not None, errors)
    sys.exit(app.exec_())
