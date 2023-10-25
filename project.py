from PyQt6.QtWidgets import QApplication
from interface import DatabaseInputForm
import sys

if __name__ == '__main__':
    app = QApplication(sys.argv)
    print("GUI Running...")
    window = DatabaseInputForm()
    sys.exit(app.exec())