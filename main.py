import sys
import os
from PySide6.QtWidgets import QApplication
from app.ui_main import MainWindow

# Suppress harmless DirectWrite warnings for legacy bitmap fonts (8514oem, Terminal, etc.)
os.environ.setdefault("QT_QPA_PLATFORM", "windows:fontengine=freetype")

def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
