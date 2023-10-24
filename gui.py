import psycopg2
import sys
from PyQt6.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QTextEdit, QScrollArea

class DatabaseInputForm(QWidget):
    def __init__(self):
        super().__init__()

        self.init_ui()

    def init_ui(self):
        self.lbl_heading = QLabel("Connect to the Database")
        self.lbl_heading.setStyleSheet("font-size: 20pt; font-weight: bold;")

        self.lbl_instructions = QLabel(f"Input your database name, user, password, host and port.\nIf you want to use the default database, input as per the placeholder.")
        self.lbl_instructions.setStyleSheet("font-size: 16pt;")
    
        self.lbl_db = QLabel('Database:')
        self.lbl_user = QLabel('User:')
        self.lbl_host = QLabel('Host:')
        self.lbl_password = QLabel('Password:')
        self.lbl_port = QLabel('Port:')
        self.lbl_result = QLabel('Connection details will be displayed here after connecting.')


        self.edit_db = QLineEdit()
        self.edit_db.setPlaceholderText('postgres')
        self.edit_user = QLineEdit()
        self.edit_user.setPlaceholderText('')
        self.edit_password = QLineEdit()
        self.edit_password.setPlaceholderText('postgres')
        self.edit_host = QLineEdit()
        self.edit_host.setPlaceholderText('0.0.0.0')
        self.edit_port = QLineEdit()
        self.edit_port.setPlaceholderText('5432')

        self.btn_connect = QPushButton('Connect', self)
        self.btn_connect.clicked.connect(self.connect_to_database)

        # Set up the layout
        layout = QVBoxLayout()
        layout.addWidget(self.lbl_heading)
        layout.addWidget(self.lbl_instructions)
        layout.addWidget(self.lbl_db)
        layout.addWidget(self.edit_db)
        layout.addWidget(self.lbl_user)
        layout.addWidget(self.edit_user)
        layout.addWidget(self.lbl_password)
        layout.addWidget(self.edit_password)
        layout.addWidget(self.lbl_host)
        layout.addWidget(self.edit_host)
        layout.addWidget(self.lbl_port)
        layout.addWidget(self.edit_port)
        layout.addWidget(self.btn_connect)
        layout.addWidget(self.lbl_result)


        self.setLayout(layout)

        # Set up the window
        self.setGeometry(300, 300, 300, 200)
        self.setWindowTitle('Database Connection Input')
        self.show()

    def connect_to_database(self):
        database = self.edit_db.text()
        user = self.edit_user.text()
        password = self.edit_password.text()
        host = self.edit_host.text()
        port = self.edit_port.text()

        result_text = f"Connecting to Database: {database}\nUser: {user}\nPassword: {password}\nHost: {host}\nPort: {port}"
        self.lbl_result.setText(result_text)

        try:
            con = psycopg2.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.lbl_result.setText(f"Connected to database: {database}@{host}:{port}")

            new_window = QueryInputForm(database, user, password, host, port)
            new_window.show()
            self.close()
        except Exception as e:
            self.lbl_result.setText(f"Failed to connect to database: {database}. Error: {e}")

class QueryInputForm(QWidget):
    def __init__(self, database, user, password, host, port):
        super().__init__()

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.init_ui()

    def init_ui(self):
        self.lbl_heading = QLabel("Query the Database")
        self.lbl_heading.setStyleSheet("font-size: 20pt; font-weight: bold;")

        self.details_label = QLabel(f"Querying database: : {self.database}@{self.host}:{self.port}")

        self.query_input = QTextEdit()
        self.lbl_result = QLabel('Result details will be displayed here after querying.')

        self.execute_button = QPushButton('Execute Query', self)
        self.execute_button.clicked.connect(lambda: self.execute_query(self.query_input.toPlainText()))

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.lbl_result)
        
        layout = QVBoxLayout()
        layout.addWidget(self.lbl_heading)
        layout.addWidget(self.details_label)
        layout.addWidget(self.query_input)
        layout.addWidget(self.execute_button)
        # layout.addWidget(self.lbl_result)
        layout.addWidget(self.scroll_area)

        self.setLayout(layout)
        self.setGeometry(400, 400, 500, 300)
        self.setFixedWidth(400)
        self.setWindowTitle('Query Input')
        self.show()


    def execute_query(self, query):
        try:
            con = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            cursor_obj = con.cursor()
            self.lbl_result.setText(f"Getting query plan...")
            query = "EXPLAIN (BUFFERS TRUE, COSTS TRUE, SETTINGS TRUE, WAL TRUE, TIMING TRUE, SUMMARY TRUE, ANALYZE TRUE, FORMAT JSON) " + query
            print("Executing query: " + query)
            cursor_obj.execute(query)
            result = cursor_obj.fetchall()
            self.lbl_result.setText(f"Results: {result}")
        except Exception as e:
            self.lbl_result.setText(f"Failed to execute the query. Error: {e}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = DatabaseInputForm()
    sys.exit(app.exec())