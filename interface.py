import json
import sys

import psycopg2
from PyQt6.QtWidgets import (QApplication, QLabel, QLineEdit, QPushButton,
                             QScrollArea, QTextBrowser, QTextEdit, QVBoxLayout,
                             QWidget, QSizePolicy)

from explore import DatabaseConnection, Node


class DatabaseInputForm(QWidget):
    def __init__(self):
        super().__init__()

        self.PLACEHOLDER_DB = self.PLACEHOLDER_USER = self.PLACEHOLDER_PASSWORD = "postgres"
        self.PLACEHOLDER_HOST = "0.0.0.0"
        self.PLACEHOLDER_PORT = "5432"

        self.init_ui()

    def init_ui(self):
        self.lbl_heading = QLabel("Connect to the Database")
        self.lbl_heading.setStyleSheet("font-size: 20pt; font-weight: bold;")

        self.lbl_instructions = QLabel(
            f"Input your database name, user, password, host and port.\nIf you want to use the default database, leave all fields empty."
        )
        self.lbl_instructions.setStyleSheet("font-size: 16pt;")

        self.lbl_db = QLabel("Database:")
        self.lbl_user = QLabel("User:")
        self.lbl_host = QLabel("Host:")
        self.lbl_password = QLabel("Password:")
        self.lbl_port = QLabel("Port:")
        self.lbl_result = QLabel(
            "Connection errors will be displayed here if necessary."
        )

        self.edit_db = QLineEdit()
        self.edit_db.setPlaceholderText(self.PLACEHOLDER_DB)
        self.edit_user = QLineEdit()
        self.edit_user.setPlaceholderText(self.PLACEHOLDER_USER)
        self.edit_password = QLineEdit()
        self.edit_password.setPlaceholderText(self.PLACEHOLDER_PASSWORD)
        self.edit_host = QLineEdit()
        self.edit_host.setPlaceholderText(self.PLACEHOLDER_HOST)
        self.edit_port = QLineEdit()
        self.edit_port.setPlaceholderText(self.PLACEHOLDER_PORT)

        self.btn_connect = QPushButton("Connect", self)
        self.btn_connect.clicked.connect(self.connect_to_database)
        self.quit_button = QPushButton("Quit", self)
        self.quit_button.clicked.connect(self.close_application)
        
        
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
        layout.addWidget(self.quit_button)

        self.setLayout(layout)

        # Set up the window
        self.setGeometry(400, 400, 300, 200)
        self.setWindowTitle("Database Connection Input")
        self.show()

    def connect_to_database(self):
        database = self.edit_db.text() if self.edit_db.text() != "" else self.PLACEHOLDER_DB
        user = self.edit_user.text() if self.edit_user.text() != "" else self.PLACEHOLDER_USER
        password = self.edit_password.text() if self.edit_password.text(
        ) != "" else self.PLACEHOLDER_PASSWORD
        host = self.edit_host.text() if self.edit_host.text() != "" else self.PLACEHOLDER_HOST
        port = self.edit_port.text() if self.edit_port.text() != "" else self.PLACEHOLDER_PORT

        result_text = f"Connecting to Database: {database}\nUser: {user}\nPassword: {password}\nHost: {host}\nPort: {port}"
        self.lbl_result.setText(result_text)

        try:
            database = 'postgres'
            user = 'postgres'
            host = '0.0.0.0'
            port = '5432'
            password = 'postgres'
            con = DatabaseConnection(host, user, password, database, port)
            self.lbl_result.setText(
                f"Connected to database: {database}@{host}:{port}"
            )

            new_window = QueryInputForm(con)
            new_window.show()
            self.close()
        except Exception as e:
            self.lbl_result.setText(
                f"Failed to connect to database: {database}. Error: {e}"
            )

    def close_application(self):
        QApplication.quit()


class QueryInputForm(QWidget):
    def __init__(self, db_con):
        super().__init__()

        self._con = db_con
        self.init_ui()

    def init_ui(self):
        self.lbl_heading = QLabel("Query the Database")
        self.lbl_heading.setStyleSheet("font-size: 20pt; font-weight: bold;")

        self.lbl_details = QLabel(
            f"Querying database: {self._con.connection_url()}"
        )
        self.x = "Blocks Explored"
        self.lbl_queryplantext = QLabel("Query Plan (Text):")
        self.lbl_queryplanvisual = QLabel("Query Plan (Visual):")
        self.lbl_block_explore = QLabel(self.x)

        self.query_input = QTextEdit()
        self.query_input.setAcceptRichText(False)
        self.query_input.setPlaceholderText(
            "e.g. SELECT * FROM orders AS o INNER JOIN customer AS c ON o.o_custkey = c.c_custkey;"
        )
        self.lbl_result = QTextBrowser()
        self.lbl_result.setPlainText(
            "Result details will be displayed here after querying."
        )

        self.execute_button = QPushButton("Execute Query", self)
        self.execute_button.clicked.connect(
            lambda: self.execute_query(self.query_input.toPlainText())
        )
        self.quit_button = QPushButton("Quit", self)
        self.quit_button.clicked.connect(self.close_application)

        #block button and browser initiator
        self.block_buttons_layout = QVBoxLayout()
        self.block_buttons_scroll_area = QScrollArea()
        self.block_buttons_scroll_area.setWidgetResizable(True)
        self.block_buttons_scroll_area.setWidget(QWidget())
        self.block_buttons_scroll_area.widget().setLayout(self.block_buttons_layout)
        self.block_content_view = QTextBrowser()

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.lbl_result)

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.lbl_heading)
        self.layout.addWidget(self.lbl_details)
        self.layout.addWidget(self.query_input)
        self.layout.addWidget(self.execute_button)
        self.layout.addWidget(self.lbl_queryplantext)
        self.layout.addWidget(self.scroll_area)
        self.layout.addWidget(self.lbl_queryplanvisual)
        self.layout.addWidget(self.lbl_block_explore)
        self.layout.addWidget(self.block_buttons_scroll_area)
        self.layout.addWidget(self.block_content_view)
        self.layout.addWidget(self.quit_button)

        self.setLayout(self.layout)
        self.setGeometry(400, 400, 600, 600)
        self.setFixedWidth(1000)
        self.setFixedHeight(1000)
        self.setWindowTitle("Query Input")
        self.show()

    def close_application(self):
        QApplication.quit()

    def execute_query(self, query):
        blocks_accessed = {}
        try:
            self.lbl_result.setPlainText(f"Getting query plan...")
            print("Getting QEP for: " + query)
            qepjson, qep = self._con.get_qep(query)
            self.lbl_result.setPlainText(
                json.dumps(qepjson, indent=4)
            )  # displays QEP as JSON
            planning_time = qep.planning_time
            execution_time = qep.execution_time
            root = qep.root
            blocks_accessed = qep.blocks_accessed
            x = 0
            for relation, block_ids in blocks_accessed.items():
                print(f"Relation: {relation}")
                for id in block_ids:
                    x += 1
                    block_contents = self._con.get_block_contents(
                        id, relation
                    )
                    for tuple in block_contents:
                        print(
                            f"block id: {id} - {tuple}"
                        )  # TODO: display tuple nicely
            # Update the UI to display block buttons
            self.lbl_block_explore.setText(f'Blocks Explored: {x}')
            self.display_block_buttons(qep.blocks_accessed)

        except Exception as e:
            self.lbl_result.setPlainText(
                f"Failed to execute the query. Error: {e}"
            )
    def display_block_buttons(self, blocks_accessed):
        # Remove all the buttons when you put in new ones
        for i in reversed(range(self.block_buttons_layout.count())):
            widgetToRemove = self.block_buttons_layout.itemAt(i).widget()
            if widgetToRemove:
                widgetToRemove.setParent(None)

        # Take in blocks accessed and creates new buttons
        for relation, block_ids in blocks_accessed.items():
            for block_id in block_ids:
                button = QPushButton(f"Block ID: {block_id} - Relation: {relation}")
                button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
                button.clicked.connect(lambda _, b=block_id, r=relation: self.show_block_contents(b, r))
                self.block_buttons_layout.addWidget(button)
    def show_block_contents(self, block_id, relation):
        # show the content
        block_contents = self._con.get_block_contents(block_id, relation)
        # Update
        res = ""
        for i in block_contents:
            res += str(i) + '\n'
        self.block_content_view.setPlainText(f"Block ID: {block_id} - Relation: {relation} \n {res}")
