import json
from collections import deque

import plotly.graph_objects as go
import plotly.offline as plt
from igraph import EdgeSeq, Graph
from prettytable import PrettyTable
from PyQt6.QtGui import QFont
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWidgets import (QApplication, QComboBox, QLabel, QLineEdit,
                             QPushButton, QScrollArea, QTextBrowser, QTextEdit,
                             QVBoxLayout, QWidget)

from explore import DatabaseConnection, Node, UnsupportedQueryException


class DatabaseInputForm(QWidget):
    def __init__(self):
        super().__init__()

        self.PLACEHOLDER_DB = (
            self.PLACEHOLDER_USER
        ) = self.PLACEHOLDER_PASSWORD = "postgres"
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
        database = (
            self.edit_db.text()
            if self.edit_db.text() != ""
            else self.PLACEHOLDER_DB
        )
        user = (
            self.edit_user.text()
            if self.edit_user.text() != ""
            else self.PLACEHOLDER_USER
        )
        password = (
            self.edit_password.text()
            if self.edit_password.text() != ""
            else self.PLACEHOLDER_PASSWORD
        )
        host = (
            self.edit_host.text()
            if self.edit_host.text() != ""
            else self.PLACEHOLDER_HOST
        )
        port = (
            self.edit_port.text()
            if self.edit_port.text() != ""
            else self.PLACEHOLDER_PORT
        )

        result_text = f"Connecting to Database: {database}\nUser: {user}\nPassword: {password}\nHost: {host}\nPort: {port}"
        self.lbl_result.setText(result_text)

        try:
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
        self.sample_queries = {
            1: "SELECT c_name, c_address FROM customer;",
            2: """SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM part, supplier, partsupp, nation, region
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND p_size = 15
                AND p_type LIKE '%BRASS'
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
                AND ps_supplycost = (
                    SELECT
                        MIN(ps_supplycost)
                    FROM
                        partsupp,
                        supplier,
                        nation,
                        region
                    WHERE
                        p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
                )
            ORDER BY s_acctbal DESC, n_name, s_name, p_partkey;""",
            3: """SELECT p_type, AVG(l_quantity) AS avg_quantity
                FROM lineitem l
                JOIN part p ON l.l_partkey = p.p_partkey
                GROUP BY p_type;""",
            4: """SELECT n_name, COUNT(c_custkey) AS customer_count
                FROM nation n
                LEFT JOIN customer c ON n.n_nationkey = c.c_nationkey
                GROUP BY n_name
                ORDER BY customer_count DESC
                LIMIT 5;""",
            5: """SELECT supp_nation, cust_nation, SUM(volume) AS revenue FROM      (   SELECT    n1.n_name AS supp_nation,    n2.n_name AS cust_nation,    l_extendedprice * (1 - l_discount) AS volume   FROM    supplier,    lineitem,    orders,    customer,    nation n1,    nation n2   WHERE    s_suppkey = l_suppkey    AND o_orderkey = l_orderkey    AND c_custkey = o_custkey    AND s_nationkey = n1.n_nationkey    AND c_nationkey = n2.n_nationkey    AND (     (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')     OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')    )      ) AS shipping GROUP BY      supp_nation,      cust_nation ORDER BY      supp_nation,      cust_nation;""",
            6: "SELECT * FROM partsupp where ps_partkey < 30 or ps_suppkey < 20;",
        }
        self.lbl_heading = QLabel("Query the Database")
        self.lbl_heading.setStyleSheet("font-size: 20pt; font-weight: bold;")

        self.lbl_details = QLabel(
            f"Querying database: {self._con.connection_url()}"
        )
        self.lbl_queryplantext = QLabel("<b>Query Plan (Text):</b>")
        self.lbl_queryplantree = QLabel("<b>Query Plan (Tree):</b>")
        self.lbl_queryplanblocks = QLabel(
            "<b>Query Plan (Blocks Accessed):</b>"
        )
        self.lbl_block_explore = QLabel("<i>Blocks Explored:</i>")
        self.lbl_queryplanblocks_relation = QLabel("Choose Relation:")
        self.lbl_queryplanblocks_block_id = QLabel("Choose Block ID:")
        self.lbl_sample_queries = QLabel("Select a sample query: ")

        self.sample_input = QComboBox()
        self.sample_input.addItems(
            [""] + [f"Sample {i}" for i in self.sample_queries]
        )
        self.sample_input.currentIndexChanged.connect(self.update_query_field)

        self.query_input = QTextEdit()
        self.query_input.setAcceptRichText(False)
        self.query_input.setPlaceholderText(
            "e.g. SELECT * FROM orders AS o INNER JOIN customer AS c ON o.o_custkey = c.c_custkey;"
        )
        self.query_result = QTextBrowser()
        self.query_result.setPlainText(
            "Result details will be displayed here after querying."
        )

        self.execute_button = QPushButton("Execute Query", self)
        self.execute_button.clicked.connect(
            lambda: self.execute_query(self.query_input.toPlainText())
        )
        self.qeptree_button = QPushButton("View Query Plan Tree", self)
        self.qeptree_button.clicked.connect(self.display_qep_tree)
        self.qeptree_button.setEnabled(False)
        self.quit_button = QPushButton("Quit", self)
        self.quit_button.clicked.connect(self.close_application)

        # block browser initiator
        self.block_content_view = QTextBrowser()
        self.block_content_view.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        self.block_content_view.horizontalScrollBar().setValue(0)
        self.block_content_view.setFont(QFont("Courier New", 11))

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.query_result)

        # dropdowns for exploring blocks
        self.relation_dropdown = QComboBox()
        self.relation_dropdown.setEnabled(False)
        self.relation_dropdown.currentTextChanged.connect(
            lambda relation: self.update_block_id_dropdown(relation)
        )
        self.block_id_dropdown = QComboBox()
        self.block_id_dropdown.setEnabled(False)
        self.block_id_dropdown.currentTextChanged.connect(
            lambda block_id: self.show_block_contents(block_id)
        )

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.lbl_heading)
        self.layout.addWidget(self.lbl_details)
        self.layout.addWidget(self.lbl_sample_queries)
        self.layout.addWidget(self.sample_input)
        self.layout.addWidget(self.query_input)
        self.layout.addWidget(self.execute_button)
        self.layout.addWidget(self.lbl_queryplantext)
        self.layout.addWidget(self.scroll_area)
        self.layout.addWidget(self.lbl_queryplantree)
        self.layout.addWidget(self.qeptree_button)
        self.layout.addWidget(self.lbl_queryplanblocks)
        self.layout.addWidget(self.lbl_block_explore)
        self.layout.addWidget(self.lbl_queryplanblocks_relation)
        self.layout.addWidget(self.relation_dropdown)
        self.layout.addWidget(self.lbl_queryplanblocks_block_id)
        self.layout.addWidget(self.block_id_dropdown)
        self.layout.addWidget(self.block_content_view)
        self.layout.addWidget(self.quit_button)

        self.setLayout(self.layout)
        self.setGeometry(400, 400, 600, 600)
        self.setFixedWidth(1000)
        self.setFixedHeight(800)
        self.setWindowTitle("Query Input")
        self.show()

    def update_query_field(self):
        if self.sample_input.currentIndex() == 0:
            self.query_input.setPlainText("")
        else:
            self.query_input.setPlainText(
                self.sample_queries[self.sample_input.currentIndex()]
            )

    def close_application(self):
        QApplication.quit()

    def execute_query(self, query):
        self.query_result.clear()
        self._con.reconnect()
        self.lbl_queryplanblocks.setText(
            "<b>Query Plan (Blocks Accessed):</b>"
        )
        self.lbl_block_explore.setText("<i>Blocks Explored:</i>")
        self.relation_dropdown.clear()
        self.relation_dropdown.setEnabled(False)
        self.block_id_dropdown.clear()
        self.block_id_dropdown.setEnabled(False)
        self.block_content_view.clear()

        self.relation_dropdown.clear()
        self.block_id_dropdown.clear()
        self.relation_dropdown.setEnabled(False)
        self.block_id_dropdown.setEnabled(False)
        self.qeptree_button.setEnabled(False)
        self.qep = None

        try:
            self.query_result.setPlainText("Getting query plan...")
            print("Getting QEP for: " + query)
            qepjson, qep = self._con.get_qep(query)

            self.qep = qep
            self.qeptree_button.setEnabled(True)

            self.query_result.setPlainText(json.dumps(qepjson, indent=4))
        except Exception as e:
            self.query_result.setPlainText(
                f"Failed to execute the query. Error: {e}"
            )
            return

        self.blocks_accessed = dict()
        try:
            self.blocks_accessed = qep.get_blocks_accessed(self._con)

            self.lbl_block_explore.setText(
                f"<i>Blocks Explored: {sum(len(blocks) for blocks in self.blocks_accessed.values())}</i>"
            )
            self.update_relation_dropdown()
            self.relation_dropdown.setEnabled(True)
        except UnsupportedQueryException:
            self.lbl_queryplanblocks.setText(
                "<b>Query Plan (Blocks Accessed):</b> (Note: this query is unsupported, hence might not have accurate blocks accessed)"
            )
            self.lbl_block_explore.setText(
                f"<i>Blocks Explored: {sum(len(blocks) for blocks in self.blocks_accessed.values())}</i>"
            )
            self.update_relation_dropdown()
            self.relation_dropdown.setEnabled(True)

    def update_relation_dropdown(self):
        self.relation_dropdown.clear()
        self.relation_dropdown.setEnabled(True)
        self.relation_dropdown.addItems(
            [""] + list(self.blocks_accessed.keys())
        )

    def update_block_id_dropdown(self, relation):
        self.relation = relation

        if self.relation == "":
            self.block_id_dropdown.clear()
            self.block_id_dropdown.setEnabled(False)
        else:
            self.block_id_dropdown.clear()
            self.block_id_dropdown.setEnabled(True)
            self.block_id_dropdown.addItems(
                [""]
                + sorted(
                    [str(i) for i in self.blocks_accessed[self.relation]],
                    key=lambda x: int(x),
                )
            )

    def show_block_contents(self, block_id):
        self.block_id = block_id

        if self.block_id == "":
            self.block_content_view.setPlainText("")
        else:
            headers = ["ctid"] + self._con.get_relation_headers(self.relation)
            block_contents = self._con.get_block_contents(
                self.block_id, self.relation
            )

            table = PrettyTable()
            table.field_names = headers
            table.add_rows(block_contents)

            self.block_content_view.setPlainText(
                f"Block ID: {self.block_id} - Relation: {self.relation}\n{table.get_string()}"
            )

    def display_qep_tree(self):
        try:
            new_window = QEPTree(self.qep)
            new_window.show()
        except Exception as e:
            print(f"Error displaying QEP tree: {e}")


class QEPTree(QWidget):
    def __init__(self, qep):
        super().__init__()
        self.qep = qep
        self.init_ui()

    def init_ui(self):
        self.layout = QVBoxLayout()
        self.lbl_heading = QLabel("QEP Tree")
        self.lbl_heading.setStyleSheet("font-size: 20pt; font-weight: bold;")
        self.layout.addWidget(self.lbl_heading)

        fig = self.generate_fig()
        html = "<html><body>"
        html += plt.plot(fig, output_type="div", include_plotlyjs="cdn")
        html += "</body></html>"
        self.view = QWebEngineView()
        self.view.setHtml(html)
        self.layout.addWidget(self.view)

        self.quit_button = QPushButton("Close QEP Tree", self)
        self.quit_button.clicked.connect(lambda: self.close())
        self.layout.addWidget(self.quit_button)

        self.setLayout(self.layout)
        self.setWindowTitle("QEP Tree")
        self.setGeometry(500, 500, 600, 600)
        self.setFixedWidth(1000)
        # self.setFixedHeight(800)
        self.show()

    def generate_fig(self):
        # G = Graph.Tree(idCounter, 2)  # 2 stands for children number
        G = Graph()
        q = deque([(-1, 0, self.qep.root)])  # add root to graph
        idCounter = 1
        labels = []
        hovertexts = []

        hovertext_map = {
            "Join": ["Join Type"],
            "Scan": ["Relation Name"],
            "Hash Join": ["Hash Cond"],
            "Merge Join": ["Merge Cond"],
            "Aggregate": ["Strategy"],
            "Group": ["Filter"],
            "Sort": ["Sort Key", "Sort Method"],
            "Index": ["Index Cond", "Filter"],
            "": ["Startup Cost", "Total Cost", "Plan Rows", "Plan Width"],
        }
        label_map = {
            "Scan": "Relation Name",
            "Hash Join": "Hash Cond",
            "Merge Join": "Merge Cond",
            "Sort": "Sort Key",
        }

        while q:
            for _ in range(len(q)):  # level order traversal
                parent, i, cur = q.popleft()
                G.add_vertex(i)

                label = f"<b>{cur.node_type}</b>"
                label_parser = (
                    lambda x: f"<br>{cur.attributes[x]}"
                    if x in cur.attributes
                    else ""
                )
                for label_match, attr in label_map.items():
                    if label_match in cur.node_type:
                        label += label_parser(attr)
                labels.append(label)

                hovertext = f"<b><i>{cur.node_type}</i></b><br>"
                hovertext_parser = (
                    lambda x: f"{x}: {cur.attributes[x]}<br>"
                    if x in cur.attributes
                    else ""
                )
                for node_match, attributes in hovertext_map.items():
                    if node_match in cur.node_type:
                        for attr in attributes:
                            hovertext += hovertext_parser(attr)
                hovertexts.append(hovertext)

                if parent != -1:
                    G.add_edge(parent, i)

                for child in cur.children:
                    q.append((i, idCounter, child))
                    idCounter += 1

        # Note: code below referenced from https://plotly.com/python/tree-plots/
        lay = G.layout("rt", root=[0])

        position = {k: lay[k] for k in range(idCounter)}
        Y = [lay[k][1] for k in range(idCounter)]
        M = max(Y)

        es = EdgeSeq(G)  # sequence of edges
        E = [e.tuple for e in G.es]  # list of edges

        L = len(position)
        Xn = [position[k][0] for k in range(L)]
        Yn = [2 * M - position[k][1] for k in range(L)]
        Xe = []
        Ye = []
        for edge in E:
            Xe += [position[edge[0]][0], position[edge[1]][0], None]
            Ye += [
                2 * M - position[edge[0]][1],
                2 * M - position[edge[1]][1],
                None,
            ]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=Xe,
                y=Ye,
                mode="lines",
                line=dict(color="rgb(210,210,210)", width=1),
                hoverinfo="none",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=Xn,
                y=Yn,
                mode="text",
                text=labels,
                hovertext=hovertexts,
                hoverinfo="text",
            )
        )

        axis = dict(
            showline=False,  # hide axis line, grid, ticklabels and  title
            zeroline=False,
            showgrid=False,
            showticklabels=False,
        )

        fig.update_layout(
            font_size=12,
            showlegend=False,
            xaxis=axis,
            yaxis=axis,
            margin=dict(l=0, r=0, b=0, t=0),
            hovermode="closest",
            plot_bgcolor="rgb(248,248,248)",
            dragmode="pan",
            margin_pad=10,
        )

        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True)

        return fig
