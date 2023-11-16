from __future__ import annotations

import copy
import dataclasses
import logging
import random
import string
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2

PLANNING_TIME = "Planning Time"
EXECUTION_TIME = "Execution Time"

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Manages the PSQL database connection
    """

    def __init__(
        self, host: str, user: str, password: str, database: str, port: int
    ) -> None:
        self._host = host
        self._user = user
        self._database = database
        self._port = port
        self._password = password
        self._con = psycopg2.connect(
            database=self._database,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )
        self.views: List[str] = []
        print(f"Established connection to db at {self.connection_url()}")

    def __del__(self):
        with self._con.cursor() as cursor:
            for view in self.views:
                cursor.execute(f"DROP VIEW {view}")

    def reconnect(self) -> None:
        """
        Reconnect to the database
        """
        self._con = psycopg2.connect(
            database=self._database,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )

    def connection_url(self) -> str:
        """
        Get the url of this connection
        """
        return "postgres://{}:<PASSWORD>@{}:{}/{}".format(
            self._user, self._host, self._port, self._database
        )

    def _query_qep(self, query: str) -> Dict[str, Any]:
        with self._con.cursor() as cursor:
            cursor.execute(
                f"EXPLAIN \
                        ( \
                            BUFFERS TRUE, \
                            COSTS TRUE, \
                            SETTINGS TRUE, \
                            WAL TRUE, \
                            TIMING TRUE, \
                            SUMMARY TRUE, \
                            ANALYZE TRUE, \
                            FORMAT JSON \
                        ) \
                        {query}"
            )
            out = cursor.fetchone()
            if out is not None:
                return out[0][0]
            else:
                raise ValueError

    def get_qep(self, query: str) -> Tuple[Any, QueryExecutionPlan]:
        output = self._query_qep(query)
        qep = QueryExecutionPlan(copy.deepcopy(output), self)
        return (output, qep)

    def get_block_contents(self, block_id: int, relation: str) -> List:
        """get contents of block with block_id of relation

        Keyword arguments:
        block_id -- block_id of block
        relation -- relation that contains block
        Return: list of tuples with block_id of relation
        """

        with self._con.cursor() as cursor:
            cursor.execute(
                f"SELECT ctid, * \
                  FROM {relation} \
                  WHERE (ctid::text::point)[0]::bigint = {block_id};"
            )
            out = cursor.fetchall()
            if out:
                return out
            else:
                raise ValueError

    def get_relation_block_ids(
        self, relation_name: str, condition: str | None
    ):
        with self._con.cursor() as cursor:
            query = f"SELECT \
                    DISTINCT (ctid::text::point)[0]::bigint as block_id \
                    FROM {relation_name};"
            if condition is not None:
                query += f"WHERE {condition}"
            cursor.execute(query)
            out = cursor.fetchall()
            if out is not None:
                return out
            else:
                raise ValueError

    def create_view(self, view_name: str, statement: str):
        view_statement = f"CREATE VIEW {view_name} AS {statement}"
        print(f"Creating view: {view_statement}")
        try:
            with self._con.cursor() as cursor:
                cursor.execute(view_statement)
        except Exception as e:
            print(e)
            raise e

        self.views.append(view_name)

    def build_select(self, relation: str, conditions: List[str]) -> str:
        query = f"SELECT * FROM {relation}"
        conditions = [c for c in conditions if c is not None]
        if len(conditions) > 0:
            query += f" WHERE {'AND'.join(conditions)}"

        return query

    def build_join(
        self,
        alias_inner: str,
        alias_outer: str,
        join_cond: str,
        join_type: str,
    ) -> str:
        match join_type:
            case "Inner":
                join_type = "INNER"
            case "Full":
                join_type = "FULL OUTER"
            case "Left":
                join_type = "LEFT OUTER"
            case "Right":
                join_type = "RIGHT OUTER"

        return f"SELECT * \
                FROM \
                    {alias_inner} \
                {join_type} JOIN {alias_outer} on {join_cond} \
                "


class QueryExecutionPlan:
    """
    Defines the execution plan of a query
    """

    def __init__(self, plan: Dict[str, Any], con: DatabaseConnection) -> None:
        """
        Constructs the query execution plan graph

        Args:
            plan: query execution plan an a dictionary
        """
        self.planning_time = plan[PLANNING_TIME]
        self.execution_time = plan[EXECUTION_TIME]
        plan = plan["Plan"]
        node_type = plan.pop("Node Type")
        children = plan.pop("Plans", None)
        self.root = Node(
            node_type=node_type,
            children_plans=children,
            attributes=plan,
        )
        # map node in qep to a view in db
        self.views: Dict[str, str] = dict()

        self.blocks_accessed = dict()
        self._get_blocks_accessed(self.root, con)

    def _merge_blocks_accessed(self, blocks_accessed: Dict[str, Set[int]]):
        for relation, block_ids in blocks_accessed.items():
            if relation in self.blocks_accessed.keys():
                self.blocks_accessed[relation].update(block_ids)
            else:
                self.blocks_accessed[relation] = block_ids

    def _get_blocks_accessed(self, root: Node, con: DatabaseConnection):
        return
        blocks_accessed = dict()
        match root.node_type:
            case "Hash" | "Sort" | "Gather Merge" | "Gather":
                for child in root.children:
                    self._get_blocks_accessed(child, con)
            case "Nested Loop":
                inner = next(
                    child
                    for child in root.children
                    if child["Parent Relationship"] == "Inner"
                )
                outer = next(
                    child
                    for child in root.children
                    if child["Parent Relationship"] == "Outer"
                )
                self._get_blocks_accessed(inner)
                
            case "Hash Join":
                for child in root.children:
                    self._get_blocks_accessed(child, con)
                alias_inner = next(
                    child["Alias"]
                    for child in root.children
                    if child["Parent Relationship"] == "Inner"
                )
                alias_outer = next(
                    child["Alias"]
                    for child in root.children
                    if child["Parent Relationship"] == "Outer"
                )
                join_statement = con.build_join(
                    self.views[alias_inner],
                    self.views[alias_outer],
                    root["Hash Cond"]
                    .replace(f"{alias_inner}.", f"{self.views[alias_inner]}.")
                    .replace(f"{alias_outer}.", f"{self.views[alias_outer]}."),
                    root["Join Type"],
                )
                con.create_view(root.node_id, join_statement)
            case "Merge Join":
                for child in root.children:
                    self._get_blocks_accessed(child, con)
                alias_inner = next(
                    child["Alias"]
                    for child in root.children
                    if child["Parent Relationship"] == "Inner"
                )
                alias_outer = next(
                    child["Alias"]
                    for child in root.children
                    if child["Parent Relationship"] == "Outer"
                )
                join_statement = con.build_join(
                    self.views[alias_inner],
                    self.views[alias_outer],
                    root["Merge Cond"]
                    .replace(f"{alias_inner}.", f"{self.views[alias_inner]}.")
                    .replace(f"{alias_outer}.", f"{self.views[alias_outer]}."),
                    root["Join Type"],
                )
                con.create_view(root.node_id, join_statement)
            case "Seq Scan" | "Parallel Seq Scan":
                blocks_accessed[root["Relation Name"]] = {
                    block_id[0]
                    for block_id in con.get_relation_block_ids(
                        root["Relation Name"], None
                    )
                }
                con.create_view(
                    root.node_id,
                    con.build_select(root["Relation Name"], [root["Filter"]]),
                )
                if root["Alias"] is not None:
                    self.views[root["Alias"]] = root.node_id
            case "Index Scan":
                blocks_accessed[root["Relation Name"]] = {
                    block_id[0]
                    for block_id in con.get_relation_block_ids(
                        root["Relation Name"], root["Index Cond"]
                    )
                }
                con.create_view(
                    root.node_id,
                    con.build_select(
                        root["Relation Name"],
                        [root["Index Cond"], root["Filter"]],
                    ),
                )
                if root["Alias"] is not None:
                    self.views[root["Alias"]] = root.node_id

        self._merge_blocks_accessed(blocks_accessed)


def random_string(N: int = 5) -> str:
    return "".join(random.choices(string.ascii_uppercase, k=N))


@dataclasses.dataclass(kw_only=True)
class Node:
    """
    Defines a node in a query execution plan
    """

    node_id: str = dataclasses.field(init=False, repr=False)
    node_type: str
    attributes: Dict[str, Any]
    children: List[Node] = dataclasses.field(init=False, repr=False)
    children_plans: dataclasses.InitVar[List[Any] | None]

    def __post_init__(
        self,
        children_plans: Optional[List[Dict[str, Any]]],
    ):
        self.node_id = f"{self.node_type.replace(' ', '_')}_{random_string()}"
        if children_plans is not None:
            self.children = [
                Node(
                    node_type=child.pop("Node Type"),
                    children_plans=child.pop("Plans", None),
                    attributes=child,
                )
                for child in children_plans
            ]
        else:
            self.children = []
        if self.node_type in ("Hash", "Sort", "Gather Merge", "Gather"):
            self.attributes["Alias"] = self.children[0]["Alias"]

    def __getitem__(self, attr: str) -> Any:
        if attr in self.attributes.keys():
            return self.attributes[attr]
        return None


if __name__ == "__main__":
    db_con = DatabaseConnection(
        "localhost", "postgres", "postgres", "postgres", 5432
    )
    _, qep = db_con.get_qep(
        "SELECT * FROM customer join nation on customer.c_nationkey = nation.n_nationkey;"
    )
    # print(qep.blocks_accessed)
