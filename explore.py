from __future__ import annotations

import dataclasses
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2

PLANNING_TIME = "Planning Time"
EXECUTION_TIME = "Execution Time"


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
        self._con = psycopg2.connect(
            database=self._database,
            user=self._user,
            password=password,
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
        qep = QueryExecutionPlan(output, self)
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

    def get_relation_block_ids(self, relation_name: str):
        with self._con.cursor() as cursor:
            cursor.execute(
                f"SELECT DISTINCT (ctid::text::point)[0]::bigint as block_id \
                FROM {relation_name};"
            )
            out = cursor.fetchall()
            if out is not None:
                return out
            else:
                raise ValueError


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
            node_type,
            children,
            **plan,
        )
        self.blocks_accessed = self._get_blocks_accessed(self.root, con)

    def _get_blocks_accessed(
        self, root: Node, con: DatabaseConnection
    ) -> Dict[str, Set[int]]:
        blocks_accessed = dict()
        match root.node_type:
            case "Seq Scan" | "Parallel Seq Scan":
                blocks_accessed[root["Relation Name"]] = {
                    block_id[0]
                    for block_id in con.get_relation_block_ids(
                        root["Relation Name"]
                    )
                }

        for child in root.children:
            child_blocks_accessed = self._get_blocks_accessed(child, con)
            for relation, block_ids in child_blocks_accessed.items():
                if relation in blocks_accessed.keys():
                    blocks_accessed[relation].update(block_ids)
                else:
                    blocks_accessed[relation] = block_ids

        return blocks_accessed


@dataclasses.dataclass
class Node:
    """
    Defines a node in a query execution plan
    """

    def __init__(
        self,
        node_type: str,
        children: Optional[List[Dict[str, Any]]],
        **kwargs,
    ):
        self.node_type = node_type
        self.attributes = kwargs
        if children is not None:
            self.children = [
                Node(
                    child.pop("Node Type"),
                    child.pop("Plans", None),
                    **child,
                )
                for child in children
            ]
        else:
            self.children = []

    def __getitem__(self, attr: str):
        return self.attributes[attr]


if __name__ == "__main__":
    db_con = DatabaseConnection(
        "localhost", "postgres", "postgres", "postgres", 5432
    )
    _, qep = db_con.get_qep(
        "SELECT * FROM customer join nation on customer.c_nationkey = nation.n_nationkey;"
    )
    # print(qep.blocks_accessed)
