from __future__ import annotations

import dataclasses
import json
from typing import Any, Dict, List, Optional

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

    def get_qep(self, query: str) -> QueryExecutionPlan:
        output = self._query_qep(query)
        qep = QueryExecutionPlan(output)
        return [output, qep]
    
    def get_block_contents(self, block_id: int, relation: str) -> List:
        """get contents of block with block_id of relation
        
        Keyword arguments:
        block_id -- block_id of block
        relation -- relation that contains block
        Return: list of tuples with block_id of relation
        """
        
        with self._con.cursor() as cursor:
            print(f"SELECT ctid, * FROM {relation} WHERE (ctid::text::point)[0]::bigint IN (SELECT (ctid::text::point)[0]::bigint FROM {relation} WHERE ctid = '({block_id}, 1)');")
            cursor.execute(
                f"SELECT ctid, * FROM {relation} WHERE (ctid::text::point)[0]::bigint IN (SELECT (ctid::text::point)[0]::bigint FROM {relation} WHERE ctid = '({block_id}, 1)');"
            )
            out = cursor.fetchall()
            if out:
                return out
            else:
                raise ValueError



class QueryExecutionPlan:
    """
    Defines the execution plan of a query
    """

    def __init__(self, plan: Dict[str, Any]) -> None:
        """
        Constructs the query execution plan graph

        Args:
            plan: query execution plan an a dictionary
        """
        self.planning_time = plan[PLANNING_TIME]
        self.execution_time = plan[EXECUTION_TIME]
        self.root = Node(
            plan["Plan"]["Node Type"], plan["Plan"]["Plans"], **plan["Plan"]
        )
        self.blocks_accessed = []


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
                    child["Node Type"],
                    child["Plans"] if "Plans" in child.keys() else None,
                    **child,
                )
                for child in children
            ]
        else:
            self.children = []


if __name__ == "__main__":
    db_con = DatabaseConnection(
        "localhost", "", "postgres", "postgres", 5432
    )
    print(
        db_con.get_qep(
            "SELECT * FROM customer join nation on customer.c_nationkey = nation.n_nationkey;"
        )[1]
    )

    root = db_con.get_qep(
            "SELECT * FROM customer join nation on customer.c_nationkey = nation.n_nationkey;"
        )[1].root
    print(root.children[0].attributes)
