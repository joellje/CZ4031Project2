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
        self._con = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
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
                            FORMAT JSON\
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
        return qep


class QueryExecutionPlan:
    """
    Defines the execution plan of a query
    """

    def __init__(self, plan: Dict[str, Any]) -> None:
        """
        Constructs the query execution plan graph

        Args:
            plan: query execution plan an a json string
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
        "localhost", "postgres", "postgres", "postgres", 5432
    )
    print(
        db_con.get_qep(
            "SELECT * FROM customer join nation on customer.c_nationkey = nation.n_nationkey ;"
        )
    )
