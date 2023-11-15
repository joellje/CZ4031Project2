from __future__ import annotations

import ast
import copy
import dataclasses
from typing import Any, Dict, List, Optional, Set, Tuple
import re

import psycopg2
from igraph.configuration import init

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
        self._password = password
        self._con = psycopg2.connect(
            database=self._database,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )

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
        self.table_map = {
            'ps' : 'partsupp',
            'l':'lineitem',
            'n' : 'nation',
            'o': 'orders',
            'p': 'part',
            'r': 'region',
            's': 'supplier'
        }
        self.table_map_reverse = {self.table_map[i]: i for i in self.table_map}
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
        self.blocks_accessed = self._get_blocks_accessed(self.root, con)

    def _get_blocks_accessed(
        self, root: Node, con: DatabaseConnection
    ) -> Dict[str, Set[int]]:
        blocks_accessed = dict()
        print(root.node_type)
        match root.node_type:
            case "Seq Scan" | "Parallel Seq Scan":
                blocks_accessed[root["Relation Name"]] = {
                    block_id[0]
                    for block_id in con.get_relation_block_ids(
                        root["Relation Name"]
                    )
                }
            case "Index Scan":
                print(root.attributes)
                relation_name = root["Relation Name"]
                alias = root["Alias"]
                if "Index Cond" in root.attributes:
                    index_cond = root["Index Cond"]
                if "Filter" in root.attributes:
                    index_cond = root["Filter"]
                print(index_cond)
                new_query = f"SELECT ctid, * FROM {relation_name} WHERE {index_cond};"

                if relation_name == alias:
                    with con._con.cursor() as cursor:
                        cursor.execute(new_query)
                        records = cursor.fetchall()
                        block_ids = set()
                        for record in records:
                            block_id, _ = ast.literal_eval(record[0])
                            block_ids.add(block_id)
                        blocks_accessed[relation_name] = block_ids
                else:
                    for i in self.table_map:
                        new_query = f"SELECT  {self.table_map_reverse[relation_name]}.ctid, {self.table_map_reverse[self.table_map[i]]}.ctid, * FROM {relation_name} {self.table_map_reverse[relation_name]}, {self.table_map[i]} {self.table_map_reverse[self.table_map[i]]} WHERE {index_cond};"
                        print(new_query)
                        try:
                            with con._con.cursor() as cursor:
                                cursor.execute(new_query)
                                records = cursor.fetchall()
                                block_ids = set()
                                for record in records:
                                    block_id, _ = ast.literal_eval(record[0])
                                    block_ids.add(block_id)
                                    block_id, _ = ast.literal_eval(record[1])
                                    block_ids.add(block_id)
                                blocks_accessed[relation_name] = block_ids
                            break
                        except:
                            print("nah boy")
                            con.reconnect()
                            continue
            case "Index Only Scan":
                print(root.attributes)
                relation_name = root["Relation Name"]
                alias = root["Alias"]
                if "Index Cond" in root.attributes:
                    index_cond = root["Index Cond"]
                if "Filter" in root.attributes:
                    index_cond = root["Filter"]
                print(index_cond)
                new_query = f"SELECT ctid, * FROM {relation_name} WHERE {index_cond};"

                if relation_name == alias:
                    with con._con.cursor() as cursor:
                        cursor.execute(new_query)
                        records = cursor.fetchall()
                        block_ids = set()
                        for record in records:
                            block_id, _ = ast.literal_eval(record[0])
                            block_ids.add(block_id)
                        blocks_accessed[relation_name] = block_ids
                else:
                    for i in self.table_map:
                        new_query = f"SELECT  {self.table_map_reverse[relation_name]}.ctid, {self.table_map_reverse[self.table_map[i]]}.ctid, * FROM {relation_name} {self.table_map_reverse[relation_name]}, {self.table_map[i]} {self.table_map_reverse[self.table_map[i]]} WHERE {index_cond};"
                        print(new_query)
                        try:
                            with con._con.cursor() as cursor:
                                cursor.execute(new_query)
                                records = cursor.fetchall()
                                block_ids = set()
                                for record in records:
                                    block_id, _ = ast.literal_eval(record[0])
                                    block_ids.add(block_id)
                                blocks_accessed[relation_name] = block_ids
                            break
                        except:
                            con.reconnect()
                            continue
            

                # print(index_cond)
                # where_matches = re.findall(r'\b(\w+)\.\w+\b', index_cond, re.IGNORECASE)
                # print(where_matches)

                # if len(where_matches) > 1:
                #     new_query = f"SELECT ctid, * FROM {where_matches[0]} CROSS JOIN {where_matches[1]} WHERE{index_cond}"
                # else:
                #     new_query = f"SELECT ctid, * FROM {relation_name} WHERE{index_cond}"
                # with con._con.cursor() as cursor:
                #     cursor.execute(new_query)
                #     records = cursor.fetchall()
                #     block_ids = set()
                #     for record in records:
                #         block_id, _ = ast.literal_eval(record[0])
                #         block_ids.add(block_id)
                #     blocks_accessed[relation_name] = block_ids

        for child in root.children:
            child_blocks_accessed = self._get_blocks_accessed(child, con)
            for relation, block_ids in child_blocks_accessed.items():
                if relation in blocks_accessed.keys():
                    blocks_accessed[relation].update(block_ids)
                else:
                    blocks_accessed[relation] = block_ids

        return blocks_accessed


@dataclasses.dataclass(kw_only=True)
class Node:
    """
    Defines a node in a query execution plan
    """

    node_type: str
    attributes: Dict[str, Any]
    children: List[Node] = dataclasses.field(init=False, repr=False)
    children_plans: dataclasses.InitVar[List[Any] | None]

    def __post_init__(
        self,
        children_plans: Optional[List[Dict[str, Any]]],
        **kwargs,
    ):
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
