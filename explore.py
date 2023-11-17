from __future__ import annotations

import copy
import dataclasses
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2

from utils import (alter_join_condition, build_join, build_select,
                   condition_is_join, get_aliases_in_condition,
                   get_child_with_attribute, random_string,
                   replace_aliases_with_views)

# Attribute Keys
PLANNING_TIME = "Planning Time"
EXECUTION_TIME = "Execution Time"
ALIAS = "Alias"
PARENT_RELATIONSHIP = "Parent Relationship"
INDEX_COND = "Index Cond"
HASH_COND = "Hash Cond"
MERGE_COND = "Merge Cond"
JOIN_TYPE = "Join Type"
SORT_KEY = "Sort Key"

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

        Returns:
        str: the url of this connection
        """
        return "postgres://{}:<PASSWORD>@{}:{}/{}".format(
            self._user, self._host, self._port, self._database
        )

    def _query_qep(self, query: str) -> Dict[str, Any]:
        """
        Query the database and return the query execution plan

        Args:
        query (str): the SQL query to be executed

        Returns:
        Dict[str, Any]: the query execution plan
        """
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
        """
        Get the query execution plan for the given SQL query

        Args:
        query (str): the SQL query to be executed

        Returns:
        Tuple[Any, QueryExecutionPlan]: a tuple containing the query execution plan and the QueryExecutionPlan object
        """
        output = self._query_qep(query)
        qep = QueryExecutionPlan(copy.deepcopy(output), self)
        return (output, qep)

    def get_relation_headers(self, relation: str) -> List:
        """
        Get the headers for a particular relation

        Args:
        relation (str): the name of the relation

        Returns:
        List: a list of strings with the headers of the relation
        """

        with self._con.cursor() as cursor:
            cursor.execute(
                f"SELECT column_name \
                FROM information_schema.columns \
                WHERE table_name = '{relation}';"
            )
            out = cursor.fetchall()

            if out:
                return [i[0] for i in out]
            else:
                raise ValueError

    def get_block_contents(self, block_id: int, relation: str) -> List:
        """
        Get the contents of the block with the given block_id in the given relation
        
        Args:
        block_id (int): the block_id of the block
        relation (str): the relation that contains the block
        
        Returns:
        List: a list of tuples with the block_id of the relation
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

    def get_relation_block_ids(self, relation_name: str) -> List:
        """
        Get the block_ids of the given relation

        Args:
        relation_name (str): the name of the relation

        Returns:
        List: a list of block_ids of the relation
		"""
        with self._con.cursor() as cursor:
            query = f"SELECT \
                    DISTINCT (ctid::text::point)[0]::bigint as block_id \
                    FROM {relation_name} "
            if condition is not None:
                query += f"WHERE {condition}"
            cursor.execute(query)
            out = cursor.fetchall()
            if out is not None:
                return out
            else:
                raise ValueError

    def create_view(self, view_name: str, statement: str):
        view_statement = f"CREATE VIEW {view_name} AS {statement};"
        print(f"Creating view: {view_statement}")
        try:
            with self._con.cursor() as cursor:
                cursor.execute(view_statement)
        except Exception as e:
            self._con.rollback()
            raise e

        self.views.append(view_name)

    def get_table_col_names(self, table_name: str):
        with self._con.cursor() as cursor:
            cursor.execute(
                "SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = 'public' "
                f"AND table_name = '{table_name.lower()}';"
            )
            out = cursor.fetchall()
            out = [o[0] for o in out]
            if out is None:
                raise ValueError
            return out


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
            "ps": "partsupp",
            "l": "lineitem",
            "n": "nation",
            "o": "orders",
            "p": "part",
            "r": "region",
            "s": "supplier",
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
        # map node in qep to a view in db
        self.views: Dict[str, str] = dict()

    def _get_blocks_accessed(
        self, root: Node, con: DatabaseConnection
    ) -> Dict[str, Set[int]]:
        """
        Returns a dictionary of relation names and the set of block IDs accessed by the given query plan.
        
        Args:
            root (Node): The root node of the query plan.
            con (DatabaseConnection): The database connection object.
        
        Returns:
            Dict[str, Set[int]]: A dictionary of relation names and the set of block IDs accessed by the given query plan.
        """
        self.blocks_accessed = dict()
        self._get_blocks_accessed(self.root, con)

    def _merge_blocks_accessed(self, blocks_accessed: Dict[str, Set[int]]):
        for relation, block_ids in blocks_accessed.items():
            if relation in self.blocks_accessed.keys():
                self.blocks_accessed[relation].update(block_ids)
            else:
                self.blocks_accessed[relation] = block_ids

    def _get_blocks_accessed(self, root: Node, con: DatabaseConnection):
        for child in root.children:
            self._get_blocks_accessed(child, con)
        blocks_accessed = dict()

        match root.node_type:
            # Scans
            case "Seq Scan" | "Parallel Seq Scan":
                # Sequential scan will access all blocks of relation
                blocks_accessed[root["Relation Name"]] = {
                    block_id[0]
                    for block_id in con.get_relation_block_ids(
                        root["Relation Name"], None
                    )
                }
                con.create_view(
                    root.node_id,
                    build_select(root["Relation Name"], [root["Filter"]]),
                )
                if root[ALIAS] is not None:
                    self.views[root[ALIAS]] = root.node_id

            case "Index Scan" | "Index Only Scan":
                index_cond = root["Index Cond"]
                filter = root["Filter"]

                # Replace table names in condition with view names
                if index_cond is not None:
                    index_cond = replace_aliases_with_views(
                        index_cond, self.views
                    )

                    if condition_is_join(index_cond):
                        # If index condition is a join,
                        # alter the condition for SELECT query to work
                        # TODO: error handling
                        index_cond = alter_join_condition(index_cond)

                # Replace table names in condition with filter names
                if filter is not None:
                    aliases = set(
                        re.findall(r"([a-zA-Z_]+\.)[a-zA-Z_]+", filter)
                    )
                    for table in aliases:
                        filter.replace(table, self.views[table[:-1]])

                # Index only scan do not access blocks
                if root.node_type == "Index Scan":
                    blocks_accessed[root["Relation Name"]] = {
                        block_id[0]
                        for block_id in con.get_relation_block_ids(
                            root["Relation Name"], index_cond
                        )
                    }
                con.create_view(
                    root.node_id,
                    build_select(
                        root["Relation Name"],
                        [index_cond, filter],
                    ),
                )
                if root[ALIAS] is not None:
                    self.views[root[ALIAS]] = root.node_id
            # Joins
            case "Nested Loop" | "Hash Join" | "Merge Join":
                inner = get_child_with_attribute(
                    root, PARENT_RELATIONSHIP, "Inner"
                )
                outer = get_child_with_attribute(
                    root, PARENT_RELATIONSHIP, "Outer"
                )
                # TODO: error handling
                assert inner is not None and outer is not None

                if root.node_type == "Nested Loop":
                    # join condition of Nested Loop is in the inner child
                    if inner["Index Cond"] is not None:
                        join_cond = inner["Index Cond"]
                    else:
                        join_cond = ""
                elif root.node_type == "Hash Join":
                    join_cond = root[HASH_COND]
                else:
                    join_cond = root[MERGE_COND]

                matches = re.findall(
                    r"(?P<outer>[a-zA-Z1-9_]+)\.?[a-zA-Z1-9_]+ = "
                    r"(?P<inner>[a-zA-Z1-9_]+)\.?[a-zA-Z1-9_]+",
                    join_cond,
                )
                if matches is None:
                    raise ValueError(
                        f"Unable to find table aliases in join condition: {join_cond}"
                    )
                for outer_alias, inner_alias in matches:
                    if root.node_type == "Nested Loop":
                        outer_alias, inner_alias = inner_alias, outer_alias
                    join_cond = re.sub(
                        rf"{inner_alias}\.", f"{inner.node_id}.", join_cond
                    )
                    join_cond = re.sub(
                        rf"{outer_alias}\.", f"{outer.node_id}.", join_cond
                    )

                # TODO: error handling
                inner_cols = con.get_table_col_names(inner.node_id)
                outer_cols = con.get_table_col_names(outer.node_id)
                select_cols = list(
                    set(inner_cols).symmetric_difference(outer_cols)
                )
                cols_intersect = list(
                    set().intersection(inner_cols, outer_cols)
                )
                select_cols.extend(
                    [f"{inner.node_id}.{col}" for col in cols_intersect]
                )

                # TODO: error handling
                join_statement = build_join(
                    inner.node_id,
                    outer.node_id,
                    join_cond,
                    root[JOIN_TYPE],
                    select_cols,
                )
                con.create_view(root.node_id, join_statement)
            case (
                "Gather Merge"
                | "Gather"
                | "Hash"
                | "Materialize"
                | "Sort"
                | "Memoize"
            ):
                # These nodes do not alter the tuples of the child view,
                # we can create a view that is the same as the child
                child = root.children[0]
                try:
                    con.create_view(root.node_id, build_select(child.node_id))
                    root.attributes[ALIAS] = child[ALIAS]
                    self.views[child[ALIAS]] = root.node_id
                except psycopg2.errors.UndefinedTable:
                    # If the child has no view, it means that
                    # the node's descendents has a node
                    # that cannot be parsed(eg. aggregate)
                    # we can skip it as there is no scans
                    # done, thus no blocks accessed
                    # raise e
                    pass
            case "Sort":
                sort_key = root[SORT_KEY]
                child = root.children[0]
                try:
                    con.create_view(
                        root.node_id,
                        build_select(child.node_id, order=sort_key),
                    )
                    root.attributes[ALIAS] = child[ALIAS]
                    self.views[child[ALIAS]] = root.node_id
                except psycopg2.errors.UndefinedTable:
                    # If the child has no view, it means that
                    # the node's descendents has a node
                    # that cannot be parsed(eg. aggregate)
                    # we can skip it as there is no scans
                    # done, thus no blocks accessed
                    pass
            case "Limit":
                limit = root["Plan Rows"]
                child = root.children[0]
                try:
                    con.create_view(
                        root.node_id, build_select(child.node_id, limit=limit)
                    )
                    root.attributes[ALIAS] = child[ALIAS]
                    self.views[child[ALIAS]] = root.node_id
                except psycopg2.errors.UndefinedTable:
                    # If the child has no view, it means that
                    # the node's descendents has a node
                    # that cannot be parsed(eg. aggregate)
                    # we can skip it as there is no scans
                    # done, thus no blocks accessed
                    pass
            case _:
                # for other node types
                pass

        self._merge_blocks_accessed(blocks_accessed)


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
        """
        Depth first search and set children
        """
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

    def __getitem__(self, attr: str) -> Any:
        """
        Get attribute of node

        Args:
            attr: attribute

        Returns:
            Attribute of node if exists else None
        """
        if attr in self.attributes.keys():
            return self.attributes[attr]
        return None
