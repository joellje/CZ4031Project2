from __future__ import annotations

import random
import re
import string
from typing import TYPE_CHECKING, Any, Dict, List, Set

if TYPE_CHECKING:
    from explore import Node


def build_select(relation: str, conditions: List[str]) -> str:
    """
    Build a SELECT query

    Args:
        relation: relation to select from
        conditions: conditions for the WHERE clause

    Returns:
        SELECT query on relation with conditions
    """
    query = f"SELECT * FROM {relation}"
    conditions = [c for c in conditions if c is not None]
    if len(conditions) > 0:
        query += f" WHERE {'AND'.join(conditions)}"

    return query


def build_join(
    relation_inner: str,
    relation_outer: str,
    join_cond: str,
    join_type: str,
) -> str:
    """
    Build a JOIN query

    Args:
        relation_inner: inner relation to join on
        relation_outer: outer relation to join on
        join_cond: join condition
        join_type: type of join, can be one of
            (Inner, Full, Left, Right)

    Returns:
        JOIN query
    """
    match join_type:
        case "Inner":
            join_type = "INNER"
        case "Full":
            join_type = "FULL OUTER"
        case "Left":
            join_type = "LEFT OUTER"
        case "Right":
            join_type = "RIGHT OUTER"
        case _:
            raise ValueError(f"{join_type} is not a valid join type")
    
    if join_cond == "":
        return f"(SELECT * \
                FROM \
                    {relation_inner} \
                CROSS JOIN {relation_outer}); \
                "
    else:
        return f"SELECT * \
                FROM \
                    {relation_inner} \
                {join_type} JOIN {relation_outer} on {join_cond} \
                "


def random_string(n: int = 5) -> str:
    """
    Generate a random string consisting of uppercase letters

    Args:
        n: number of characters of random string

    Returns:
        Random string of length n
    """
    return "".join(random.choices(string.ascii_uppercase, k=n))


def get_aliases_in_condition(cond: str) -> Set[str]:
    """
    Extract the aliases in a condition

    Args:
        cond: condition to extract

    Returns:
        set of aliases

    Examples:
        >>> get_aliases_in_condition("(table_a.id = table_b.id)")
        {"table_a", "table_b"}
    """
    return set(re.findall(r"([a-zA-Z_1-9?]+)\.[a-zA-Z_]+", cond))  # type: Set[str]


class ViewNotFoundException(Exception):
    pass


def replace_aliases_with_views(statement: str, views: Dict[str, str]) -> str:
    """
    Replace aliases in a statement with corresponding views

    Args:
        statement: statement to replace
        view: dictionary mapping aliases to view names

    Returns:
        altered statement with aliases replaced with view names

    Examples:
        >>> views = {"table_a": "view_a", "table_b": "view_b"}
        >>> replace_aliases_with_views("(table_a.id = table_b.b_id)")
        "(view_a.id = view_b.bid)"
    """
    print("statement: ", statement)
    print("views: ", views)
    aliases = get_aliases_in_condition(statement)
    print("aliases: ", aliases)
    for alias in aliases:
        statement = re.sub(rf"{alias}\.", f"{views[alias]}.", statement)
    print("statement: ", statement)
    return statement


def condition_is_join(cond: str) -> bool:
    """
    Checks if a condition is a JOIN condition

    Args:
        cond: condition to check

    Returns:
        True if condition is a join else False

    Examples:
        >>> condition_is_join("(table_a.id = table_b.id)")
        True

        >>> condition_is_join("(table_a.id > 50)")
        False
    """

    return (
        re.search(
            r"[a-zA-Z_]+\.?[a-zA-Z_]+ = ([a-zA-Z_]+)\.[a-zA-Z_]+",
            cond,
        )
        is not None
    )


def alter_join_condition(cond: str) -> str:
    """
    Alter JOIN condition in WHERE clause

    Args:
        cond: condition to alter

    Returns:
        Altered condition

    Examples:
        >>> alter_join_condition("(table_a.id = table_b.b_id)")
        "(table_a.id IN (SELECT b_id from table_b))"
    """
    cond_attributes = re.search(
        r"(?P<left>[a-zA-Z_]+\.?[a-zA-Z_]+) = "
        r"(?P<table_right>[a-zA-Z_]+)\.(?P<right_column>[a-zA-Z_]+)",
        cond,
    )
    if cond_attributes is None:
        raise ValueError(
            f"Unable to find join attributes in condition: {cond}"
        )
    left = cond_attributes.group("left")
    table_right = cond_attributes.group("table_right")
    right_column = cond_attributes.group("right_column")
    return "( {} IN (SELECT {} FROM {}) )".format(
        left, right_column, table_right
    )


def get_child_with_attribute(
    node: Node, attr_name: str, attr_val: Any
) -> Node | None:
    """
    Get the first child node with an attribute having a certain value

    Args:
        node: parent node
        attr_name: name of attribute
        attr_val: value of attribute to match

    Returns:
        first node found else None
    """
    return next(
        child for child in node.children if child[attr_name] == attr_val
    )
