import random
import string
from typing import List


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
