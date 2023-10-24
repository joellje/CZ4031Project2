import psycopg2

try:
    con = psycopg2.connect(
        database="postgres",
        user="",
        host="localhost",
        port= '5432'
    )
    cursor_obj = con.cursor()
    cursor_obj.execute("EXPLAIN SELECT * FROM orders AS o INNER JOIN customer AS c ON o.o_custkey = c.c_custkey LIMIT 10;")
    result = cursor_obj.fetchall()
    print("Results: ", result)
except Exception as e:
    print(e)