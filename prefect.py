import requests
import sqlite3
from contextlib import closing
from collections import namedtuple

from prefect import task, Flow
from prefect.tasks.database.sqlite import SQLiteScript


# Create
create_table = SQLiteScript(
    db='todos.db',
    script='''
    CREATE TABLE IF NOT EXISTS todo (
        user_id INTEGER,
        id INTEGER,
        title TEXT,
        completed INTEGER
    )
    '''
)


# EXTRACT
@task
def get_todo_data():
    r = requests.get(
        "https://jsonplaceholder.cypress.io/todos",
        params={'_limit': 10}
    )
    return r.json()


# TRANSFORM
@task
def parse_todo_data(raw):
    Todo = namedtuple(
        'Todo',
        ['user_id', 'id', 'title', 'completed']
    )

    todos = []

    for row in raw:
        todo = Todo(
            user_id=row.get('userId'),
            id=row.get('id'),
            title=row.get('title'),
            completed=row.get('completed')
        )

        todos.append(todo)

    return todos


# LOAD
@task
def store_todos(parsed):
    insert_cmd = "INSERT INTO todo VALUES(?,?,?,?)"

    with closing(sqlite3.connect("todos.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed)
            conn.commit()


# Crete flow
with Flow('jsonplaceholder etl flow') as f:

    db_table = create_table()

    raw = get_todo_data()

    parsed = parse_todo_data(raw)

    populated_table = store_todos(parsed)

    populated_table.set_upstream(db_table)


f.visualize()