import azure.functions as func
from azure.data.tables import TableClient
import logging
import json
from datetime import datetime
from dotenv import load_dotenv

import os
from flask import Flask, Response,  request , jsonify
load_dotenv()

flask_app = Flask(__name__) 

conn_string = os.environ["AzureWebJobsStorage"]



app = func.WsgiFunctionApp(app=flask_app.wsgi_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS) 


@flask_app.route('/todos', methods=['GET'])
def get_todos():
    logging.info('Python HTTP trigger function processed a request. GET ALL')
    table_client = TableClient.from_connection_string(conn_str=conn_string, table_name="todos")
    my_filter = "PartitionKey eq 'todo'"
    all_todos = table_client.query_entities(my_filter)
    logging.info('Query read')

    records = []

    for entity in all_todos:
        records.append(entity)
    logging.info('records appended')
    return jsonify(records)

@flask_app.route('/todos/<int:row_id>', methods=['GET'])
def get_todo(row_id):
    logging.info(f'Python HTTP trigger function processed a request. GET One with id {row_id}')
    table_client = TableClient.from_connection_string(conn_str=conn_string, table_name="todos")
    my_filter = "PartitionKey eq 'todo' and RowKey eq '" + str(row_id) + "'"
    db_result = table_client.query_entities(my_filter)

    for item in db_result:
        logging.info(item)
        return jsonify(item)

    return {"error": "todo not found"}, 404



@flask_app.route('/todos', methods=['POST'])
def create_todo():
    logging.info('Python HTTP trigger function processed a request. POST new todo')
    new_todo = request.get_json()
    new_todo["PartitionKey"] = "todo"
    new_todo["RowKey"] = str(new_todo["id"])
    new_todo["done"] = False
    new_todo["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    table_client = TableClient.from_connection_string(conn_str=conn_string, table_name="todos")
    table_client.create_entity(new_todo)
    logging.info("record added")

    return jsonify(new_todo), 201


@flask_app.route('/todos/<int:todo_id>', methods=['PUT'])
def update_todo_by_id(todo_id):
    
    logging.info(f'Update todo with id {todo_id}')
    table_client = TableClient.from_connection_string(conn_str=conn_string, table_name="todos")

    try:
        todo = table_client.get_entity(partition_key='todo', row_key=str(todo_id))
    except:
        return {"error": "todo not found"}, 404

    body = request.get_json()
    if 'title' in body:
        todo['title'] = body['title']
    if 'done' in body:
        todo['done'] = body['done']
    if todo['done'] and ('finished_on' not in todo or todo['finished_on'] == None):
        todo['finished_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        todo['finished_on'] = None
    
    table_client.update_entity(mode='merge', entity=todo)
    return jsonify(todo)



@flask_app.route('/todos/<int:todo_id>', methods=['DELETE'])
def delete_todo_by_id(todo_id):
    logging.info(f'Delete todo with id {todo_id}')
    table_client = TableClient.from_connection_string(conn_str=conn_string, table_name="todos")
    filter = f"PartitionKey eq 'todo' and RowKey eq '{str(todo_id)}'"

    db_result = table_client.query_entities(filter)

    for item in db_result:
        table_client.delete_entity(row_key=str(todo_id), partition_key="todo")
        return jsonify({'result': True})

    return {"error": "todo not found"}, 404
