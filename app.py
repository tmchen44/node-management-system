from chalice import Chalice, BadRequestError, ConflictError
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
# from uuid import uuid4

app = Chalice(app_name='clarity-challenge')
app.debug = True

# Global variables
# dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
node_table = dynamodb.Table('nodes')
project_table = dynamodb.Table('projects')

# Attribute list for projects and nodes
PROJECT_INFO = {"project_name",
                "customer_name",
                "start_date",
                "end_date"}

NODE_INFO = {"latitude",
             "longitude",
             "shipping_status",
             "config_status"}

attr_dict = {"project_id": PROJECT_INFO, "node_id": NODE_INFO}

########################
# Node API
########################

@app.route('/', api_key_required=True)
@app.route('/nodes', api_key_required=True)
def list_nodes():
    results = node_table.scan(
        FilterExpression='node_id <> :zero',
        ExpressionAttributeValues={
            ':zero': '0'
        }
    )
    return results['Items']

@app.route('/nodes', methods=['POST'], api_key_required=True)
def create_node():
    node_id = node_table.update_item(
        Key={'node_id': '0'},
        UpdateExpression='ADD current_id :c',
        ExpressionAttributeValues={
            ':c': 1
        },
        ReturnValues='UPDATED_NEW'
    )['Attributes']['current_id']
    node_id = str(int(node_id))
    try:
        new_node = node_table.update_item(
            Item={
                'node_id': node_id,
                'shipping_status': 'pending',
                'config_status': 'unconfigured',
                'assigned_to': 'None'
            },
            ConditionExpression='attribute_not_exists(node_id)',
            ReturnValues='ALL_NEW'
        )['Attributes']
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "An error occured while creating the node. Please try again."
            raise ConflictError(message)
    response = {
        'Item': new_node,
        'Message': 'Node created.'
    }
    return response

@app.route('/nodes/{node_id}', api_key_required=True)
def get_node(node_id):
    result = check_id(node_table, node_id)
    return result['Item']

@app.route('/nodes/{node_id}', methods=['PATCH'], api_key_required=True)
def modify_node(node_id):
    data = app.current_request.json_body
    updated_node = modify(data, node_table, node_id)
    response = {
        'Item': updated_node,
        'Message': 'Node info updated.'
    }
    return response

########################
# Project API
########################

@app.route('/projects', api_key_required=True)
def list_projects():
    results = project_table.scan(
        FilterExpression='project_id <> :zero',
        ExpressionAttributeValues={
            ':zero': '0'
        }
    )
    return results['Items']

@app.route('/projects', methods=['POST'], api_key_required=True)
def create_project():
    project_id = project_table.update_item(
        Key={'project_id': '0'},
        UpdateExpression='ADD current_id :c',
        ExpressionAttributeValues={
            ':c': 1
        },
        ReturnValues='UPDATED_NEW'
    )['Attributes']['current_id']
    project_id = str(int(project_id))
    try:
        new_project = project_table.update_item(
            Item={
                'project_id': project_id
            },
            ConditionExpression='attribute_not_exists(project_id)',
            ReturnValues='ALL_NEW'
        )['Attributes']
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "An error occured while creating the project. Please try again."
            raise ConflictError(message)
    response = {
        'Item': new_project,
        'Message': 'Project created.'
    }
    return response

@app.route('/projects/{project_id}', api_key_required=True)
def get_project(project_id):
    result = check_id(project_table, project_id)
    return result['Item']

@app.route('/projects/{project_id}/nodes', api_key_required=True)
def get_project_nodes(project_id):
    check_id(project_table, project_id)
    results = node_table.scan(
        FilterExpression="assigned_to = :proj_id",
        ExpressionAttributeValues={
            ':proj_id': project_id
        }
    )
    return results['Items']

@app.route('/projects/{project_id}', methods=['PATCH'], api_key_required=True)
def modify_project(project_id):
    data = app.current_request.json_body
    updated_project = modify(data, project_table, project_id)
    response = {
        'Item': updated_project,
        'Message': 'Project info updated.'
    }
    return response

########################
# Node Assignment API
########################

@app.route('/nodes/{node_id}/assign/{project_id}', methods=['PATCH'], api_key_required=True)
def assign_node(node_id, project_id):
    check_id(project_table, project_id)
    condition = 'attribute_exists(node_id) AND node_id <> :zero'
    try:
        node_table.update_item(
            Key={
                'node_id': node_id
            },
            UpdateExpression='SET assigned_to = :proj',
            ConditionExpression=condition,
            ExpressionAttributeValues={
                ':proj': project_id,
                ':zero': '0'
            }
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "The given node_id does not exist in the database."
            raise BadRequestError(message)
    return {'Message': 'Node {} assigned to project {}.'.format(node_id, project_id)}

@app.route('/nodes/{node_id}/unassign', methods=['PATCH'], api_key_required=True)
def detach_node(node_id):
    condition = 'attribute_exists(node_id) AND node_id <> :zero'
    try:
        updated_node = node_table.update_item(
            Key={
                'node_id': node_id
            },
            UpdateExpression='SET assigned_to = :n',
            ConditionExpression=condition,
            ExpressionAttributeValues={
                ':zero': '0',
                ':n': 'None'
            },
            ReturnValues='UPDATED_OLD'
        )['Attributes']
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "The given node_id does not exist in the database."
            raise BadRequestError(message)
    old_project_id = updated_node['assigned_to']
    if old_project_id == 'None':
        return {'Message': 'Node was already unassigned. No changes were made.'}
    else:
        return {'Message': 'Unassigned node {} from project {}'.format(node_id, old_project_id)}


########################
# Helper methods
########################

def check_id(table, id_value):
    id_string = table.key_schema[0]['AttributeName']
    result = table.get_item(Key={id_string: id_value})
    if 'Item' not in result or id_value == '0':
        message = "The given {} does not exist in the database.".format(id_string)
        raise BadRequestError(message)
    return result

def modify_generate_expr_and_values(data, id_string):
    if not data or len(data.keys()) <= 0:
        message = "You must provide at least one modifiable attribute in the request json body."
        raise BadRequestError(message)
    expression = "SET "
    values = {}
    values[':zero'] = '0'
    for key, value in data.items():
        if key not in attr_dict[id_string]:
            raise BadRequestError("Invalid attribute detected. Please check that json keys match those defined in the documentation.")
        value_exp = ':' + key
        expression += key + " = " + value_exp + ", "
        values[value_exp] = value
    expression = expression.rstrip(', ')
    return expression, values

def modify(data, table, id_value):
    id_string = table.key_schema[0]['AttributeName']
    expression, values = modify_generate_expr_and_values(data, id_string)
    condition = 'attribute_exists({0}) AND {0} <> :zero'.format(id_string)
    try:
        updated = table.update_item(
            Key={
                id_string: id_value
            },
            UpdateExpression=expression,
            ConditionExpression=condition,
            ExpressionAttributeValues=values,
            ReturnValues='ALL_NEW'
        )['Attributes']
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "The given {} does not exist in the database.".format(id_string)
            raise BadRequestError(message)
    return updated
