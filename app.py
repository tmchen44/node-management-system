from chalice import Chalice, BadRequestError, ConflictError
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
# from uuid import uuid4

app = Chalice(app_name='clarity-challenge')
app.debug = True

# global variables
# dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
node_table = dynamodb.Table('nodes')
project_table = dynamodb.Table('projects')

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
    response = node_table.scan(
        FilterExpression=Attr('node_id').ne('0')
    )
    return response['Items']

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
        node_table.put_item(
            Item={
                'node_id': node_id,
                'shipping_status': 'pending',
                'config_status': 'unconfigured'
            },
            ConditionExpression='attribute_not_exists(node_id)'
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "An error occured while creating the node. Please try again."
            raise ConflictError(message)
    response = {}
    response['Item'] = node_table.get_item(Key={'node_id': node_id})['Item']
    response['Message'] = 'You have created a node.'
    return response

@app.route('/nodes/{node_id}', api_key_required=True)
def get_node(node_id):
    node = check_id(node_table, node_id)
    return node['Item']

@app.route('/nodes/{node_id}', methods=['PATCH'], api_key_required=True)
def modify_node(node_id):
    check_id(node_table, node_id)
    data = app.current_request.json_body
    updated = modify(data, node_table, node_id)
    response = {}
    response['Item'] = updated
    response['Message'] = 'Node info updated.'
    return response

########################
# Project API
########################

@app.route('/projects', api_key_required=True)
def list_projects():
    response = project_table.scan(
        FilterExpression=Attr('project_id').ne('0')
    )
    return response['Items']

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
        project_table.put_item(
            Item={
                'project_id': project_id
            },
            ConditionExpression='attribute_not_exists(project_id)'
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "An error occured while creating the project. Please try again."
            raise ConflictError(message)
    response = {}
    response['Item'] = project_table.get_item(
                        Key={'project_id': project_id})['Item']
    response['Message'] = 'You have created a project.'
    return response

@app.route('/projects/{project_id}', api_key_required=True)
def get_project(project_id):
    project = check_id(project_table, project_id)
    return project['Item']

@app.route('/projects/{project_id}/nodes', api_key_required=True)
def get_project_nodes(project_id):
    check_id(project_table, project_id)
    nodes = node_table.scan(
        FilterExpression="assigned_to = :proj_id",
        ExpressionAttributeValues={
            ':proj_id': project_id
        }
    )
    return nodes['Items']

@app.route('/projects/{project_id}', methods=['PATCH'], api_key_required=True)
def modify_project(project_id):
    check_id(project_table, project_id)
    data = app.current_request.json_body
    updated = modify(data, project_table, project_id)
    response = {}
    response['Item'] = updated
    response['Message'] = 'Project info updated.'
    return response

########################
# Node Assignment API
########################

@app.route('/nodes/{node_id}/assign/{project_id}', methods=['PATCH'], api_key_required=True)
def assign_node(node_id, project_id):
    data = app.current_request.json_body
    check_id(node_table, node_id)
    check_id(project_table, project_id)
    node = node_table.get_item(Key={'node_id': node_id})['Item']
    if node.get('assigned_to') == project_id:
        return {"Message": "Node {} has already been assigned to project {}. No changes were made.".format(node_id, project_id)}
    elif node.get('assigned_to') != None:
        detach(node_id, node['assigned_to'])
    updated_node = node_table.update_item(
        Key={
            'node_id': node_id
        },
        UpdateExpression='SET assigned_to = :proj',
        ExpressionAttributeValues={
            ':proj': project_id
        },
        ReturnValues='ALL_NEW'
    )
    project = project_table.get_item(Key={'project_id': project_id})['Item']
    if project.get('node_list') == None:
        expression = 'SET node_list = :n'
    else:
        expression = 'SET node_list = list_append(node_list, :n)'
    updated_project = project_table.update_item(
        Key={
            'project_id': project_id
        },
        UpdateExpression=expression,
        ExpressionAttributeValues={
            ':n': [node_id]
        },
        ReturnValues='ALL_NEW'
    )
    response = {}
    response['Node'] = updated_node['Attributes']
    response['Project'] = updated_project['Attributes']
    response['Message'] = 'Node {} added to project {}.'.format(node_id, project_id)
    return response

@app.route('/nodes/{node_id}/unassign', methods=['PATCH'], api_key_required=True)
def detach_node(node_id):
    check_id(node_table, node_id)
    try:
        updated_node = node_table.update_item(
            Key={
                'node_id': node_id
            },
            UpdateExpression='REMOVE assigned_to',
            ConditionExpression='attribute_exists(assigned_to)',
            ReturnValues='UPDATED_OLD'
        )['Attributes']
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == 'ConditionalCheckFailedException':
            message = "Node was not assigned to a project. No changes were made."
            return {'Message': message}
    project_id = updated_node['assigned_to']
    return {'Message': 'Unassigned node {} from project {}'.format(node_id, project_id)}


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
    updated = table.update_item(
        Key={
            id_string: id_value
        },
        UpdateExpression=expression,
        ExpressionAttributeValues=values,
        ReturnValues='ALL_NEW'
    )
    return updated['Attributes']
