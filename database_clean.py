import boto3
from uuid import uuid4
from decimal import Decimal

# dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

node_table = dynamodb.Table('nodes')
project_table = dynamodb.Table('projects')

# Delete the tables
node_table.delete()
project_table.delete()
