import boto3
from uuid import uuid4
from decimal import Decimal

# dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

node_table = dynamodb.Table('nodes')
project_table = dynamodb.Table('projects')

print(node_table.key_schema)

# # Generate node_id
# node_1 = str(uuid4())
# # Create a node with id = 1 for ease of debugging
# node_table.put_item(
#     Item={
#         'node_id': '1',
#         'shipping_status': 'pending',
#         'config_status': 'unconfigured'
#     },
#     ConditionExpression='attribute_not_exists(node_id)'
# )
#
# # Attempt to overwrite the node
# try:
#     node_table.put_item(
#         Item={
#             'node_id': '2',
#             'shipping_status': 'shipping',
#             'config_status': 'unconfigured'
#         },
#         ConditionExpression='attribute_not_exists(node_id)'
#     )
# except ConditionalCheckFailedException:
#     print("you can't create a node with the same id as an existing node")
# # Returns the below error code:
# # botocore.errorfactory.ConditionalCheckFailedException
#
# # Update the node with latitude, longitude and change shipping status
# node_table.update_item(
#     Key={
#         'node_id': '1'
#     },
#     UpdateExpression='SET latitude = :lat, longitude = :long, shipping_status = :ship',
#     ExpressionAttributeValues={
#         ':lat': Decimal('4.569'),
#         ':long': Decimal('2.382'),
#         ':ship': 'shipping'
#     }
# )
#
# # node_table.delete_item(Key={
# #     'node_id': 'c71f0396-ace9-4241-afb3-593a47d415c7'
# # })
#
# # Generate project_id
# # project_1 = str(uuid4())
#
# # Create project
# project_table.put_item(
#     Item={
#         'project_id': '1',
#         'project_name': 'Berkeley Network',
#         'customer_name': 'City of Berkeley',
#         'start_date': '01-01-2016',
#         'end_date': '12-31-2017'
#     },
#     ConditionExpression='attribute_not_exists(node_id)'
# )
#
# # Update project
# project_table.update_item(
#     Key={
#         'project_id': '2'
#     },
#     UpdateExpression='SET project_name = :name, customer_name = :cust',
#     ExpressionAttributeValues={
#         ':name': 'SF Network',
#         ':cust': 'City of San Francisco',
#     }
# )

# Add project to node
# node_table.update_item(
#     Key={
#         'node_id': '1'
#     },
#     UpdateExpression='ADD project_id = :proj',
#     ExpressionAttributeValues={
#         ':proj': '1'
#     }
# )

# node_table.update_item(
#     Key={
#         'node_id': '1'
#     },
#     UpdateExpression='REMOVE project_id'
# )

# Add node to project
# project_table.update_item(
#     Key={
#         'project_id': '1'
#     },
#     UpdateExpression='SET node_list = list_append(node_list, :n)',
#     ExpressionAttributeValues={
#         ':n': ['1']
#     }
# )

# add a second node to a project
# project_table.update_item(
#     Key={
#         'project_id': '1'
#     },
#     UpdateExpression='ADD node_list :n',
#     ExpressionAttributeValues={
#         ':n': {'3', '4'}
#     }
# )

# remove one node from the project
# if all nodes are gone, node_list attribute is removed
# project_table.update_item(
#     Key={
#         'project_id': '1'
#     },
#     UpdateExpression='REMOVE node_list :n',
#     ExpressionAttributeValues={
#         ':n': '2'
#     }
# )

# Delete the tables
# node_table.delete()
# project_table.delete()
