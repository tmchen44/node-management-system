import boto3

# dynamodb = boto3.resource('dynamodb', region_name='us-west-1)
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Create table to store nodes
node_table = dynamodb.create_table(
    TableName='nodes',
    KeySchema=[
        {
            'AttributeName': 'node_id',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'node_id',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Create table to store projects
project_table = dynamodb.create_table(
    TableName='projects',
    KeySchema=[
        {
            'AttributeName': 'project_id',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'project_id',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until table exists
node_table.meta.client.get_waiter('table_exists').wait(TableName='nodes')
project_table.meta.client.get_waiter('table_exists').wait(TableName='projects')

# Place first item to act as counter
node_table.put_item(
    Item={
        'node_id': '0',
        'current_id': 0
    }
)

project_table.put_item(
    Item={
        'project_id': '0',
        'current_id': 0
    }
)
