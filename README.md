# Node Management System
This backend application provides a RESTful API for managing nodes and projects.

## Getting Started
### Using the API
1. Install httpie (link to website [here](https://httpie.org/doc#installation))
2. Make API requests as outlined in `documentation.pdf`.

### Deploying the API
1. Install Python 3, Chalice, and Boto3 and their associated dependences.
2. Set up your AWS credentials to properly run DynamoDB with a serverless app; set up IAM to use an API key.
3. Run `python database_setup.py`, then run `chalice deploy`.

## Project Overview
With the API, equipment nodes and projects can be created, read, and updated. Nodes can be associated and deassociated with projects. Please see `documentation.pdf` for more details regarding the API.
