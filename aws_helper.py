import configparser

"""
A helper file to abstract the config access
"""

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DB_HOST=config.get('DB','HOST')
DB=config.get('DB','NAME')
DB_USER=config.get('DB','USER')
DB_PASSWORD=config.get('DB','PASSWORD')
DB_PORT=config.get('DB','PORT')

CLUSTER_TYPE=config.get('CLUSTER','TYPE')
CLUSTER_NODE_TYPE=config.get('CLUSTER','NODE_TYPE')
CLUSTER_IDENTIFIER=config.get('CLUSTER','IDENTIFIER')
CLUSTER_NUM_NODES=config.get('CLUSTER','NUM_NODES')

S3_LOG_DATA=config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA=config.get('S3', 'SONG_DATA')

IAM_ROLE_ARN=config.get('IAM_ROLE','ARN')

VPC_ID='VPC_ID'

def set_cluster_endpoint(endpoint):
    DB_HOST=endpoint

def set_vpc(vpcId):
    VPC_ID=vpcId