import boto3
import pandas as pd
import yaml
import aws_helper

aws_credentials_file=open('credentials.yaml')
aws_credentials =yaml.load(aws_credentials_file, Loader=yaml.FullLoader)
AWS_KEY=aws_credentials['KEY']
AWS_SECRET=aws_credentials['SECRET']

def open_tcp_port():
    """
    If required, will open a VPC port to the redshift cluster. Not currently used.
    """
    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    try:
        print('OPENING TCP PORT')
        vpc = ec2.Vpc(id=aws_helper.VPC_ID)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(aws_helper.CLUSTER_PORT),
            ToPort=int(aws_helper.CLUSTER_PORT)
        )
    except Exception as e:
        print(e)

def main():
    """
    The code in this file has been adapted from Lesson 3 Data Warehousing, Exercise 2 
    in the Udacity DataEngineering NanoDegree.
    It creates the redshift cluster
    """
    ## Set up client
    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    ## Create redshift cluster
    try:
        print('SETTING UP REDSHIFT CLUSTER')
        response = redshift.create_cluster(        
            #HW
            ClusterType=aws_helper.CLUSTER_TYPE,
            NodeType=aws_helper.CLUSTER_NODE_TYPE,
            # NumberOfNodes=int(aws_helper.CLUSTER_NUM_NODES),

            #Identifiers & Credentials
            DBName=aws_helper.DB,
            ClusterIdentifier=aws_helper.CLUSTER_IDENTIFIER,
            MasterUsername=aws_helper.DB_USER,
            MasterUserPassword=aws_helper.DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[aws_helper.IAM_ROLE_ARN]  
        )
        print('SUCCESSFULLY SET UP REDSHIFT CLUSTER')
    except Exception as e:
        print(e)
    
    clusterProps = redshift.describe_clusters(ClusterIdentifier=aws_helper.CLUSTER_IDENTIFIER)['Clusters'][0]

    try:
        aws_helper.set_cluster_endpoint(clusterProps['Endpoint']['Address'])
        aws_helper.set_vpc(clusterProps['VpcId'])
    except Exception as e:
        print(e)


def delete_cluster():
    """
    Can be called to delete the Redshift cluster created above
    """
    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    redshift.delete_cluster(ClusterIdentifier=aws_helper.CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

if __name__ == "__main__":
    main()