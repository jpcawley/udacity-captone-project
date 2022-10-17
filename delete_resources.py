import configparser
import boto3
import logging
import os
import datetime

def get_config_variables():
    ''' 
    Get configuration variables. 
    '''
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('default', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('default', 'AWS_SECRET_ACCESS_KEY')
    os.environ['AWS_DEFAULT_REGION'] = config.get('default', 'REGION')
    os.environ['AWS_DEFAULT_FORMAT'] = config.get('default', 'FORMAT')

    ARN = config.get('IAM_ROLE', 'ARN')
    BUCKET_NAME = config.get('S3', 'BUCKET_NAME')

    Ec2KeyName = config.get('EC2', 'Ec2KeyName')
    
    return ARN, BUCKET_NAME, Ec2KeyName
    
def createClients():
    ''' 
    Create client for emr.
    '''
    
    ec2= boto3.client('ec2')
    emr = boto3.client('emr')
    iam = boto3.client('iam')
    s3Resource = boto3.resource('s3')
    
    logging.info('EC2 and EMR clients successfully created.')
    
    return ec2, emr, iam, s3Resource

def deleteS3Resources(bucket_name, resource):
    '''
    Empty and delete S3 resources.
    
    parameters:
        bucket_name: bucket_name to be deleted
        resource: s3 resource 
    '''
    
    delete = str(input('Do you want to delete your S3 resources? (y/n) '))
    if delete.upper() == 'Y':
        try:
            deleteBckt = str(input('To delete this resource type "permanently delete": '))

            if deleteBckt.upper() == 'PERMANENTLY DELETE':
                s3Bucket = resource.Bucket(bucket_name)

                for file in s3Bucket.objects.all():
                    resource.Object(bucket_name, file.key).delete()

                print(f"S3 bucket '{bucket_name}' successfully emptied.")

                s3Bucket.delete(Bucket=bucket_name,)

                print(f"S3 bucket '{bucket_name}' successfully deleted.\n")

            else:
                print('Invalid input.\n')
                return deleteS3Resources(bucket_name, resource)
        except Exception as e:
            print(e)
    else:
        print('S3 bucket resources not deleted. This decision could cost YOU alot.\n')
        

def delete_emr_cluster(client):
    '''
    Terminate EMR cluster.
    
    parameters:
        client: emr client

    Source: https://hands-on.cloud/working-with-emr-in-python-using-boto3/#h-working-with-emr-clusters-using-boto3
    '''
    delete = str(input('Do you want to delete your EMR resources? (y/n) '))
    if delete.upper() == 'Y':
        try:
            deleteClstr = str(input('To delete this resource type "terminate": '))

            if deleteClstr.upper() == 'TERMINATE':
                get_cluster = client.list_clusters(
                    CreatedAfter=datetime.datetime(2022, 9, 1),
                    ClusterStates=[
                        'WAITING'
                    ]
                )

                cluster_id = get_cluster['Clusters'][0]['Id']

                response = client.terminate_job_flows(
                    JobFlowIds=[cluster_id]
                )
                
                print(f"EMR cluster with ID: {cluster_id} terminating.\n")

            else:
                print('Invalid input.\n')
                return delete_emr_cluster(client)
        except Exception as e:
            print(e)
    else:
        print('EMR cluster NOT terminated. This decision could cost YOU alot.\n')

def delete_key_pair(client, key):
    ''' 
    Delete key pair. 
        parameters:
            client: ec2 client
            key: key pair name
    '''
    try:
        response = client.delete_key_pair(
            KeyName=key
        )
        print('Key pair successfully deleted.\n')
    except Exception:
        print('This key pair does not exist.\n')
    
def cleanup_workspace():
    os.system('rm -rf awscliv2.zip')
    os.system('rm -rf aws')
    
def main():
    '''
    Post ETL clean up.
    '''
    ARN, BUCKET_NAME, Ec2KeyName = get_config_variables()
    
    print('Creating service clients and resources for project clean up.')
    
    ec2, emr, iam, s3Resource = createClients()
    
    print('Deleting S3 Resources.')
    
    deleteS3Resources(BUCKET_NAME, s3Resource)
    
    print('Deleting EMR Resources.')
    
    delete_emr_cluster(emr)
    
    print('Deleting EC2 key-pair Resources.')
    
    delete_key_pair(ec2, Ec2KeyName)
    
    cleanup_workspace()
    

if __name__ == "__main__":
    main()