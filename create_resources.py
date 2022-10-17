import configparser
import boto3
import time
from zipfile import ZipFile
import os
import logging
import shutil
import subprocess
import pandas as pd
# import pandasETL


def update_workspace():
    '''
    Update workspace packages. 
    '''
    os.system('pip uninstall awscli')
    os.system('rm -rf awscliv2.zip')
    os.system('rm -rf aws')
    os.system('curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"')
    os.system('unzip awscliv2.zip')
    os.system('sudo ./aws/install')
    os.system('sudo apt-get update && sudo apt-get install -yy less && apt install openssh-client')
    os.system('pip install geopy')
    os.system('pip install s3fs')

def get_config_variables():
    ''' 
    Get configuration variables. 
    '''

    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    AWS_ACCESS_KEY_ID = config.get('default', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('default', 'AWS_SECRET_ACCESS_KEY')
    
    # set environment variables for lcoal python file
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('default', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('default', 'AWS_SECRET_ACCESS_KEY')
    os.environ['AWS_DEFAULT_REGION'] = config.get('default', 'REGION')
    os.environ['AWS_DEFAULT_FORMAT'] = config.get('default', 'FORMAT')
    
    # create ~/.aws/config file
    os.system(f'aws configure set default.region us-east-1; aws configure set AWS_ACCESS_KEY_ID "{AWS_ACCESS_KEY_ID}" ; \
                    aws configure set AWS_SECRET_ACCESS_KEY "{AWS_SECRET_ACCESS_KEY}"')

    if not os.path.exists('/root/.aws'):
        path = '/root/.aws'
        subprocess.call(f'mkdir {path}')

    os.system('export AWS_CONFIG_FILE=~/.aws/config')
    os.system('export AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials')

    BUCKET = config.get('S3', 'BUCKET')
    BUCKET_NAME = config.get('S3', 'BUCKET_NAME')

    Ec2KeyName = config.get('EC2', 'Ec2KeyName')
    Ec2SubnetId = config.get('EC2', 'Ec2SubnetId')
    pemFile = config.get('EC2', 'pemFile')

    ClusterName = config.get('EMR', 'ClusterName')

    return BUCKET, BUCKET_NAME, Ec2KeyName, Ec2SubnetId, pemFile, ClusterName


def createClients():
    '''
    Create clients and resources for ec2, s3, iam, redshift.
    '''

    logging.info(f'Creating clients and resources.')
    ec2Resource = boto3.resource('ec2')
    ec2Client = boto3.client('ec2')
    s3Client = boto3.client('s3')
    s3Resource = boto3.resource('s3')
    iam = boto3.client('iam')
    redshift = boto3.client('redshift')
    emr = boto3.client('emr')

    logging.info(
        'Created the following clients: ec2Client, s3Client, iam, redshift, emr and resources: ec2Resource, s3Resource.')

    return ec2Resource, ec2Client, s3Client, s3Resource, iam, redshift, emr


def createS3Bucket(client, resource, bucket_name):
    '''
    Create S3 bucket in us-east-1.
        parameters:
            client: S3 client
            resource: S3 resource
            bucket_name: S3 bucket name
    '''
    try:
        logging.info(f'Attempting to creat bucket {bucket_name}.')
        client.create_bucket(Bucket=bucket_name)
        logging.info(
            f'Successfully created bucket {resource.Bucket(bucket_name)}.')
    except Exception as e:
        print(e)


def convert_state_data(file):
    ''' 
    Convert all TXT files for name data to JSON.
        parameters:
            file: file to convert from csv
    '''
    state_df = pd.read_csv(file, error_bad_lines=False)
    jsonPath = f'raw_data/states.json'
    state_df.to_json(jsonPath, orient='records',)

    return jsonPath


def get_storm_data(src):
    '''
    Pull storm data from url and write to csv.
        parameters:
            src: source link for storm data
    '''

    raw_data = pd.read_csv(src, header=None, sep='\n', )
    storm_data = raw_data[0].str.split(',', expand=True)
    
    csvPath = 'raw_data/raw_storm_data.csv'
    storm_data.to_csv(csvPath)
        
    return csvPath


def upload_data_to_s3(*args):
    '''Upload names_by_state, primary_results, us-cities-demographics, and states information to the S3 bucket.
        parameters:
            *args: Paths for files to be uploaded, bucket name, and S3 client
    '''
    file_paths, bucket_name, s3Client = args
    files = []
    
    for path in file_paths:
        if 'name_data_by_state' in path:
            zippedFile = ZipFile('raw_data/namesbystate.zip')
            zippedFile.extractall(path)

            files += ['raw_data/name_data_by_state/' +
                         file for file in zippedFile.namelist() if file.endswith('.TXT')]
        else:
            files.append(path)

    logging.info(f'Beginning file uploads to bucket "{bucket_name}".')

    try:
        for file_name in files:
            s3Client.upload_file(file_name, bucket_name, file_name)
            logging.info(file_name + ' successfully uploaded to bucket.')
            print(file_name + ' successfully uploaded to bucket.')
    except Exception as e:
        print(e)


def create_key_pair(resource, key, pemFile):
    ''' Create a new key pair. '''
    new_keypair = resource.create_key_pair(KeyName=key)

    with open(f'/home/workspace/{pemFile}', 'w') as file:
        file.write(new_keypair.key_material)
        os.chmod(f'/home/workspace/{pemFile}', 0o400)

    # move to hidden folder
    shutil.move(f"/home/workspace/{pemFile}",
                os.path.join('/root/.aws', f'{pemFile}'))


def create_emr_cluster(client, key, subnetId, cluster):
    ''' Create an EMR cluster with Spark.
        parameters:
            client: emr client
            key: EC2 key pair name
            subnetId: EC2 subnet id
            cluster: cluster name

        Source: https://hands-on.cloud/working-with-emr-in-python-using-boto3/#h-working-with-emr-clusters-using-boto3
    '''

    logging.info('Created EMR cluster in us-east-1')
    response = client.run_job_flow(
        Name=cluster,
        ReleaseLabel='emr-5.28.0',
        Instances={
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 10
                                },
                                'VolumesPerInstance': 1
                            },
                        ],
                        'EbsOptimized': False
                    }
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 10
                                },
                                'VolumesPerInstance': 1
                            },
                        ],
                        'EbsOptimized': False
                    }
                },
            ],
            'Ec2KeyName': key,
            'Ec2SubnetId': subnetId,
            'HadoopVersion': 'Amazon',
        },
        Applications=[
            {
                'Name': 'Spark',
            },
        ],
        VisibleToAllUsers=True,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole'
    )

    return


def describe_emr_cluster(client):
    '''
    Get cluster id and master node for the EMR cluster.
    parameters:
            client: emr client
    '''
    clusterResp = client.list_clusters(
        ClusterStates=[
            'STARTING', 'WAITING', 'TERMINATING', 'TERMINATED']
    )

    clusterStatus = clusterResp['Clusters'][0]['Status']['State']
    cluster_id = clusterResp['Clusters'][0]['Id']

    print('Cluster creation in progress. Checking status every 30 seconds.\n')

    try:
        start_time = time.time()
        while clusterStatus != 'WAITING':
            time.sleep(30)
            clusterResp = client.list_clusters(
                ClusterStates=[
                    'STARTING', 'WAITING', 'TERMINATING', 'TERMINATED']
            )
            clusterStatus = clusterResp['Clusters'][0]['Status']['State']
            print('Current status:', clusterStatus)

            if clusterStatus in ('TERMINATED', 'TERMINATING', 'TERMINATED WITH ERRORS'):
                break

            end_time = time.time()
            elapsed_time = (end_time - start_time) / 60
            print("Elapsed time: ~{:.2f} seconds.".format(
                (end_time - start_time)))

        describe_emr = client.list_instances(
            ClusterId=cluster_id,
            InstanceGroupTypes=[
                'MASTER'
            ],
        )

        print('EMR cluster created successfully.\n')
        master_node = describe_emr['Instances'][0]['PublicDnsName']

    except Exception as e:
        print(e)

    return master_node


def connect_to_emr(pemFile, masterNode):
    '''
    Connect to EMR cluster with SSH.
    parameters:
            pemFile: pem file for SSH connection
            masterNode: cluster master node for SSH connection
    '''
    try:
        print('\nAttempting cluster connection with SSH.\n')
        resp = f'ssh -i ~/.aws/{pemFile} hadoop@{masterNode}'
        print(resp)
        os.system(resp)
        logging.info('Successfully connected to EMR cluster.\n')
    except Exception as e:
        print(e)

    return


def copy_files(pemFile, masterNode):
    '''
    Copy relevant files to Hadoop for ETL execution.
    parameters:
            pemFile: pem file for SSH connection
            etlFile: etl file for SSH connection
            configFile: config file for Hadoop
            masterNode: cluster master node for SSH connection
    '''

    cmds = []
    copyPemFile = f'scp -i ~/.aws/{pemFile} ~/.aws/{pemFile} hadoop@{masterNode}:/home/hadoop/'
    cmds.append(copyPemFile)

    copyEtlFile = f'scp -i ~/.aws/{pemFile} mainETL.py hadoop@{masterNode}:/home/hadoop/'
    cmds.append(copyEtlFile)

    copyConfigFile = f'scp -i ~/.aws/{pemFile} dl.cfg hadoop@{masterNode}:/home/hadoop/'
    cmds.append(copyConfigFile)

    copyReqFile = f'scp -i ~/.aws/{pemFile} requirements.txt hadoop@{masterNode}:/home/hadoop/'
    cmds.append(copyReqFile)

    for cmd in cmds:
        os.system(cmd)

    # reconnect to EMR with SSH
    connect_to_emr(pemFile, masterNode)
    
    return


def main():
    '''
    Main function to perform Pre ETL processing.
    '''
    start_time = time.time()

    update_workspace()

    BUCKET, BUCKET_NAME, Ec2KeyName, Ec2SubnetId, pemFile, ClusterName = get_config_variables()

    ec2Resource, ec2Client, s3Client, s3Resource, iam, redshift, emr = createClients()

    createS3Bucket(s3Client, s3Resource, BUCKET_NAME)
    
    print('\nConverting state data from csv to json.\n')

    jsonPath = convert_state_data('raw_data/states.csv')
    
    print('Writing storm data from a url to csv.\n')

    csvPath = get_storm_data('https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2021-041922.txt')

    files = [jsonPath, csvPath, 'raw_data/name_data_by_state/', 'capstone_data_dictionary.xlsx']

    upload_data_to_s3(files, BUCKET_NAME, s3Client)

    print('\nCreating a key pair in us-east-1.\n')
    
    create_key_pair(ec2Resource, Ec2KeyName, pemFile)

    create_emr_cluster(emr, Ec2KeyName, Ec2SubnetId, ClusterName)

    master_node = describe_emr_cluster(emr)

    end_time = time.time()

    elapsed_time = (end_time - start_time) / 60

    print("Pre ETL processing completed in: ~{:.2f} seconds or ~{:.2f} minutes.".format(
        (end_time - start_time), elapsed_time))

    connect_to_emr(pemFile, master_node)

    copy_files(pemFile, master_node)


if __name__ == "__main__":
    main()
