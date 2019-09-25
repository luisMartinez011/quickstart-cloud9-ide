from __future__ import print_function
from crhelper import CfnResource
import logging
import boto3
import json
from time import sleep

logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=True, log_level='DEBUG', boto_level='CRITICAL')

try:
    ec2_client = boto3.client('ec2')
    ssm_client = boto3.client('ssm')
    cfn_client = boto3.client('cloudformation')
except Exception as e:
    helper.init_failure(e)


def ssm_ready(instance_id):
    try:
        response = ssm_client.describe_instance_information(Filters=[{'Key': 'InstanceIds', 'Values': [instance_id]}])
        logger.debug(response)
        return True
    except ssm_client.exceptions.InvalidInstanceId:
        return False


def get_command_output(instance_id, command_id):
    response = ssm_client.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
    if response['Status'] in ['Pending', 'InProgress', 'Delayed']:
        return
    return response


def send_command(instance_id, commands):
    logger.debug("sending command to %s : %s" % (instance_id, commands))
    try:
        return ssm_client.send_command(InstanceIds=[instance_id], DocumentName='AWS-RunShellScript',
                                       Parameters={'commands': commands})
    except ssm_client.exceptions.InvalidInstanceId:
        logger.debug("Failed to execute ssm command", exc_info=True)
        return


@helper.create
def create(event, context):
    logger.debug("Got Create")
    response = ec2_client.describe_instances(Filters=[{
        'Name': 'tag:aws:cloud9:environment', 'Values': [event['ResourceProperties']['Cloud9Environment']]
    }])
    instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
    ec2_client.associate_iam_instance_profile(
        IamInstanceProfile={'Name': event['ResourceProperties']['InstanceProfile']},
        InstanceId=instance_id)    
    while not ssm_ready(instance_id):
        retries -= 1
        if retries == 0:
            raise Exception("Timed out waiting for instance to register with ssm")
        sleep(15)


@helper.poll_create
def poll_create(event, context):
    logger.info("Got create poll")
    instance_response = ec2_client.describe_instances(Filters=[{
        'Name': 'tag:aws:cloud9:environment', 'Values': [event['ResourceProperties']['Cloud9Environment']]
    }])
    instance_id = instance_response['Reservations'][0]['Instances'][0]['InstanceId']
    bootstrap_path = event['ResourceProperties']['BootstrapPath']
    ssm_param_response = ssm_client.get_parameter(Name=event['ResourceProperties']['SSMParamStore'])
    size = json.loads(ssm_param_response['Value'])['size']
    retries = 6
    while True:
        commands = ['mkdir -p /tmp/bootstrap', 'cd /tmp/bootstrap',
                    'aws s3 cp ' + bootstrap_path + ' ./bootstrap.sh --quiet',
                    'chmod +x ./bootstrap.sh', './bootstrap.sh ' + size]
        send_response = send_command(instance_id, commands)
        if send_response:
            break
        retries -= 1
        if retries == 0:
            return
        sleep(10)
    retries = 40
    while True:
        try:
            cmd_output_response = get_command_output(instance_id, send_response['Command']['CommandId'])
            break
        except ssm_client.exceptions.InvocationDoesNotExist:
            logger.debug('invocation not available in ssm yet', exc_info=True)
        retries -= 1
        if retries == 0:
            raise Exception("Timed out waiting for ssm command to complete")
        sleep(15)
    if cmd_output_response['StandardErrorContent']:
        raise Exception("ssm command failed: " + cmd_output_response['StandardErrorContent'][:235])
    return instance_id


def handler(event, context):
    helper(event, context)
