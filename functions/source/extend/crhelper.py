# -*- coding: utf-8 -*-
#
# crhelper.py
#
# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##################################################################################################

from __future__ import print_function
import boto3
import logging
import json
from botocore.vendored import requests
import threading
import random
import string

lambda_client = boto3.client('lambda')
events_client = boto3.client('events')

def log_config(event, loglevel=None, botolevel=None):
    if 'ResourceProperties' in event.keys():
        if 'loglevel' in event['ResourceProperties'] and not loglevel:
            loglevel = event['ResourceProperties']['loglevel']
        if 'botolevel' in event['ResourceProperties'] and not botolevel:
            botolevel = event['ResourceProperties']['botolevel']
    if not loglevel:
        loglevel = 'warning'
    if not botolevel:
        botolevel = 'error'

    loglevel = getattr(logging, loglevel.upper(), 20)
    botolevel = getattr(logging, botolevel.upper(), 40)
    mainlogger = logging.getLogger()
    mainlogger.setLevel(loglevel)
    logging.getLogger('boto3').setLevel(botolevel)
    logging.getLogger('botocore').setLevel(botolevel)
    logfmt = '[%(requestid)s][%(asctime)s][%(levelname)s] %(message)s \n'
    mainlogger.handlers[0].setFormatter(logging.Formatter(logfmt))
    return logging.LoggerAdapter(mainlogger, {'requestid': event['RequestId']})

log = log_config({"RequestId": "CONTAINER_INIT"})

def send(event, context, response_status, response_data, physical_resource_id, logger, reason=None):

    response_url = event['ResponseURL']
    logger.debug("CFN response URL: " + response_url)

    response_body = dict()
    response_body['Status'] = response_status
    msg = 'See details in CloudWatch Log Stream: ' + context.log_stream_name
    if not reason:
        response_body['Reason'] = msg
    else:
        response_body['Reason'] = str(reason)[0:255] + '... ' + msg

    if physical_resource_id:
        response_body['PhysicalResourceId'] = physical_resource_id
    elif 'PhysicalResourceId' in event:
        response_body['PhysicalResourceId'] = event['PhysicalResourceId']
    else:
        response_body['PhysicalResourceId'] = context.log_stream_name

    response_body['StackId'] = event['StackId']
    response_body['RequestId'] = event['RequestId']
    response_body['LogicalResourceId'] = event['LogicalResourceId']
    if response_data and response_data != {} and response_data != [] and isinstance(response_data, dict):
        response_body['Data'] = response_data

    json_response_body = json.dumps(response_body)

    logger.debug("Response body:\n" + json_response_body)

    headers = {
        'content-type': '',
        'content-length': str(len(json_response_body))
    }

    try:
        response = requests.put(response_url,
                                data=json_response_body,
                                headers=headers)
        logger.info("CloudFormation returned status code: " + response.reason)
    except Exception as e:
        logger.error("send(..) failed executing requests.put(..): " + str(e))
        raise

def timeout(event, context, logger):
    logger.error("Execution is about to time out, sending failure message")
    send(event, context, "FAILED", {}, "", reason="Execution timed out", logger=logger)

def cfn_handler(event, context, create_func, update_func, delete_func, poll_func, logger, init_failed):

    logger.info("Lambda RequestId: %s CloudFormation RequestId: %s" %
                (context.aws_request_id, event['RequestId']))

    response_data = {}

    physical_resource_id = None

    logger.debug("EVENT: " + json.dumps(event))

    if init_failed:
        send(event, context, "FAILED", response_data, physical_resource_id, init_failed, logger)
        raise init_failed

    t = threading.Timer((context.get_remaining_time_in_millis()/1000.00)-0.5,
                        timeout, args=[event, context, logger])
    t.start()

    try:
        logger.info("Received a %s Request" % event['RequestType'])
        if 'Poll' in event.keys():
            physical_resource_id, response_data = poll_func(event, context)
        elif event['RequestType'] == 'Create':
            physical_resource_id, response_data = create_func(event, context)
        elif event['RequestType'] == 'Update':
            physical_resource_id, response_data = update_func(event, context)
        elif event['RequestType'] == 'Delete':
            physical_resource_id, response_data = delete_func(event, context)

        if "Complete" in response_data.keys():
            if 'Poll' in event.keys():
                remove_poll(event, context)

            logger.info("Completed successfully, sending response to cfn")
            send(event, context, "SUCCESS", cleanup_response(response_data), physical_resource_id, logger=logger)
        else:
            logger.info("Stack operation still in progress, not sending any response to cfn")

    except Exception as e:
        reason = str(e)
        logger.error(e, exc_info=True)
        try:
            remove_poll(event, context)
        except Exception as e2:
            logger.error("Failed to remove polling event")
            logger.error(e2, exc_info=True)
        send(event, context, "FAILED", cleanup_response(response_data),
             physical_resource_id, reason=reason, logger=logger)
    finally:
        t.cancel()

def cleanup_response(response_data):
    for k in ["Complete", "Poll", "permission", "rule"]:
        if k in response_data.keys():
            del response_data[k]
    return response_data

def rand_string(l):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(l))

def add_permission(context, rule_arn):
    sid = 'QuickStartStackMaker-' + rand_string(8)
    lambda_client.add_permission(
        FunctionName=context.function_name,
        StatementId=sid,
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com',
        SourceArn=rule_arn
    )
    return sid

def put_rule():
    response = events_client.put_rule(
        Name='QuickStartStackMaker-' + rand_string(8),
        ScheduleExpression='rate(2 minutes)',
        State='ENABLED',

    )
    return response["RuleArn"]

def put_targets(func_name, event):
    region = event['rule'].split(":")[3]
    account_id = event['rule'].split(":")[4]
    rule_name = event['rule'].split("/")[1]
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                'Id': '1',
                'Arn': 'arn:aws:lambda:%s:%s:function:%s' % (region, account_id, func_name),
                'Input': json.dumps(event)
            }
        ]
    )

def remove_targets(rule_arn):
    events_client.remove_targets(
        Rule=rule_arn.split("/")[1],
        Ids=['1']
    )

def remove_permission(context, sid):
    lambda_client.remove_permission(
        FunctionName=context.function_name,
        StatementId=sid
    )

def delete_rule(rule_arn):
    events_client.delete_rule(
        Name=rule_arn.split("/")[1]
    )

def setup_poll(event, context):
    event['rule'] = put_rule()
    event['permission'] = add_permission(context, event['rule'])
    put_targets(context.function_name, event)

def remove_poll(event, context):
    error = False
    if 'rule' in event.keys():
        remove_targets(event['rule'])
    else:
        log.error("Cannot remove CloudWatch events rule, Rule arn not available in event")
        error = True
    if 'permission' in event.keys():
        remove_permission(context, event['permission'])
    else:
        log.error("Cannot remove lambda events permission, permission id not available in event")
        error = True
    if 'rule' in event.keys():
        delete_rule(event['rule'])
    else:
        log.error("Cannot remove CloudWatch events target, Rule arn not available in event")
        error = True
    if error:
        raise Exception("failed to cleanup CloudWatch event polling")
