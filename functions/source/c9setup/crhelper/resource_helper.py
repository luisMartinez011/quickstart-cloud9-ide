from __future__ import print_function
import threading
from crhelper.utils import _send_response
from crhelper import log_helper
import logging
import random
import boto3
import string
import json
import os

logger = logging.getLogger(__name__)

SUCCESS = 'SUCCESS'
FAILED = 'FAILED'


class CfnResource(object):

    def __init__(self, json_logging=False, log_level='DEBUG', boto_level='ERROR', polling_interval=2):
        try:
            self._create_func = None
            self._update_func = None
            self._delete_func = None
            self._poll_create_func = None
            self._poll_update_func = None
            self._poll_delete_func = None
            self._timer = None
            self._init_failed = None
            self._json_logging = json_logging
            self._log_level = log_level
            self._boto_level = boto_level
            self._send_response = False
            self._polling_interval = polling_interval
            self._sam_local = os.getenv('AWS_SAM_LOCAL')
            if not self._sam_local:
                self._lambda_client = boto3.client('lambda')
                self._events_client = boto3.client('events')
            if json_logging:
                log_helper.setup(log_level, boto_level=boto_level, RequestType='ContainerInit')
            else:
                log_helper.setup(log_level, formatter_cls=None, boto_level=boto_level)
        except Exception as e:
            logger.error(e, exc_info=True)
            self.init_failure(e)

    def __call__(self, event, context):
        try:
            if self._json_logging:
                log_helper.setup(self._log_level, boto_level=self._boto_level, RequestType=event['RequestType'],
                                 StackId=event['StackId'], RequestId=event['RequestId'],
                                 LogicalResourceId=event['LogicalResourceId'], aws_request_id=context.aws_request_id)
            else:
                log_helper.setup(self._log_level, boto_level=self._boto_level, formatter_cls=None)
            logger.debug(event)
            self.Status = SUCCESS
            self.Reason = ""
            self.PhysicalResourceId = ""
            self.StackId = event["StackId"]
            self.RequestId = event["RequestId"]
            self.LogicalResourceId = event["LogicalResourceId"]
            self.Data = {}
            self._event = event
            self._context = context
            self._response_url = event['ResponseURL']
            if self._timer:
                self._timer.cancel()
            if self._init_failed:
                return self._send(FAILED, str(self._init_failed))
            self._set_timeout()
            self._wrap_function(self._get_func())
            # Check for polling functions
            if self._poll_enabled() and self._sam_local:
                logger.info("Skipping poller functionality, as this is a local invocation")
            elif self._poll_enabled():
                # Setup polling on initial request
                if 'Poll' not in event.keys() and self.Status != FAILED:
                    logger.info("Setting up polling")
                    self._setup_polling()
                    # Returned resource id is ignored if polling is enabled
                    self.PhysicalResourceId = None
                # if physical id is set, or there was a failure then we're done
                if self.PhysicalResourceId or self.Status == FAILED:
                    logger.info("Polling complete, removing cwe schedule")
                    self._remove_polling()
                    self._send_response = True
            # If polling is not enabled, then we should respond
            else:
                self._send_response = True
            self._timer.cancel()
            if self._send_response:
                # Use existing PhysicalResourceId if it's in the event and no ID was set
                if not self.PhysicalResourceId and "PhysicalResourceId" in event.keys():
                        logger.info("PhysicalResourceId present in event, Using that for response")
                        self.PhysicalResourceId = event['PhysicalResourceId']
                # Generate a physical id if none is provided
                elif not self.PhysicalResourceId or self.PhysicalResourceId is True:
                    if "PhysicalResourceId" in event.keys():
                        logger.info("PhysicalResourceId present in event, Using that for response")
                    logger.info("No physical resource id returned, generating one...")
                    self.PhysicalResourceId = event['StackId'].split('/')[1] + '_' + event['LogicalResourceId'] + '_' + self._rand_string(8)
                self._send()
        except Exception as e:
            logger.error(e, exc_info=True)
            self._send(FAILED, str(e))

    def _poll_enabled(self):
        return "_poll_{}_func".format(self._event['RequestType'].lower())

    def create(self, func):
        self._create_func = func
        return func

    def update(self, func):
        self._update_func = func
        return func

    def delete(self, func):
        self._delete_func = func
        return func

    def poll_create(self, func):
        self._poll_create_func = func
        return func

    def poll_update(self, func):
        self._poll_update_func = func
        return func

    def poll_delete(self, func):
        self._poll_delete_func = func
        return func

    def _wrap_function(self, func):
        try:
            self.PhysicalResourceId = func(self._event, self._context)
        except Exception as e:
            logger.error(str(e), exc_info=True)
            self.Reason = str(e)
            self.Status = FAILED

    def _timeout(self):
        logger.error("Execution is about to time out, sending failure message")
        self._send(FAILED, "Execution timed out")

    def _set_timeout(self):
        self._timer = threading.Timer((self._context.get_remaining_time_in_millis() / 1000.00) - 0.5,
                                      self._timeout, args=[self._event, self._context, logger])
        self._timer.start()

    def _get_func(self):
        request_type = "_{}_func"
        if "Poll" in self._event.keys():
            request_type = "_poll" + request_type
        return getattr(self, request_type.format(self._event['RequestType'].lower()))

    def _send(self, status=None, reason=""):
        response_body = {
            'Status': self.Status,
            'PhysicalResourceId': str(self.PhysicalResourceId),
            'StackId': self.StackId,
            'RequestId': self.RequestId,
            'LogicalResourceId': self.LogicalResourceId,
            'Reason': str(self.Reason),
            'Data': self.Data,
        }
        if status:
            response_body.update({'Status': status, 'Reason': reason})
        _send_response(self._response_url, response_body)

    def init_failure(self, error):
        self._init_failed = error
        logger.error(str(error), exc_info=True)

    def _cleanup_response(self):
        for k in ["Complete", "Poll", "permission", "rule"]:
            if k in self.Data.keys():
                del self.Data[k]

    @staticmethod
    def _rand_string(l):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(l))

    def _add_permission(self, rule_arn):
        sid = self._event['LogicalResourceId'] + self._rand_string(8)
        self._lambda_client.add_permission(
            FunctionName=self._context.function_name,
            StatementId=sid,
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=rule_arn
        )
        return sid

    def _put_rule(self):
        response = self._events_client.put_rule(
            Name=self._event['LogicalResourceId'] + self._rand_string(8),
            ScheduleExpression='rate({} minutes)'.format(self._polling_interval),
            State='ENABLED',

        )
        return response["RuleArn"]

    def _put_targets(self, func_name):
        region = self._event['rule'].split(":")[3]
        account_id = self._event['rule'].split(":")[4]
        rule_name = self._event['rule'].split("/")[1]
        self._events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': 'arn:aws:lambda:%s:%s:function:%s' % (region, account_id, func_name),
                    'Input': json.dumps(self._event)
                }
            ]
        )

    def _remove_targets(self, rule_arn):
        self._events_client.remove_targets(
            Rule=rule_arn.split("/")[1],
            Ids=['1']
        )

    def _remove_permission(self, sid):
        self._lambda_client.remove_permission(
            FunctionName=self._context.function_name,
            StatementId=sid
        )

    def _delete_rule(self, rule_arn):
        self._events_client.delete_rule(
            Name=rule_arn.split("/")[1]
        )

    def _setup_polling(self):
        self._event['Poll'] = True
        self._event['rule'] = self._put_rule()
        self._event['permission'] = self._add_permission(self._event['rule'])
        self._put_targets(self._context.function_name)

    def _remove_polling(self):
        error = False
        if 'rule' in self._event.keys():
            self._remove_targets(self._event['rule'])
        else:
            logger.error("Cannot remove CloudWatch events rule, Rule arn not available in event")
            error = True
        if 'permission' in self._event.keys():
            self._remove_permission(self._event['permission'])
        else:
            logger.error("Cannot remove lambda events permission, permission id not available in event")
            error = True
        if 'rule' in self._event.keys():
            self._delete_rule(self._event['rule'])
        else:
            logger.error("Cannot remove CloudWatch events target, Rule arn not available in event")
            error = True
        if error:
            raise Exception("failed to cleanup CloudWatch event polling")
