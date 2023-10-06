"""Amazon boto3 interface."""

from __future__ import annotations

try:
    import boto3
    from botocore import exceptions
    from botocore.awsrequest import AWSRequest
    from botocore import parsers
except ImportError:
    boto3 = None

    class _void:
        pass

    class BotoCoreError(Exception):
        pass
    exceptions = _void()
    exceptions.BotoCoreError = BotoCoreError
    AWSRequest = _void()


__all__ = (
    'exceptions', 'AWSRequest', 'get_response'
)

def get_response(operation_model, http_response):
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status_code,
    }
    response_dict['body'] = http_response.content

    parser = parsers.create_parser('query')
    return http_response, parser.parse(
        response_dict, operation_model.output_shape
    )
