"""Amazon boto3 interface."""

from __future__ import annotations

try:
    import boto3
    from botocore import exceptions
    from botocore import parsers
    from botocore.awsrequest import AWSRequest
    from botocore.response import StreamingBody
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


def get_response(operation_model, http_response, protocol):
    """
    This is a copy of botocore`s get_response

    https://github.com/boto/botocore/blob/1.29.126/botocore/response.py#L180-L201

    method with the exception of hard-coding the protocol to 'query' instead of
    reading it from the given `operation_model`. The reason for this is
    explained in detail here:

    https://github.com/celery/kombu/issues/1783#issuecomment-1750019844

    References:
    - https://github.com/celery/kombu/issues/1726
    - https://github.com/celery/kombu/pull/1759
    - https://github.com/celery/kombu/issues/1783#issuecomment-1747739904
    - https://github.com/celery/kombu/issues/1783#issuecomment-1750019844
    """
    # Use 'query' instead of `operation_model.metadata['protocol']` as in the
    # original implementation of botocore's `get_response`. The rest of the
    # method is copied verbatim from botocore's original implementation.
    # protocol = 'query'  # operation_model.metadata['protocol']
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status_code,
    }
    # TODO: Unfortunately, we have to have error logic here.
    # If it looks like an error, in the streaming response case we
    # need to actually grab the contents.
    if response_dict['status_code'] >= 300:
        response_dict['body'] = http_response.content
    elif operation_model.has_streaming_output:
        response_dict['body'] = StreamingBody(
            http_response.raw, response_dict['headers'].get('content-length')
        )
    else:
        response_dict['body'] = http_response.content

    parser = parsers.create_parser(protocol)
    return http_response, parser.parse(
        response_dict, operation_model.output_shape
    )
