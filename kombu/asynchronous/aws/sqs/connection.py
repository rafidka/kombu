"""Amazon SQS Connection."""

from __future__ import annotations

from kombu.asynchronous import get_event_loop
from kombu.asynchronous.aws.connection import AsyncAWSQueryConnection

from .ext import boto3

__all__ = ('AsyncSQSConnectionV2',)

import queue
import threading


class AsyncSQSConnectionV2:
    # TODO Rename sqs_connection to sqs_client.
    def __init__(self, sqs_connection, debug=0, region=None, n_threads=50, **kwargs):
        self.sqs_client = sqs_connection
        # Queue for the requests
        self.request_queue = queue.Queue()
        
        # Queue for the results
        self.result_queue = queue.Queue()
        self.hub = kwargs.get('hub') or get_event_loop()
        
        # Start threads to process requests
        for _ in range(n_threads):
            threading.Thread(target=self._worker_thread, daemon=True).start()

        # Start a thread to pull results and call callbacks
        # threading.Thread(target=self._callback_thread, daemon=True).start()
        self._callback_thread_tref = self.hub.call_repeatedly(
            0.1, self._callback_loop)

    # TODO Ensure this is being called.
    def close(self):
        self._callback_thread_tref.cancel()

    def send_request(self, api_name, callback=None, *args,**kwargs):
        """Send a request to be processed asynchronously.

        Args:
        - request_func: The boto3 request function.
        - *args, **kwargs: Arguments and keyword arguments for request_func.
        - callback: Callback to be called with the result.
        """
        self.request_queue.put((api_name, args, kwargs, callback))

    def _worker_thread(self):
        """Thread to process requests."""

        while True:
            api_name, args, kwargs, callback = self.request_queue.get()
            method = getattr(self.sqs_client, api_name)
            try:
                result = method(*args, **kwargs)
                if callback:
                    self.result_queue.put((callback, result, None))
            except Exception as e:
                if callback:
                    self.result_queue.put((callback, None, e))
            self.request_queue.task_done()

    def _callback_loop(self):
        # todo avoid infinite loops.
        # todo put a time limit to this statement.
        while True:
            try:
                callback, result, exception = self.result_queue.get_nowait()
            except queue.Empty:
                return
            if exception:
                # callback(None, exception)
                # TODO We need to log the exception.
                print(exception)
            else:
                callback(result)
            self.result_queue.task_done()

    def receive_message(
        self, queue_url, number_messages=1, visibility_timeout=None,
        attributes=('ApproximateReceiveCount',), wait_time_seconds=None,
        callback=None
    ):
        kwargs = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": number_messages,
            "MessageAttributeNames": attributes,
            "WaitTimeSeconds": wait_time_seconds,
        }
        if visibility_timeout:
            kwargs["VisibilityTimeout"] = visibility_timeout

        return self.send_request('receive_message', callback, **kwargs)

    def delete_message(self, queue_url, receipt_handle, callback=None):
        return self.send_request('delete_message', callback,
                                 QueueUrl=queue_url,
                                 ReceiptHandle=receipt_handle)
