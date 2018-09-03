# coding=utf-8

import os, sys, logging, threading

class StreamingThread(threading.Thread):
    def __init__(self, storage_client, container_name, blob_name, pipe_path):
        threading.Thread.__init__(self)
        #super(StreamingThread, self).__init__()

        self.storage_client = storage_client
        self.container_name = container_name
        self.blob_name = blob_name
        self.pipe_path = pipe_path
        self.exception = None

    def get_exception(self):
        return self.exception

    def run(self):
        logging.debug("StreamingThread.run(): Start streaming upload for {} to {}/{}".format(self.pipe_path, self.container_name, self.blob_name))

        try:
            with open(self.pipe_path, "rb", buffering=0) as stream:
                #
                # For streaming to work, we need to ensure that 
                # use_byte_buffer=True and 
                # max_connections=1 are set
                #
                self.storage_client.create_blob_from_stream(
                    container_name=self.container_name,
                    blob_name=self.blob_name, stream=stream,
                    use_byte_buffer=True, max_connections=1)
                logging.debug("Finished streaming upload of {}/{}".format(self.container_name, self.blob_name))
                os.remove(self.pipe_path)

                logging.debug("Finished streaming upload of {}/{}".format(self.container_name, self.blob_name))
        except Exception as e:
            logging.fatal("Exception during streaming upload: {}".format(e.message))
            self.exception = e

    def stop(self):
        logging.debug("Requested cancellation of upload to {}/{}".format(self.container_name, self.blob_name))
        if os.path.exists(self.pipe_path):
            os.remove(self.pipe_path)
        # TODO Kill thread... 
        #
        # The problem is that 
        # (a) Python does not allow me to kill a thread from the outside, and 
        # (b) the create_blob_from_stream() method does not allow external cancellation.
        #
