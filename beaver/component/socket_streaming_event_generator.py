import logging
import threading
import time

logger = logging.getLogger(__name__)


class GenerateSocketStreamingEvent(threading.Thread):
    """
    Bind the <hostname> <port> and open the listening socket with <num_threads> and it keeps pushing the <data> to each
    connection in <num_threads> till the all the rows in the <data_list> is pushed with <interval> waits.
    HINT: If spark-submit job is not reading from opened connections, then code may freeze at `c, address = s.accept()`
    """

    def __init__(self, data_list, socket_obj, interval=600, cur_thread=1):
        self.data_list = data_list
        self.socket_obj = socket_obj
        self.interval = interval
        self.cur_threads = cur_thread
        threading.Thread.__init__(self)
        logger.info("data_list[0] = %s, socket = %s, interval = %d, cur_thread_count = %d",
                    data_list[0], socket_obj.getsockname(), interval, cur_thread)

    def run(self):
        logger.info("Connection ( %d ) is waiting to establish...", self.cur_threads)
        c, address = self.socket_obj.accept()
        logger.info("Connection ( %d ) is established Successfully!", self.cur_threads)

        count = 0
        for cur_row in self.data_list:
            logger.info("Pushing row[%d] = [%s] to connection = (%d) of socket = (%s)", count, cur_row,
                        self.cur_threads, self.socket_obj.getsockname)
            c.send(str(cur_row)+"\n")

            logger.info("Sleeping for %s seconds" % self.interval)
            time.sleep(self.interval)
            count = count + 1

        logger.info("Thread Completed pushing all the rows to connection = (%d) of socket = (%s)", self.cur_threads,
                    self.socket_obj.getsockname)
