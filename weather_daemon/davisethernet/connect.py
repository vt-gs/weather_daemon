
import socket
import time


def connect(host, port, num_retry=5, retry_delay=5, timeout=10):
    """Make a connection to the requested host and port.  This function
    will attempt to make a socket connection waiting for 'timeout' seconds
    for each attempt.  If the socket connects the function returns with the
    socket handle.  If the connection attempt fails (e.g. timed out) additional
    attempts will be made.  A delay of 'retry_delay' seconds between attempts
    and up to 'num_retry' attempts will be made.  Note, this is a blocking
    function.  Connection attempts and delays will block execution until they
    complete.  A value of 'None' will be returned if the connection attempt(s)
    fail.
    """
    connected = False
    sock      = None
    count     = 0
    while not connected and count < num_retry:
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            connected = True
        except Exception, err:
            count += 1
            time.sleep(retry_delay)
    return sock
