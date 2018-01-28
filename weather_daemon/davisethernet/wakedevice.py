import socket
import time

def wakedevice(sock, num_retry=3, retry_delay=1):
    """Wake the devce connected to the provided socket.  This function
    will attempt to send the wake up string ('\n') and receive the anticipated
    response ('\n\r').  If the device responds the function returns with the
    value True.  If the awake attempt fails (e.g. timed out, device dead, etc.)
    additional attempts will be made.  A delay of 'retry_delay' seconds between
    attempts and up to 'num_retry' attempts will be made.  Note, this is a
    blocking function.  Wake up attempts and delays will block execution until
    they complete.  A value of 'False' will be returned if the wake up attempt(s)
    fail, or 'True' if the device is found and ready.
    """
    awake = False
    count = 0
    if sock is not None:
        while not awake and count < num_retry:
            try:
                sock.send('\n')
                res = sock.recv(1024)
                if (len(res) == 2)  and  (res[0] == '\n')  and  (res[1] == '\r'):
                    awake = True
            except Exception, err:
                print(err)

            time.sleep(retry_delay)
            count += 1

    return awake
