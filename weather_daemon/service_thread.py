#!/usr/bin/env python
#############################################
#   Title: Weather Daemon Service Thread    #
# Project: VTGS Relay Control Daemon        #
# Version: 2.0                              #
#    Date: Dec 15, 2017                     #
#  Author: Zach Leffke, KJ4QLP              #
# Comment:                                  #
#   -                                       #
#############################################

import threading
import time
import socket
import errno

from Queue import Queue
from logger import *

class Service_Thread(threading.Thread):
    def __init__ (self, args):
        threading.Thread.__init__(self, name = 'Service_Thread')
        self._stop  = threading.Event()
        self.args   = args

        self.ip     = args.ser_ip
        self.port   = args.ser_port
        self.q      = Queue()

        self.state  = 0x00

        self.logger = logging.getLogger('wxd')
        print "Initializing {}".format(self.name)
        self.logger.info("Initializing {}".format(self.name))

    def run(self):
        print "{:s} Started...".format(self.name)
        self.logger.info('Launched {:s}'.format(self.name))
        try:
            self.rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.rx_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.rx_sock.settimeout(0.01) #
            self.rx_sock.setblocking(0)
            self.rx_sock.bind((self.ip, self.port))
            self.logger.info("Ready to receive Service data on: [{}:{}]".format(self.ip, self.port))
            print "Ready to receive Service data on: [{}:{}]".format(self.ip, self.port)
        except Exception as e:
            print 'Some other Exception occurred:', e
            self.logger.info("Could not set up Service data on: {}:{}".format(self.ip, self.port))
            self.logger.info(e)
            sys.exit()

        while (not self._stop.isSet()):
            try:
                #data, addr = self.rx_sock.recvfrom(1024)
                data, addr= self.rx_sock.recvfrom(1024)
                data = data.strip('\n') 
                if data:
                    #print addr, data
                    print "\n[{:s}:{:d}]->[{:s}:{:d}] Received User Message: {:s}".format(addr[0], addr[1], self.ip, self.port, data)
                    self.logger.info("[{:s}:{:d}]->[{:s}:{:d}] Received User Message: {:s}".format(addr[0], addr[1], self.ip, self.port, data))
                    self.q.put(data)
            except socket.error, v:
                errorcode=v[0]
                #print v
                if errorcode==errno.EWOULDBLOCK:  #Expected, No data on uplink
                    #print 'socket timeout'
                    pass
            except Exception as e:
                print 'Some other Exception occurred:', e
                self.logger.info(e)
            
            time.sleep(0.01) #Needed to throttle CPU

        self.rx_sock.close()
        self.logger.warning('{:s} Terminated'.format(self.name))
        sys.exit()

    def _send_resp(self, msg):
        print "{:s} | Sending Response: {:s}".format(self.name, str(msg))

    def stop(self):
        print '{:s} Terminating...'.format(self.name)
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()



