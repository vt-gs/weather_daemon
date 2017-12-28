#!/usr/bin/env python
#############################################
#   Title: Weather Daemon Main Thread       #
# Project: VTGS Relay Control Daemon        #
# Version: 1.0                              #
#    Date: Dec 27, 2017                     #
#  Author: Zach Leffke, KJ4QLP              #
# Comment:                                  #
#   -Weather Daemon Main Thread             #
#   -Intended for use with systemd          #
#############################################

import threading
import time

from logger import *
import davis
import service_thread

class Main_Thread(threading.Thread):
    def __init__ (self, args):
        threading.Thread.__init__(self, name = 'Main_Thread')
        self._stop      = threading.Event()
        self.args = args

        self.state  = 'BOOT' #BOOT, STANDBY, ACTIVE, FAULT

        #setup logger
        self.main_log_fh = setup_logger('wxd', ts=args.startup_ts, log_path=args.log_path)
        self.logger = logging.getLogger('wxd') #main logger

    def run(self):
        print "{:s} Started...".format(self.name)
        self.logger.info('Launched {:s}'.format(self.name))
        try:
            while (not self._stop.isSet()): 
                if self.state == 'BOOT':
                    #Daemon activating for the first time
                    #Activate all threads
                    #State Change:  BOOT --> STANDBY
                    #All Threads Started
                    if self._init_threads():#if all threads activate succesfully
                        self.logger.info('Successfully Launched Threads, Switching to ACTIVE State')
                        #self.set_state_standby()
                        self.set_state('ACTIVE')
                    else:
                        self.set_state('FAULT')
                    pass
                elif self.state == 'STANDBY':
                    pass
                elif self.state == 'WX_INHIBIT':
                    pass
                elif self.state == 'ADM_INHIBIT':
                    pass
                elif self.state == 'ACTIVE':
                    #Describe ACTIVE here
                    #read uplink Queue from C2 Radio thread
                    #print 'ACTIVE'
                    #if (not self.service_thread.q.empty()):
                    #    msg = self.service_thread.q.get()
                    #    print '{:s} | Service Thread RX Message: {:s}'.format(self.name, msg)
                        #self.wx_thread.tx_q.put(msg)
                    if (not self.wx_thread.rx_q.empty()):
                        wx_msg = self.wx_thread.rx_q.get()
                        print '{:s} | WX rx_q message: {:s}'.format(self.name, str(wx_msg))
                        #self._send_service_resp(rel_msg)

                    #print "Querying relays"
                    #rel_state, rel_int = self.relay_thread.read_all_relays()
                    #print rel_state, rel_int
                    #time.sleep(1)
                    pass
            
                time.sleep(0.01) #Needed to throttle CPU

        except (KeyboardInterrupt, SystemExit): #when you press ctrl+c
            print "\nCaught CTRL-C, Killing Threads..."
            self.logger.warning('Caught CTRL-C, Terminating Threads...')
            self.wx_thread.stop()
            self.wx_thread.join() # wait for the thread to finish what it's doing
            self.service_thread.stop()
            self.service_thread.join() # wait for the thread to finish what it's doing
            self.logger.warning('Terminating {:s}...'.format(self.name))
            sys.exit()
        sys.exit()

    def _send_service_resp(self,msg):
        self.service_thread._send_resp(msg)
        


    def set_state(self, state):
        self.state = state
        self.logger.info('Changed STATE to: {:s}'.format(self.state))
        if self.state == 'ACTIVE':
            time.sleep(1)
        if self.state == 'FAULT':
            pass
            time.sleep(10)

    def _init_threads(self):
        try:
            #Initialize Relay Thread
            self.logger.info('Setting up Weather_Thread')
            self.wx_thread = davis.Ethernet_VantagePro2(self.args) 
            self.wx_thread.daemon = True

            #Initialize Server Thread
            self.logger.info('Setting up Service_Thread')
            self.service_thread = service_thread.Service_Thread(self.args) 
            self.service_thread.daemon = True

            #Launch threads
            self.logger.info('Launching WX_Thread')
            self.wx_thread.start() #non-blocking
    
            self.logger.info('Launching Service_Thread')
            self.service_thread.start() #non-blocking

            return True
        except Exception as e:
            self.logger.warning('Error Launching Threads:')
            self.logger.warning(str(e))
            self.logger.warning('Setting STATE --> FAULT')
            self.state = 'FAULT'
            return False

    def stop(self):
        print '{:s} Terminating...'.format(self.name)
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
