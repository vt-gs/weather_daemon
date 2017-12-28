#!/usr/bin/env python
#############################################
#   Title: Numato Ethernet Relay Interface  #
# Project: VTGS Relay Control Daemon        #
# Version: 2.0                              #
#    Date: Dec 15, 2017                     #
#  Author: Zach Leffke, KJ4QLP              #
# Comment:                                  #
#   -Relay Control Daemon                   #
#   -Intended for use with systemd          #
#############################################

import sys
import threading
import logging
import time
import socket
import binascii
import datetime
import struct
import numpy


from Queue import Queue
from watchdog_timer import *

class Ethernet_VantagePro2(threading.Thread):
    def __init__ (self, args):
        threading.Thread.__init__(self, name = 'Weather_Thread')
        self._stop      = threading.Event()
        self.ip         = args.wx_ip
        self.port       = args.wx_port
        self.rate       = args.wx_rate

        self.logger     = logging.getLogger('wxd')
        print "Initializing {}".format(self.name)
        self.logger.info("Initializing {}".format(self.name))

        self.loop_wd    = Watchdog(self.rate, self._loop_watchdog_event)

        self.connected  = False

        self.loop_q       = Queue() #messages into thread
        self.rx_q         = Queue() #messages into thread

    def run(self):
        print "{:s} Started...".format(self.name)
        self.logger.info('Launched {:s}'.format(self.name))
        self.loop_wd.start()

        if (not self.connected):
            self._connect()
 
        while (not self._stop.isSet()):
            while (not self.loop_q.empty()):
                msg = self.loop_q.get()        
                print msg
            time.sleep(0.25) #Query station every 5 seconds

        self.loop_wd.stop()
        self.logger.warning('{:s} Terminated'.format(self.name))
        sys.exit()

    def _loop_watchdog_event(self):
        self.loop_wd.reset()
        ts = datetime.datetime.utcnow()
        #print ts, 'Loop Watchdog Fired'
        if self.connected:
            self._loop_cmd()

    def _parse_loop_msg(self, frame, ts = None):
        #print msg['ts'], len(msg['msg']), 
        print binascii.hexlify(frame)
        msg = {}
        msg['ts'] = ts
        #print numpy.uint8(struct.unpack('<B',frame[0:1]))[0] #ACK
        #print frame[1:4] #LOO  

        #3 Hour Barometer Trend
        #-60 = Falling Rapidly = 196 (as an unsigned byte)
        #-20 = Falling Slowly = 236 (as an unsigned byte)
        #0 = Steady
        #20 = Rising Slowly
        #60 = Rising Rapidly   
        msg['bar_trend']    = int(frame[4])    #Bar Trend
        msg['pkt_type']     = int(frame[5])    #Packet Type, 0 = LOOP, 1 = LOOP2
        msg['next_record']  = binascii.hexlify(frame[6:8]) #NExt Record See Manual
        msg['barometer']    = round(numpy.int16(struct.unpack('<H',frame[8:10]))[0]/1000.0, 6) #In. Hg.
        msg['inside_temp']  = round(numpy.int16(struct.unpack('<h',frame[10:12]))[0]/10.0, 6) #deg F
        msg['inside_hum']   = round(numpy.int8(struct.unpack('<B',frame[12:13]))[0]/100.0, 6) # % humidity
        msg['outside_temp'] = round(numpy.int16(struct.unpack('<h',frame[13:15]))[0]/10.0, 6) #deg F
        msg['wind_speed']   = numpy.uint8(struct.unpack('<B',frame[15:16]))[0] #mph
        msg['wind_avg']     = numpy.uint8(struct.unpack('<B',frame[16:17]))[0] #mph
        msg['wind_dir']     = numpy.uint16(struct.unpack('<H',frame[17:19]))[0] #deg F
        msg['extra_temps']  = binascii.hexlify(frame[19:26])
        msg['soil_temps']   = binascii.hexlify(frame[26:30])
        msg['leaf_temps']   = binascii.hexlify(frame[30:34])
        msg['outside_hum']  = round(numpy.int8(struct.unpack('<B',frame[34:35]))[0]/100.0, 6) # % humidity
        msg['leaf_temps']   = binascii.hexlify(frame[35:42])
        msg['rain_rate']    = numpy.uint16(struct.unpack('<H',frame[42:44]))[0]/100.0 #inches/hour
        msg['uv_index']     = numpy.uint8(struct.unpack('<B',frame[44:45]))[0] #uv index
        msg['solar_rad']    = round(numpy.uint16(struct.unpack('<H',frame[45:47]))[0], 6) #watts/m^2
        msg['battery']      = numpy.uint16(struct.unpack('<H',frame[88:90]))[0]*300.0/512.0/100.0 #Volts
        msg['storm_date']   = binascii.hexlify(frame[49:51])
        msg['day_rain']     = numpy.uint16(struct.unpack('<H',frame[51:53]))[0]/100.0 #inches/hour
        msg['month_rain']   = numpy.uint16(struct.unpack('<H',frame[53:55]))[0]/100.0 #inches/hour
        msg['year_rain']    = numpy.uint16(struct.unpack('<H',frame[55:57]))[0]/100.0 #inches/hour
        msg['day_et']     = numpy.uint16(struct.unpack('<H',frame[57:59]))[0]/1000.0 #inches/hour
        msg['month_et']   = numpy.uint16(struct.unpack('<H',frame[59:61]))[0]/100.0 #inches/hour
        msg['year_et']    = numpy.uint16(struct.unpack('<H',frame[61:63]))[0]/100.0 #inches/hour
        msg['the_rest']     = binascii.hexlify(frame[47:])
        #print msg['bar_trend']
        return msg

    def _loop_cmd(self):
        self.sock.send('LOOP 1\r\n')
        #time.sleep(.05)
        data, addr = self.sock.recvfrom(1024)
        ts = datetime.datetime.utcnow()
        if ((len(data) == 100) and (ord(data[0]) == 0x06)):  #LOOP command should return packet of length 100
            #print binascii.hexlify(data[0])
            loop_msg = self._parse_loop_msg(bytearray(data), ts)
            self.loop_q.put(loop_msg)


    def _connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #TCP Socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(1)   #set socket timeout
        
        print 'Attempting to connect to weather station: [{:s}:{:s}]'.format(self.ip, str(self.port))
        self.logger.info('Attempting to connect to weather station: [{:s}:{:s}]'.format(self.ip, str(self.port)))
        try:
            self.sock.connect((self.ip, self.port))
            print 'Succesful connection to weather station: {:s}:{:s}'.format(self.ip, str(self.port))
            self.logger.info('Succesful connection to weather station: [{:s}:{:s}]'.format(self.ip, str(self.port)))
            self.connected = True
            print 'Connected!'
        except socket.error as msg:
            print "Exception Thrown: " + str(msg) + " (" + str(self.timeout) + "s)"
            print "Unable to connect to Remote Relay at IP: " + str(self.ip) + ", Port: " + str(self.port)  
            self.connected = False
            self.logger.info('Failed to connect to weather station: [{:s}:{:s}]'.format(self.ip, str(self.port)))
            self.connected = False

        time.sleep(0.5)

    def disconnect(self):
        #disconnect from wx station
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.connected = False

    def stop(self):
        print '{:s} Terminating...'.format(self.name)
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
