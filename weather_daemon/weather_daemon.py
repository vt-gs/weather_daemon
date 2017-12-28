#!/usr/bin/env python
#########################################
#   Title: Weather Monitoring Daemon    #
# Project: VTGS Weather Daemon          #
# Version: 1.0                          #
#    Date: Dec 27, 2017                 #
#  Author: Zach Leffke, KJ4QLP          #
# Comment:                              #
#   -Weather Monitoring Daemon          #
#   -Intended for use with systemd      #
#########################################

import math
import string
import time
import sys
import os
import datetime
import logging
import json

#from optparse import OptionParser
from main_thread import *
import argparse


def main():
    """ Main entry point to start the service. """

    startup_ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    #--------START Command Line argument parser------------------------------------------------------
    parser = argparse.ArgumentParser(description="Weather Monitoring Daemon")

    service = parser.add_argument_group('Daemon Service connection settings')
    service.add_argument('--ser_ip',
                         dest='ser_ip',
                         type=str,
                         default='0.0.0.0',
                         help="Service IP",
                         action="store")
    service.add_argument('--ser_port',
                         dest='ser_port',
                         type=int,
                         default='4000',
                         help="Service Port",
                         action="store")

    wx = parser.add_argument_group('Weather Station connection settings')
    wx.add_argument('--wx_ip',
                       dest='wx_ip',
                       type=str,
                       default='10.42.0.70',
                       help="Weahter Station IP",
                       action="store")
    wx.add_argument('--wx_port',
                       dest='wx_port',
                       type=int,
                       default='10001',
                       help="Weather Station Port",
                       action="store")
    wx.add_argument('--wx_rate',
                       dest='wx_rate',
                       type=int,
                       default='5',
                       help="Weather Station Query Rate (seconds)",
                       action="store")

    other = parser.add_argument_group('Other daemon settings')
    other.add_argument('--log_path',
                       dest='log_path',
                       type=str,
                       default='/log/wxd',
                       help="Relay daemon logging path",
                       action="store")
    other.add_argument('--startup_ts',
                       dest='startup_ts',
                       type=str,
                       default=startup_ts,
                       help="Daemon startup timestamp",
                       action="store")
    #other.add_argument('--config_file',
    #                   dest='config_file',
    #                   type=str,
    #                   default="relay_config_vul.json",
    #                   help="Daemon startup timestamp",
    #                   action="store")

    args = parser.parse_args()
    #--------END Command Line argument parser------------------------------------------------------ 

    main_thread = Main_Thread(args)
    main_thread.daemon = True
    main_thread.run()
    sys.exit()
    

if __name__ == '__main__':
    main()
    

