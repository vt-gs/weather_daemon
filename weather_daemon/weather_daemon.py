#!/usr/bin/env python
###############################################################################
#    Title: Weather Monitoring Daemon
#  Project: VTGS Weather Daemon
#  Version: 2.0
#     Date: Jan 2018
#   Author: T Krauss
# Overview:
#            - Weather Monitoring Daemon
#      ToDo:
#            - Verify all keys in the combined structure are unique
#            - Verify extraction of data from native structures is correct
#            - Decide on a policy for how/when to log various failures.
#              We should log failures to conenct (both the Davis and RabbitMQ)
#              but how and where is a "best guess" (or non-existant) at this
#              point.
#            - Verify the logging format.  REVIEW: the logging format seems to
#              be reasonable but it should agree with all the other daemons
#              and code so should be looked at.
###############################################################################
# Design:
# This daemon extracts data from an Ethernet-connected Davis weather station
# (specifically a VantagePro2) and sends messages to a RabbitMQ broker. Its
# overall design:
#   - Connects to a Davis VantagePro2 weather station via an attached
#     serial-to-ethernet port.
#   - The socket connection to the Davis is maintained for as long as this
#     script is running.  If the ethernet-to-serial supports only a single
#     connection other connections will be blocked.
#   - Connection to the RabbitMQ broker is maintained for as long as this
#     script is running.
#   - All data is pulled from the Davis device each time through the main
#     loop.  There is a (currently) hard-coded delay in the loop to reduce
#     CPU requirements and reduce the query rate on the Davis device. Making
#     the delay too short could "confuse" the weather station and/or the
#     serial-to-ethernet device.
#   - Data pulled from the Davis device is extracted from the native structures
#     and turned into a Python dictionary.
#   - The combined dictionary of all weather data is broken into messages to
#     be sent via an external YAML file.  The structure of the file is a list
#     of structures that define the routing key, delay, and keys to be sent
#     for each message.  There is no limit to the number of messages defined
#     nor is there a limit on the number of fields per message.
#   - Failed/lost connections on either the broker or Davis device will be
#     immediately reconnected.
#   - Communication to the RabbitMQ broker utilize the VTGS common Python
#     packages 'rabbitcomms' and 'davisethernet'.  Neither packge is threaded
#     although both are thread safe.  As such, a separate thread is started
#     to service the RabbitMQ messages.  The connection/data retrieval from
#     the Davis device is synchronous, blocking until data is returned.  Note
#     that threading is probably not necessary since we're pulling data, then
#     publishing data in a rather sequential manner.  The threading of the
#     RabbitMQ comms keeps the delays specified reasonably close to the rates
#     requested in the YAML file, especially if we fire off a large number of
#     messages in rapid succession.
#   - This script never terminates (there is a 'while True' loop)
###############################################################################
import time
import sys
from datetime import timedelta, datetime
import logging
import json

#from optparse import OptionParser
from main_thread import *
import argparse
import ConfigParser # Note that this is renamed to configparser in Python 3
import yaml
from threading import Thread
from rabbitcomms import BrokerProducer
import davisethernet.read
import configutils

CONFIG_FILE_NAME = 'weather_daemon.conf'
LOG_FORMAT       = ('%(name)-8s %(levelname)-9s %(asctime)10s %(funcName)-35s %(message)s')


def main():
    """ Main entry point for the never-ending daemon service. """

    ###
    # Load the configuration/setup.  Parameters will be loaded from a collection
    # of file locations (see the configutils module) as well as the command line
    # parameters.
    config = configutils.load_config(CONFIG_FILE_NAME)
    #configutils.prettyprint(config)
    #
    ###

    ###
    # Set up the logging.  Some of these parameters were pulled from the command
    # line so we had to wait until here to set this up.
    logger = logging.getLogger( config.get('logging', 'logger_name') )
    if config.get('logging', 'level').upper() == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif config.get('logging', 'level').upper() == 'INFO':
        logger.setLevel(logging.INFO)
    elif config.get('logging', 'level').upper() == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif config.get('logging', 'level').upper() == 'ERROR':
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.CRITICAL)

    if config.getboolean('logging', 'log_to_file'):
        handler = logging.FileHandler(args.log_path)
    else:
        handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    #
    ###

    ###
    # Grab the message definitions from the YAML file.
    # DANGER: There is no try around this so if it fails the script will
    # fail to start.  REVIEW: This seems like a reasonable choice if we
    # haven't or can't define messages but it may need to be looked at.
    with open(config.get('defaults', 'yaml_file'), 'r') as stream:
        messages = yaml.load(stream)
    #
    ###

    ###
    # Set up the connection to the broker.  We'll also create the "producer"
    # which is, at this point, simply an instance of the BrokerProducer from
    # the VTGS 'rabbitcomms' package.  No customization or derived class is
    # needed (for now).
    amqp_url      = 'amqp://{}:{}@{}:{}/%2F'.format(
                         config.get('broker', 'user'),
                         config.get('broker', 'pass'),
                         config.get('broker', 'host'),
                         config.get('broker', 'port'))
    producer = BrokerProducer(amqp_url,
                              config.get('broker', 'exchange'),
                              exchange_type=config.get('broker', 'exchange_type'),
                              loggername=config.get('logging', 'logger_name'),
                              updatedelay=config.getfloat('broker', 'update_delay'))
    # Since the BrokerProducer class is not threaded but we might need to fire off
    # a lot of messages "at the same time" we'll set up a thread to run the
    # consumer.  This allows us to jam a lot of messages into the producer
    # and let it/the thread send them out without this main thread blocking.
    # That just keeps our delay loop more "in sync" with the delays specified
    # in the YAML file.
    tp = Thread(target=producer.run)
    tp.daemon = True;
    tp.start()
    #
    ###

    ###
    # Set up the list of delays.  The YAML file read defines a list of
    # dictionaries, each entry being a message.  Each message defined in the
    # YAML file has an associated delay (inter-message time) as the
    # messages[i]['delay'] dictionary value.  Lets make a list (via list
    # comprehension) of the delays.  Each entry will be a datetime
    # timedelta value so we can add it to the current time to get the next
    # "event time" (time to send the message).
    numMess = len(messages)
    current_time  = datetime.now()
    deltas = [timedelta(seconds=messages[i]['delay']) for i in range(numMess)]
    trigger_times = [current_time+dt for dt in deltas]
    #
    ###

    ###
    # Finally ready for the main loop.  We'll stay in this "forever" - or
    # at least until killed by someone.

    # Connect to the Davis device.  This is just the initial attempt to
    # connect.  We'll verify the connection every time through the loop to
    # ensure we always have a connection.
    wx_ip = config.get('station', 'host')
    wx_port = config.getint('station', 'port')
    sock = davisethernet.connect(wx_ip, wx_port, num_retry=5, retry_delay=2, timeout=2)
    while True:
        # ...each time through the loop...
        # when are we?
        current_time  = datetime.now()

        # Check the socket to make sure we have a connection.  Note that this
        # doesnt really "check" that the connection is live.  Under Linux we
        # may not get that notice until we try to communicate via the socket.
        while sock is None:
            logger.debug('Cannot connect to weather station at {}:{}'.format(wx_ip, wx_port))
            # REVIEW: Probably should fire off a message to the broker if we
            # fail to get a connection after a certain amount of time.
            sock = davisethernet.connect(wx_ip, wx_port, num_retry=5, retry_delay=2, timeout=2)
            time.sleep(10)

        # Presumably we have a connection so try to wake the device.
        device_awake = davisethernet.wakedevice(sock, num_retry=3, retry_delay=1)
        awake_tries = 0
        brk_ip = config.get('broker', 'host')
        brk_port = config.getint('broker', 'port')
        while not device_awake:
            LOGGER.debug('Cannot wake weather station at {}:{}'.format(brk_ip, brk_port))
            # REVIEW: Probably should fire off a message to the broker if we
            # fail to get a connection after a certain amount of time.
            device_awake = davisethernet.wakedevice(sock, num_retry=3, retry_delay=1)
            if awake_tries > MAX_AWAKE_TRIES:
                break
            time.sleep(10)

        # Have a connection and the device is awake.  Pull all the data we
        # need/want.  REVIEW: We're pulling all the data each time through the
        # loop even though we probably don't need it all.  This is a bit
        # inefficient but since there isn't too much data it should be OK.
        loop   = davisethernet.read.LOOP(sock)
        loop2  = davisethernet.read.LOOP2(sock)
        hldata = davisethernet.read.HILOWS(sock)
        # Create the "combined" dictinary.  This is the complete set of data
        # key-values that we'll extract message data from.
        combined = loop.copy()
        combined.update(loop2)
        combined.update(hldata)

        # Run through the list of all messages read from the YAML file and
        # see which, if any, should be sent at this point in time.
        for i, t in enumerate(trigger_times):
            if current_time > t:
                # OK, it seems that the current time is after the "trigger
                # time" of this message.  Let's fire it off...

                # First extract the list of data items from the combined
                # dictionary.  The list of keys for the message was pulled from
                # the YAML file so we'll use those keys to pull values from
                # combined with a dictionary comprehension.
                mess = {key: combined[key] for key in messages[i]['keys']}

                # Send the message to the AMQP broker.  The routing key for the
                # message was pulled from the YAML file.  REVIEW: we're assuming
                # that the messages are simple JSON dictionaries (maps) - probably
                # OK but we may want to support different message types/structures.
                producer.send(json.dumps(mess), messages[i]['routing_key'])

                # Since we just sent the message, increment the trigger time
                # to send this message again.
                trigger_times[i] = current_time + deltas[i]

                # REVIEW: we're _not_ waiting for an acknowledge from either
                # the AMQP broker nor any client.  This should be OK since we
                # don't necessarily need a weather client attached and can
                # control message dropping at the broker level (persistence in
                # the exchange).

        # Delay within the main loop to reduce the CPU overhead and limit
        # the calling rate to the DAVIS device.  REVIEW: The delay specified
        # was pulled out of the air - it's not based on much more than a
        # guess but it seems reasonable.
        time.sleep( config.getfloat('defaults', 'main_loop_delay') )


if __name__ == '__main__':
    main()
    

