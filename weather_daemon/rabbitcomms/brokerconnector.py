import pika
import json
import logging
import time
from threading import Thread


class BrokerConnector(object):
    """BrokerConnector provides basic functionality to support the connection
    "sequence" of a typical Pika to RabbitMQ connection.  Individual
    functions could and should be overridden if alternate or additional
    functionality is desired.  Derived classes for consumers and producers
    would need to implement the final steps in the connection process.
    Specifically, both producers and consumers would pick up at the
    'on_exchange_declareok' function and re-implement that to customize their
    behaviour.  Since the producer doesn't need to declare a queue it could
    simply start publishing to the exchange.  The consumer on the other hand
    would continue with the declaration and binding of a queue.
    """

    def __init__(self, amqp_url, exchange, exchange_type='direct',
                 queue='', loggername=None):
        """Create a new instance of the connector class, passing in the RabbitMQ
        connection URL and exchange/routing information.  The specified exchange
        will be created if it does not exist.

        :param str amqp_url:      The Rabbit connection url.
        :param str exchang:       The name of the exchange to interact with.
        :param str exchange_type: The RabbitMQ exchange type.  If the exchange
                                  already exists the type _must_ agree with
                                  the existing value or an error will be thrown.
        :param str queue:         The name of the queue used for interaction.  If
                                  the string is empty (the default) a new, unique
                                  queue will be created by RabbitMQ.
        """
        # Store the passed-in parameters in local (class) data...
        self.url           = amqp_url
        self.exchange      = exchange
        self.exchange_type = exchange_type
        self.loggername    = loggername
        # ...and set up the storage for the connection data
        self.connection    = None
        self.channel       = None
        self.closing       = False
        self.consumer_tag  = None


    def run(self):
        """Run the BrokerConnector-derived instance by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self.connection = self.connect()
        self.connection.ioloop.start()

    def connect(self):
        """Connect to the RabbitMQ server and return the connection handle.
        When the connection is established, on_connection_open will be called.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Connecting to %s', self.url)
        return pika.SelectConnection(pika.URLParameters(self.url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, conn):
        """Called when the connection to the RabbitMQ server has been
        established.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """Add an "on close" callback that will be called whenever
        the server closes the connection unexpectedly.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """Called when the server connection is closed unexpectedly. Try
        to reconnect after a small delay.
        """
        self.channel = None
        if self.closing:
            self.connection.ioloop.stop()
        else:
            if self.loggername is not None:
                logging.getLogger(self.loggername).warning('Connection closed, reopening in 5 seconds: (%s) %s',
                                 reply_code, reply_text)
            self.connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Called by the IOLoop timer if the connection is closed. See the
        on_connection_closed method.
        """
        # This is the old connection IOLoop instance, stop its ioloop
        self.connection.ioloop.stop()

        if not self.closing:

            # Create a new connection
            self.connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self.connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with the server and provide the "on open" callback.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """Called when the channel has been opened (we registered it in the
        channel open call).  We'll also declare the exchange.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """Register a callback for when/if the server closes the channel.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Called when the server closes the channel.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).warning('Channel %i was closed: (%s) %s',
                              channel, reply_code, reply_text)
        if not self.closing:
            self.connection.close()

    def setup_exchange(self, exchange_name):
        """Declare the exchange on the server. We'll register a "declare OK"
        callback which will be called when at the completion of the exchange
        declaration on the server.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Declaring exchange "%s"', exchange_name)
        self.channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.exchange_type)

    def on_exchange_declareok(self, frame):
        """Called when the server has finished the creation of the exchange.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Exchange declared successfully')

    def close_channel(self):
        """Close the channel with the server cleanly.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Closing the channel')
        if self.channel:
            self.channel.close()

    def stop(self):
        """Cleanly shutdown the connection to the server.
        """
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Stopping consumer')
        self.closing = True
        self.connection.ioloop.start()

    def close_connection(self):
        """Close the connection to server"""
        if self.loggername is not None:
            logging.getLogger(self.loggername).debug('Closing connection')
        self.connection.close()
