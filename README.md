# VTGS Weather Monitor Daemon
Daemon for monitoring the Davis VantagePro2 weather station.  This daemon extracts data from an Ethernet-connected Davis weather station
(specifically a VantagePro2) and sends messages to a RabbitMQ broker. Its
overall design:
  - Connects to a Davis VantagePro2 weather station via an attached
    serial-to-ethernet port.
  - The socket connection to the Davis is maintained for as long as this
    script is running.  If the ethernet-to-serial supports only a single
    connection other connections will be blocked.
  - Connection to the RabbitMQ broker is maintained for as long as this
    script is running.
  - All data is pulled from the Davis device each time through the main
    loop.  There is a (currently) hard-coded delay in the loop to reduce
    CPU requirements and reduce the query rate on the Davis device. Making
    the delay too short could "confuse" the weather station and/or the
    serial-to-ethernet device.
  - Data pulled from the Davis device is extracted from the native structures
    and turned into a Python dictionary.
  - The combined dictionary of all weather data is broken into messages to
    be sent via an external YAML file.  The structure of the file is a list
    of structures that define the routing key, delay, and keys to be sent
    for each message.  There is no limit to the number of messages defined
    nor is there a limit on the number of fields per message.
  - Failed/lost connections on either the broker or Davis device will be
    immediately reconnected.
  - Communication to the RabbitMQ broker utilize the VTGS common Python
    packages 'rabbitcomms' and 'davisethernet'.  Neither packge is threaded
    although both are thread safe.  As such, a separate thread is started
    to service the RabbitMQ messages.  The connection/data retrieval from
    the Davis device is synchronous, blocking until data is returned.  Note
    that threading is probably not necessary since we're pulling data, then
    publishing data in a rather sequential manner.  The threading of the
    RabbitMQ comms keeps the delays specified reasonably close to the rates
    requested in the YAML file, especially if we fire off a large number of
    messages in rapid succession.
  - This script never terminates (there is a 'while True' loop)
