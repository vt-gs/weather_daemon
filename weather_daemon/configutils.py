import argparse
import ConfigParser # Note that this is renamed to configparser in Python 3

# In order to make this routine a little more useful for other applications
# we can map each command line parameter to a configuration file section
# and variable name.  This map takes the command line parameter name as a key
# and maps it to a tuple of (section, variable name).  CAUTION: If we reuse this
# in a different application we still need to make sure all the command line
# parameters have mappings here so this isn't as simple as it could be.
CMD_PARAM_MAPPING = {
    'message_yaml_file': ('defaults', 'yaml_file'),
    'wx_host':           ('station', 'hodt'),
    'wx_port':           ('station', 'port'),
    'log_path':          ('logging', 'path'),
    'log_to_file':       ('logging', 'log_to_file'),
    'log_level':         ('logging', 'level'),
    'brk_host':          ('broker', 'host'),
    'brk_port':          ('broker', 'port'),
    'brk_pass':          ('broker', 'pass'),
    'brk_user':          ('broker', 'user'),
}


def load_config(filename):
    """Read configuration value from four sources:
      1. /etc/.../<filename>
      2. ./<filename>
      3. values from --config command line argument (file)
      4. command line arguments
    The order gives the precedence so variables in ./<filename> (number 2)
    override this from /etc/.../<filename> (number 1 above).  Command line
    arguments will have the highest precedence and will therefore override
    any configuration file values.  Returns the ConfigParser object loaded with
    the desired values.
    """

    # We'll build the initial list of ordered files from which to load
    # configurations.  This is the base location list and will be used if no
    # other options are passed.  Then parse the command line arguments and add
    # any config file specified there to the list.  Read the composite list
    # of command file locations in the precedence order.  Finally, override
    # the configuration options/values with command-line values.
    # REVIEW: What should the "real" paths be here?  Certainly /etc is not the
    # right place for our files...
    configFileNames = ['/etc/{}'.format(filename), # /etc/filename
                       './{}'.format(filename)]    # ./filename

    ###
    # Parse the command line arguments to set the required and optional
    # parameters.

    # Set up the argument parser.  Note that the "formatter_class" is, here,
    # just being used to "widen" the help output.  It extends the allowable
    # with to 50 characters rather then the comically narrow 20 characters.
    parser = argparse.ArgumentParser(description="Weather Station Reporting Daemon",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=50))

    # Define the parameters we need/expect from the command line.  Note that
    # all the defaut values are specified as constants.
    wx = parser.add_argument_group('Weather station connection settings')
    wx.add_argument('--wx_host',
                       dest='wx_host',
                       help="Weather station host name/IP",
                       action="store")
    wx.add_argument('--wx_port',
                       dest='wx_port',
                       help="Weather Station Port",
                       action="store")

    wx = parser.add_argument_group('Message broker connection settings')
    wx.add_argument('--brk_host',
                       dest='brk_host',
                       help="AMQP broker host name/IP",
                       action="store")
    wx.add_argument('--brk_port',
                       dest='brk_port',
                       help="AMQP Broker Port",
                       action="store")
    wx.add_argument('--brk_user',
                       dest='brk_user',
                       help="AMQP Broker User",
                       action="store")
    wx.add_argument('--brk_pass',
                       dest='brk_pass',
                       help="AMQP Broker Password",
                       action="store")

    other = parser.add_argument_group('Other daemon settings')
    other.add_argument('--config',
                       dest='configFilename',
                       help="Daemon configuration file path",
                       action="store")
    other.add_argument('--log_path',
                       dest='log_path',
                       help="Daemon logging path",
                       action="store")
    other.add_argument('--log_level',
                       dest='log_level',
                       help="Daemon logging path",
                       action="store")
    other.add_argument('--log_to_file',
                       dest='log_to_file',
                       help="Logging sent to file (default False)",
                       action="store")
    other.add_argument('--message_yaml_file',
                       dest='message_yaml_file',
                       help="Name of file containing message definitions",
                       action="store")

    args = parser.parse_args()

    if args.configFilename is not None:
        # Add the specified file to the list of configuration file names.  We'll
        # add it at the end so it is the last file loaded, thereby overriding
        # the values read from other files.
        configFileNames.append(args.configFilename)
    #
    ###

    ###
    # Read the configuration values from the set of files...
    config = ConfigParser.ConfigParser()
    config.read(configFileNames)
    #
    ###

    ###
    # Override values read in by those from the command line.
    # DANGER: Using exec is usually not a good idea since random commands could
    # be executed by malicious users.  If the configuration file had a value of,
    # say, 'rm -rf *' then an exec of that value qould clobber the disk.  In this
    # case we're evaling something of the form 'args.<something>' so it can't be
    # a command.
    for attr, value in vars(args).items():
        if value is not None:
            if attr in CMD_PARAM_MAPPING:
                sect, varname = CMD_PARAM_MAPPING[attr]
                config.set(sect, varname, eval('args.{}'.format(attr)))

    #
    ###

    return config


def prettyprint(config):
    """Pretty print the values in the provided ConfigParser object
    """
    for section in config.sections():
        print('{}:'.format(section))
        for n,v in config.items(section):
           print('   {:20s}  {}'.format(n+":",v))
