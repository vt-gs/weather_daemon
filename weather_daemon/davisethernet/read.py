import socket
import struct
import select
import time


def _read_data(sock, command='LOOP 1'):
    """Issue a 'read' command to the Davis device and read back the
    returned data.  No parsing is done here rather the raw bytes are
    returned.
    """
    buffer = None
    if sock is not None:
        # Clear out any stale data on the socket.  Sometimes the Davis
        # or serial to ethernet bridge devices leave characters on the
        # socket (usually \n\r).  Strip them so we're sure we have only
        # the data we're requesting and that its byte structure is right.
        input = [sock]
        while 1:
            inputready, o, e = select.select(input,[],[], 0.0)
            if len(inputready)==0: break
            for s in inputready: s.recv(1)

        # Sent the specified command and read back all the returned data.
        buffer = ''
        sock.send('{}\n'.format(command))
        done = False
        # Each read may get only part of the data so loop until there's
        # nothing left.  We'll actually use the timeout associated with
        # the socket to throw an exception to ensure there's nothing left.
        while not done:
            try:
                buffer += sock.recv(1024)
            except Exception, err:
                done = True

    return buffer


def LOOP(sock):
    """Reads "LOOP" data from the connected Davis device.  It
    is assumed that the sock parameter is an open connection to
    the ethernet-based weather station.  The LOOP data format
    and content can be found in the "Vantage Pro2 Serial
    Communication Reference Manual" (page 21).  The returned data
    is passed in a dictionary.
    """
    data = {}
    if sock is not None:
        try:
            buffer = _read_data(sock, 'LOOP 1')

            check          = struct.unpack('<3s',buffer[1:4])[0] # should be "LOO"
            data['bar_trend']      = struct.unpack('<b',buffer[4])[0]
            buffer_type    = struct.unpack('<B',buffer[5])[0]
            next_record    = struct.unpack('<H',buffer[6:8])[0]
            data['barometer']      = float( struct.unpack('<H',buffer[8:10])[0] )/1000.0
            data['inside_temp']    = float( struct.unpack('<H',buffer[10:12])[0] )/10.0
            data['inside_humid']   = float( struct.unpack('<B',buffer[12])[0] )
            data['outside_temp']   = float( struct.unpack('<H',buffer[13:15])[0] )/10.0
            data['wind_speed']     = float( struct.unpack('<B',buffer[15])[0] )
            data['avg_wind_speed'] = float( struct.unpack('<B',buffer[16])[0] )
            data['wind_dir']       = float( struct.unpack('<H',buffer[17:19])[0] )
            data['extra_temps']    = struct.unpack('<7B',buffer[19:26])
            data['soil_temps']     = struct.unpack('<4B',buffer[26:30])
            data['leaf_temps']     = struct.unpack('<4B',buffer[30:34])
            data['outside_humid']  = float( struct.unpack('<B',buffer[34])[0] )
            data['extra_humid']    = struct.unpack('<7B',buffer[35:42])
            data['rain_rate']      = float( struct.unpack('<H',buffer[42:44])[0] ) * 0.01
            data['uv']             = float( struct.unpack('<B',buffer[44])[0] )
            data['solar_rad']      = float( struct.unpack('<H',buffer[45:47])[0] )
            data['storm_rain']     = float( struct.unpack('<H',buffer[47:49])[0] )/100.0
            # Bit 15 to bit 12 is the month,
            # bit 11 to bit 7 is the day,
            # bit 6 to bit 0 is the year offseted by 2000
            ss = struct.unpack('<H',buffer[49:51])[0]
            if ss == 65535  or  ss == 0:
                data['storm_start'] = None
            else:
                mn = (ss & 0b1111000000000000) >> 12
                dy = (ss & 0b0000111110000000) >> 7
                yr = (ss & 0b0000000001111111) + 2000
                data['storm_start'] = '{}-{}-{}'.format(yr, mn, dy)
            data['day_rain']       = float( struct.unpack('<H',buffer[51:53])[0] )*0.01 # inches
            data['month_rain']     = float( struct.unpack('<H',buffer[53:55])[0] )*0.01 # inches
            data['year_rain']      = float( struct.unpack('<H',buffer[55:57])[0] )*0.01 # inches
            data['day_ET']         = float( struct.unpack('<H',buffer[57:59])[0] )/1000.0
            data['month_ET']       = float( struct.unpack('<H',buffer[59:61])[0] )/100.0
            data['year_ET']        = float( struct.unpack('<H',buffer[61:63])[0] )/100.0
            data['soil_moistures'] = struct.unpack('<4B',buffer[63:67])
            data['leaf_wetness']   = struct.unpack('<4B',buffer[67:71])
            data['inside_alarms']  = struct.unpack('<B',buffer[71])[0]
            data['rain_alarms']    = struct.unpack('<B',buffer[72])[0]
            data['outside_alarms'] = struct.unpack('<H',buffer[73:75])[0]
            data['extra_alarms']   = struct.unpack('<8B',buffer[75:83])
            data['soil_alarms']    = struct.unpack('<4B',buffer[83:87])
            data['tx_batt_stat']   = struct.unpack('<B',buffer[87])[0]
            data['con_batt_volt']  = ((300*float( struct.unpack('<H',buffer[88:90])[0] ))/512)/100
            data['forecast_icons'] = struct.unpack('<B',buffer[90])[0]
            data['forecast_rule']  = struct.unpack('<B',buffer[91])[0]
            # Sunrise & sunset stored as hour * 100 + min.
            sr = struct.unpack('<H',buffer[92:94])[0]
            hr = int(sr/100)
            mn = int(sr-hr*100)
            data['sunrise']        = '{}:{}'.format(hr,mn)
            sr = struct.unpack('<H',buffer[94:96])[0]
            hr = int(sr/100)
            mn = int(sr-hr*100)
            data['sunset']         = '{}:{}'.format(hr,mn)

        except Exception, err:
            data = {}

    return data


def LOOP2(sock):
    """Reads "LOOP2" data from the connected Davis device.  It
    is assumed that the sock parameter is an open connection to
    the ethernet-based weather station.  The LOOP2 data format
    and content can be found in the "Vantage Pro2 Serial
    Communication Reference Manual" (page 25).  The returned data
    is passed in a dictionary.
    """
    data = {}
    if sock is not None:
        try:
            buffer = _read_data(sock, 'LPS 2 1')

            check                    = struct.unpack('<3s',buffer[1:4])[0] # should be "LOO"
            data['bar_trend']        = struct.unpack('<b',buffer[4])[0]
            buffer_type              = struct.unpack('<B',buffer[5])[0]
            data['barometer']        = float( struct.unpack('<H',buffer[8:10])[0] )/1000.0
            data['inside_temp']      = float( struct.unpack('<H',buffer[10:12])[0] )/10.0
            data['inside_humid']     = float( struct.unpack('<B',buffer[12])[0] )
            data['outside_temp']     = float( struct.unpack('<H',buffer[13:15])[0] )/10.0
            data['wind_speed']       = float( struct.unpack('<B',buffer[15])[0] )
            data['wind_dir']         = float( struct.unpack('<H',buffer[17:19])[0] )
            data['wind_spd_avg_10']  = float( struct.unpack('<H',buffer[19:21])[0] )/10.0
            data['wind_spd_avg_2']   = float( struct.unpack('<H',buffer[21:23])[0] )/10.0
            # Should this be divided by 10?  It seems to report incorrectly if
            # we do that even though the data sheet indicates we should.
            data['wind_gust_10']     = float( struct.unpack('<H',buffer[23:25])[0] )
            data['wind_gust_10_dir'] = float( struct.unpack('<H',buffer[25:27])[0] )
            data['dew_point']        = float( struct.unpack('<h',buffer[31:33])[0] )
            data['outside_humid']    = float( struct.unpack('<B',buffer[34])[0] )
            data['heat_index']       = float( struct.unpack('<h',buffer[36:38])[0] )
            data['wind_chill']       = float( struct.unpack('<h',buffer[38:40])[0] )
            data['THSW_index']       = float( struct.unpack('<h',buffer[40:42])[0] )
            data['rain_rate']        = float( struct.unpack('<H',buffer[42:44])[0] ) * 0.01
            data['uv']               = float( struct.unpack('<B',buffer[44])[0] )
            data['solar_rad']        = float( struct.unpack('<H',buffer[45:47])[0] )
            data['storm_rain']       = float( struct.unpack('<H',buffer[47:49])[0] )/100.0
            # Bit 15 to bit 12 is the month,
            # bit 11 to bit 7 is the day,
            # bit 6 to bit 0 is the year offseted by 2000
            ss = struct.unpack('<H',buffer[49:51])[0]
            if ss == 65535  or  ss == 0:
                data['storm_start'] = None
            else:
                mn = (ss & 0b1111000000000000) >> 12
                dy = (ss & 0b0000111110000000) >> 7
                yr = (ss & 0b0000000001111111) + 2000
                data['storm_start']    = '{}-{}-{}'.format(yr, mn, dy)
            data['rain_day']          = float( struct.unpack('<H',buffer[51:53])[0] )*0.01 # inches
            data['rain_last_15']      = float( struct.unpack('<H',buffer[53:55])[0] )*0.01
            data['rain_last_hr']      = float( struct.unpack('<H',buffer[55:57])[0] )*0.01
            data['day_ET']            = float( struct.unpack('<H',buffer[57:59])[0] )/1000.0
            data['rain_last_24']      = float( struct.unpack('<H',buffer[59:61])[0] )*0.01

        except Exception, err:
            data = {}

    return data


def HILOWS(sock):
    """Reads "HILOWS" data from the connected Davis device.  It
    is assumed that the sock parameter is an open connection to
    the ethernet-based weather station.  The HILOW data format
    and content can be found in the "Vantage Pro2 Serial
    Communication Reference Manual" (page 27).  The returned data
    is passed in a dictionary.
    """
    data = {}
    if sock is not None:
        try:
            buffer = _read_data(sock, 'HILOWS')

            # Barometer section
            ack = struct.unpack('<B',buffer[0])[0]
            data['day_low_bar']  = float(struct.unpack('<H',buffer[1:3])[0])/1000.0
            data['day_high_bar'] = float(struct.unpack('<H',buffer[3:5])[0])/1000.0
            data['month_low_bar']  = float(struct.unpack('<H',buffer[5:7])[0])/1000.0
            data['month_high_bar'] = float(struct.unpack('<H',buffer[7:9])[0])/1000.0
            data['year_low_bar']   = float(struct.unpack('<H',buffer[9:11])[0])/1000.0
            data['year_high_bar']  = float(struct.unpack('<H',buffer[11:13])[0])/1000.0
            # Times stored as hour * 100 + min.
            t   = struct.unpack('<H',buffer[13:15])[0]
            if t == 65535:
                data['time_low_bar'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_low_bar'] = '{}:{}'.format(hr,mn)
            t   = struct.unpack('<H',buffer[15:17])[0]
            if t == 65535:
                data['time_high_bar'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_bar'] = '{}:{}'.format(hr,mn)

            # Wind speed section
            data['day_high_wind']  = float(struct.unpack('<B',buffer[17])[0])
            t   = struct.unpack('<H',buffer[18:20])[0]
            if t == 65535:
                data['time_high_wind'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_wind'] = '{}:{}'.format(hr,mn)
            data['month_high_wind'] = float(struct.unpack('<B',buffer[20])[0])
            data['year_high_wind'] = float(struct.unpack('<B',buffer[21])[0])

            # Inside temperature section
            data['day_high_in_temp']  = float(struct.unpack('<H',buffer[22:24])[0])/10.0
            data['day_low_in_temp']  = float(struct.unpack('<H',buffer[24:26])[0])/10.0
            t   = struct.unpack('<H',buffer[26:28])[0]
            if t == 65535:
                data['time_high_in_temp'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_in_temp'] = '{}:{}'.format(hr,mn)
            t   = struct.unpack('<H',buffer[28:30])[0]
            if t == 65535:
                data['time_low_in_temp'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_low_in_temp'] = '{}:{}'.format(hr,mn)
            data['month_low_in_temp'] = float(struct.unpack('<h',buffer[30:32])[0])/10.0
            data['month_high_in_temp'] = float(struct.unpack('<h',buffer[32:34])[0])/10.0
            data['year_low_in_temp'] = float(struct.unpack('<h',buffer[34:36])[0])/10.0
            data['year_high_in_temp'] = float(struct.unpack('<h',buffer[36:38])[0])/10.0

            # Inside humidity section
            data['day_high_in_hum'] = float(struct.unpack('<B',buffer[38])[0])
            data['day_low_in_hum']  = float(struct.unpack('<B',buffer[39])[0])
            t   = struct.unpack('<H',buffer[40:42])[0]
            if t == 65535:
                data['time_high_in_hum'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_in_hum'] = '{}:{}'.format(hr,mn)
            t   = struct.unpack('<H',buffer[42:44])[0]
            if t == 65535:
                data['time_low_in_hum'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_low_in_hum'] = '{}:{}'.format(hr,mn)
            data['month_high_in_hum'] = float(struct.unpack('<B',buffer[44])[0])
            data['month_low_in_hum'] = float(struct.unpack('<B',buffer[45])[0])
            data['year_high_in_hum'] = float(struct.unpack('<B',buffer[46])[0])
            data['year_low_in_hum'] = float(struct.unpack('<B',buffer[47])[0])

            # Outside temperature section
            data['day_low_out_temp']  = float(struct.unpack('<H',buffer[48:50])[0])/10.0
            data['day_high_out_temp']  = float(struct.unpack('<H',buffer[50:52])[0])/10.0
            t   = struct.unpack('<H',buffer[52:54])[0]
            if t == 65535:
                data['time_low_out_temp'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_low_out_temp'] = '{}:{}'.format(hr,mn)
            t   = struct.unpack('<H',buffer[54:56])[0]
            if t == 65535:
                data['time_high_out_temp'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_out_temp'] = '{}:{}'.format(hr,mn)
            data['month_high_out_temp'] = float(struct.unpack('<h',buffer[56:58])[0])/10.0
            data['month_low_out_temp'] = float(struct.unpack('<h',buffer[58:60])[0])/10.0
            data['year_high_out_temp'] = float(struct.unpack('<h',buffer[60:62])[0])/10.0
            data['year_low_out_temp'] = float(struct.unpack('<h',buffer[62:64])[0])/10.0

            # Dew point section
            data['day_low_dew']  = float(struct.unpack('<H',buffer[64:66])[0])
            data['day_high_dew']  = float(struct.unpack('<H',buffer[66:68])[0])
            t   = struct.unpack('<H',buffer[68:70])[0]
            if t == 65535:
                data['time_low_dew'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_low_dew'] = '{}:{}'.format(hr,mn)
            t   = struct.unpack('<H',buffer[70:72])[0]
            if t == 65535:
                data['time_high_dew'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_dew'] = '{}:{}'.format(hr,mn)
            data['month_high_dew'] = float(struct.unpack('<h',buffer[72:74])[0])
            data['month_low_dew'] = float(struct.unpack('<h',buffer[74:76])[0])
            data['year_high_dew'] = float(struct.unpack('<h',buffer[76:78])[0])
            data['year_low_dew'] = float(struct.unpack('<h',buffer[78:80])[0])

            # Wind chill section
            data['day_low_wchill']  = float(struct.unpack('<h',buffer[80:82])[0])
            t   = struct.unpack('<H',buffer[82:84])[0]
            if t == 65535:
                data['time_low_wchill'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_low_wchill'] = '{}:{}'.format(hr,mn)
            data['month_low_wchill']  = float(struct.unpack('<h',buffer[84:86])[0])
            data['year_low_wchill']  = float(struct.unpack('<h',buffer[86:88])[0])

            # Heat index section
            data['day_high_heat']  = float(struct.unpack('<h',buffer[88:90])[0])
            t   = struct.unpack('<H',buffer[90:92])[0]
            if t == 65535:
                data['time_high_heat'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_heat'] = '{}:{}'.format(hr,mn)
            data['month_high_heat']  = float(struct.unpack('<h',buffer[92:94])[0])
            data['year_high_heat']  = float(struct.unpack('<h',buffer[94:96])[0])

            # THSW section
            data['day_high_THSW']  = float(struct.unpack('<h',buffer[96:98])[0])
            t   = struct.unpack('<H',buffer[98:100])[0]
            if t == 65535:
                data['time_high_THSW'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_THSW'] = '{}:{}'.format(hr,mn)
            data['month_high_THSW']  = float(struct.unpack('<h',buffer[100:102])[0])
            data['year_high_THSW']  = float(struct.unpack('<h',buffer[102:104])[0])

            # Solar radiation section
            data['day_high_sol_rad']  = float(struct.unpack('<H',buffer[104:106])[0])
            t   = struct.unpack('<H',buffer[106:108])[0]
            if t == 65535:
                data['time_high_sol_rad'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_sol_rad'] = '{}:{}'.format(hr,mn)
            data['month_high_sol_rad']  = float(struct.unpack('<H',buffer[108:110])[0])
            data['year_high_sol_rad']  = float(struct.unpack('<H',buffer[110:112])[0])

            # Rain rate section
            data['day_high_rain_rate']  = float(struct.unpack('<H',buffer[117:119])[0])*0.01
            t   = struct.unpack('<H',buffer[119:121])[0]
            if t == 65535:
                data['time_high_rain_rate'] = None
            else:
                hr = int(t/100)
                mn = int(t-hr*100)
                data['time_high_rain_rate'] = '{}:{}'.format(hr,mn)
            data['hour_high_rain_rate']  = float(struct.unpack('<H',buffer[121:123])[0])*0.01
            data['month_high_rain_rate']  = float(struct.unpack('<H',buffer[123:125])[0])*0.01
            data['year_high_rain_rate']  = float(struct.unpack('<H',buffer[125:127])[0])*0.01

        except Exception, err:
            data = {}

        return data
