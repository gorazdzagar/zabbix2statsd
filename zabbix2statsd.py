#!/usr/bin/python
# Filename: zabbix2statsd.py

__author__ = "gzagar, OPEX, Smartbox Experience Ltd."
__copyright__ = "Smartbox Experience Ltd."
__credits__ = ["The OPEX team"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "gzagar"
__email__ = "gorazd.zagar@smartandco.com"
__status__ = "Production"

import statsd
import sys
import string
import re
import MySQLdb
import ConfigParser

# Hosts configuration file
cfg_file = './zabbix2statsd.cfg'

# Read ini-style configuration file
try:
   config = ConfigParser.ConfigParser()
   config.readfp(open(cfg_file))
except IOError as e:
   print "I/O error({0}): {1}".format(e.errno, e.strerror)
   sys.exit("Please check the configuration file " + cfg_file + "exists!")
except:
   print "Unexpected error:", sys.exc_info()[0]
   raise
   sys.exit(1)

try:
   # StatsD configuration
   statsd_host = config.get('StatsD','statsd_host')
   statsd_port = config.get('StatsD','statsd_port')
   sample_rate = config.get('StatsD','sample_rate')

   # MySQL configuration
   mysql_host = config.get('Zabbix','mysql_host')
   mysql_user = config.get('Zabbix','mysql_user')
   mysql_pass = config.get('Zabbix','mysql_pass')
   mysql_db   = config.get('Zabbix','mysql_db')
except:
  sys.exit("Error reading the configuration parameters for MySQL and StatsD from " + cfg_file + ". Exiting...")

# Internal config
zabbix_table = ('history','','','history_uint')

# Establish MySQL connection
try:
  db = MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass, db=mysql_db)
except:
  sys.exit("Error establishing the connection to MySQL server (" + mysql_user + "@" + mysql_host + ", db: " + mysql_db + ")!")

# Creating a cursor object for database connection
cur = db.cursor()

# Establishing the connection
try:
   statsd.Connection.set_defaults(host=statsd_host, port=statsd_port, sample_rate=sample_rate, disabled=False)
except:
   sys.exit("Error setting the connection defaults for StatsD server (host: " + statsd_host + ":" + statsd_port + ")!")

# Get a list of hosts
try:
   hosts = config.get('Hosts','hosts_include').split(', ')
except:
   sys.exit("Host list parsing error from configration file!")

for host in hosts:
   # replace the * with the %
   host = re.sub("\*","%",host)
   cur.execute("select items.key_, items.itemid, items.value_type, hosts.host,hosts.name from items, hosts where hosts.name like '" + host + "' and items.hostid = hosts.hostid and items.value_type in (0,3) and items.status = 0 and hosts.status = 0;")
   # Loop through each item
   for row in cur.fetchall() :
     # If the host is a vm autodiscovered by the vmware template we read the visible name instead of the host.host field
      if ( re.match("\w{8}(-\w{4}){3}-\w{12}?", row[3]) ):
       zabbix_host = re.sub("\.","_",row[4])
     #Remove the -vmware suffix from the visible name"
       zabbix_host = re.sub("-vmware$","",zabbix_host);
      else:
       zabbix_host = re.sub("\.","_",row[3])
      zabbix_item = re.sub("\.","-",row[0])
      zabbix_item = re.sub("\:","-",zabbix_item)
      zabbix_item = re.sub("\[\]",".def_param",zabbix_item)
      zabbix_item = re.sub("\[",".",zabbix_item)
      zabbix_item = re.sub("\"","_",zabbix_item)
      zabbix_item = re.sub('^[a-zA-Z0-9_-]+$', zabbix_item + '.default', zabbix_item);
      zabbix_item = re.sub("[^a-zA-Z0-9._-]","",zabbix_item)

      cur.execute("select " + zabbix_table[row[2]] + ".value from " + zabbix_table[row[2]] + " where " + zabbix_table[row[2]] + ".itemid = " + str(row[1]) + " and " + zabbix_table[row[2]] + ".clock = (select max(" + zabbix_table[row[2]] + ".clock) from " + zabbix_table[row[2]] + " where " + zabbix_table[row[2]] + ".itemid = " + str(row[1]) + ")")

      for item_value in cur.fetchall() :
         gauge = statsd.Gauge('zabbix')
         gauge.send(zabbix_host + '.' + zabbix_item, item_value[0])
