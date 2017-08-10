require 'java'
# new
require 'bigdecimal'
require 'time'
require './lib/jars/slf4j-api-1.7.9.jar'
# new
require './lib/jars/commons-logging-1.2.jar'
require './lib/jars/commons-configuration-1.10.jar'
require './lib/jars/slf4j-simple-1.7.20.jar'
require './lib/jars/hadoop-core-1.2.1.jar'
require './lib/jars/hive-exec-2.1.1.jar'
require './lib/orc_schema'
require './lib/orc_options'
require './lib/orc_reader_options'
require './lib/orc_file_writer'
require './lib/orc_file_reader'

# new
java_import 'org.slf4j.LoggerFactory'
java_import 'org.apache.hadoop.hive.common.type.HiveDecimal'
# new

java_import 'org.apache.hadoop.conf.Configuration'
java_import 'org.apache.hadoop.fs.Path'
java_import 'org.apache.orc.CompressionKind'
java_import 'org.apache.orc.TypeDescription'
java_import 'org.apache.orc.OrcFile'



