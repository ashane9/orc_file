require 'java'
# new
require 'bigdecimal'
require 'time'
require 'jars/slf4j-api-1.7.9.jar'
# new
require 'jars/commons-logging-1.2.jar'
require 'jars/commons-configuration-1.10.jar'
require 'jars/slf4j-simple-1.7.20.jar'
require 'jars/hadoop-core-1.2.1.jar'
require 'jars/hive-exec-2.1.1.jar'
require 'orc_schema'
require 'orc_options'
require 'orc_reader_options'
require 'orc_file_writer'
require 'orc_file_reader'

# new
java_import 'org.slf4j.LoggerFactory'
java_import 'org.apache.hadoop.hive.common.type.HiveDecimal'
# new

java_import 'org.apache.hadoop.conf.Configuration'
java_import 'org.apache.hadoop.fs.Path'
java_import 'org.apache.orc.CompressionKind'
java_import 'org.apache.orc.TypeDescription'
java_import 'org.apache.orc.OrcFile'



