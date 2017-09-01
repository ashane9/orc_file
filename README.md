# ORCFILE
Ruby Gem for creating and reading Optimized Row Columnar (ORC) files.
This gem can also be paired using the dif_fileio and/or factory_girl.

## Installation

Add this line to your application's Gemfile:

    gem 'orcfile'

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install orcfile

## Usage
###OrcFileWriter
To write a file, you will need to initialize the OrcFileWriter class.
This object needs a table schema, your dataset, and the path to store the file.
    
    OrcFileWriter.new(table_schema, data_set, path) 
####*table_schema*
The table_schema must be a hash containing the column name and datatype as the key-value pair.
      
Valid datatypes are:    
- integer 
- decimal
- float
- date
- datetime
- time
- string  

    
    table_schema = {:id => :integer, :amount => :decimal, :rate => :float}
    
####*data_set*
The data_set must contain a hash with the column name and data value as the key-value pair.

For one row in the dataset:

    data_set = {:id => 1, :amount => 1000.01, :rate => 0.0005}
    
For multiple rows in the dataset:

    dataset = [{:id => 1, :amount => 1000.01, :rate => 0.0005},
               {:id => 2, :amount => 2500.5, :rate => 0.1},
               {:id => 3, :amount => 10.12, :rate => 10.0134}]

####*path*
The path should be the full file path or relative to your working directory. You must also specify the file name.

    path = '/temp/orc_file.orc'
    
    
###write_to_orc
Once you have the OrcFileWriter object initialized you must call write_to_orc to write out the file

      OrcFileWriter.new(table_schema, data_set, path).write_to_orc

