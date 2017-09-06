require 'orc_options'

class OrcFileWriter
  attr_reader :writer, :orc_options, :data_set, :table_schema, :output_path

  def initialize(table_schema, data_set, path='orc_file.orc', options={})
    @orc_options = OrcOptions.new
    @orc_options.set_options(options)
    @table_schema = table_schema
    @data_set = data_set
    @orc_options.define_table_schema(table_schema)
    path.concat '.orc' unless path.include? '.orc'
    path_object = Path.new(path)
    @writer = OrcFile.createWriter(path_object, @orc_options.orc)
  end

  def create_row(row)
    orc_row = @orc_options.orc_schema.schema.createRowBatch()
    orc_row.size = 1
    row.each_with_index do |(key, value), index|
      data_type = @table_schema[key]
      case data_type
        when :integer
          data_for_column = value.to_java(:long)
        when :decimal
          data_for_column = HiveDecimal.create(value.to_d.to_java)
        when :float
          data_for_column = value.to_java(:double)
        when :datetime, :time
          data_for_column = value.to_time
        when :date
          # hive needs date formated as number of days since epoch (01/01/1970)
          data_for_column = (value - Date.new(1970,1,1)).to_i
        when :string
          data_for_column = value.to_s.bytes.to_a
        else
          raise ArgumentError, "column data type #{data_type} not defined"
      end

      orc_row.cols[index].fill(data_for_column)
    end
    orc_row
  end

  def write_to_orc
    if @data_set.is_a? Array
      @data_set.each do |row|
        @writer.addRowBatch(create_row(row))
      end
    else
      @writer.addRowBatch(create_row(@data_set))
    end
    @writer.close
  end

end
