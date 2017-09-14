class OrcFileReader
  attr_reader :reader, :orc_options, :table_schema

  def initialize(table_schema, path='orc_file.orc')
    @orc_options = OrcReaderOptions.new
    @table_schema = table_schema
    path = Path.new(path)
    @reader = OrcFile.createReader(path, @orc_options.orc)
  end

  def read_row(row_batch, row_index)
    orc_row = {}
    row_batch.cols.each_with_index do |column, index|
      column_name = @table_schema.keys[index]
      data_type = @table_schema[column_name]
      case data_type
        when :integer
          orc_row[column_name] = column.vector[row_index]
        when :decimal
          orc_row[column_name] = column.vector[row_index].get_hive_decimal.to_s.to_d
        when :float
          #sets float value as 0.0005000000237487257 instead of 0.0005
          orc_row[column_name] = column.vector[row_index]
        when :datetime
          orc_row[column_name] = DateTime.strptime(column.time[row_index].to_s, '%Q').to_time.to_datetime
        when :time
          orc_row[column_name] = Time.strptime(column.time[row_index].to_s, '%Q')
        when :date
          # orc_row[column_name] = Time.at(column.vector.first * 86400).to_date
          orc_row[column_name] = Date.new(1970,1,1) + column.vector[row_index]
        when :string
          orc_row[column_name] = column.toString(row_index)
      end
    end
    orc_row
  end

  def read_from_orc
    rows = Array.new
    row_batch = @reader.get_schema.createRowBatch()
    @reader.rows.next_batch(row_batch)

    @reader.number_of_rows.times do |row_index|
      rows << read_row(row_batch, row_index)
    end
    rows
  end

end