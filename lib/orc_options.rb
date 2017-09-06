require 'orc_schema'

class OrcOptions
  attr_reader :orc_schema, :orc

  def initialize()
    conf = Configuration.new
    @orc_schema = OrcSchema.new
    @orc = OrcFile.writerOptions(conf)
  end

  def define_table_schema(table_schema)
    raise TypeError, 'table_schema must be a Hash of {column_name: data_type}' unless table_schema.is_a? Hash
    raise ArgumentError, 'table_schema cannot be an empty hash' if table_schema.empty?
    table_schema.each do |column_name, data_type|
      @orc_schema.add_column(column_name, data_type)
    end
    @orc.setSchema(@orc_schema.schema)
  end

  def define_stripe_size(stripe_size)
    @orc.stripeSize(stripe_size)
  end

  def define_row_index_stride(row_index_stride)
    @orc.rowIndexStride(row_index_stride)
  end

  def define_buffer_size(buffer_size)
    @orc.bufferSize(buffer_size)
  end

  def define_write_mode(write_mode)
    @orc.mode(write_mode)
  end

  def define_compression(compression)
    begin
      @orc.compress(CompressionKind.valueOf(compression))
    rescue java.lang.IllegalArgumentException
      raise ArgumentError, "#{compression} is not a valid CompressionKind. Must be one of the following: \n#{CompressionKind.constants}"
    end
  end

  def set_options(opts)
    define_stripe_size(opts[:stripe_size]) unless opts[:stripe_size].nil?
    define_row_index_stride(opts[:row_index_stride]) unless opts[:row_index_stride].nil?
    define_buffer_size(opts[:buffer_size]) unless opts[:buffer_size].nil?
    define_compression(opts[:compression]) unless opts[:compression].nil?
    define_write_mode(opts[:mode]) unless opts[:mode].nil?
  end

end