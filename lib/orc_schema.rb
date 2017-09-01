class OrcSchema
  attr_reader :schema
  def initialize
    @schema = TypeDescription.createStruct()
  end

  def add_column(column_name, data_type)
    case data_type.downcase.to_sym
      when :integer
        type = TypeDescription.createLong()
      when :datetime, :time
        type = TypeDescription.createTimestamp()
      when :date
        type = TypeDescription.createDate()
      when :decimal
        type = TypeDescription.createDecimal()
      when :float
        type = TypeDescription.createFloat()
      when :double
        type = TypeDescription.createDouble()
      when :string
        type = TypeDescription.createString()
      else
        raise ArgumentError, "column data type #{data_type} not defined"
    end
    @schema.addField(column_name.to_s, type)
  end
end