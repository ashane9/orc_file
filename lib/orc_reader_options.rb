class OrcReaderOptions
  attr_reader :orc, :orc_schema

  def initialize()
    conf = Configuration.new
    @orc_schema = OrcSchema.new
    @orc = OrcFile.readerOptions(conf)
  end
end