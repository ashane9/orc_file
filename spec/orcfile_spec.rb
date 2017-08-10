require './lib/orcfile'
require 'date'

java_import 'org.apache.orc.impl.WriterImpl'
java_import 'org.apache.orc.impl.ReaderImpl'
java_import 'org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch'
java_import 'org.apache.hadoop.hive.ql.exec.vector.LongColumnVector'
java_import 'org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector'
java_import 'org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector'
java_import 'org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector'
java_import 'org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector'

describe OrcFile do
  before(:all) do
    @table_schema ={:column1 => :integer, :column2 => :datetime, :column3 => :time, :column4 => :date,
                    :column5 => :decimal, :column6 => :float, :column7 => :string}

    @data_set ={:column1 => 1, :column2 => DateTime.now, :column3 => Time.now, :column4 => Date.today,
                :column5 => 1000.01.to_d, :column6 => 0.0005, :column7 => 'the string column'}

    @orc_file_path = 'spec/orc_file.orc'
  end
  # let(:table_schema) {{:column1 => :integer, :column2 => :datetime, :column3 => :time, :column4 => :date,
  #                      :column5 => :decimal, :column6 => :float, :column7 => :double, :column8 => :string}}
  # let(:data_set) {{:column1 => 1, :column2 => DateTime.now, :column3 => Time.now, :column4 => Date.today,
  #                  :column5 => 1000.01, :column6 => 0.0001, :column7 => 1000.0001, :column8 => 'the string column'}}
  # let(:orc_file_path) {'spec/orc_file.orc'}

  context 'OrcSchema' do
    let(:orc_schema) {OrcSchema.new}
    context 'initialize' do
      it 'will initialize instance variable schema as a Type Description object' do
        expect(orc_schema.schema).to be_a_kind_of(TypeDescription)
      end

      it 'will define the Type Description Category as a STRUCT' do
        expect(orc_schema.schema.category).to eq(TypeDescription::Category.const_get :STRUCT)
      end
    end

    context 'add_column' do
      context('integer') do
        before do
          orc_schema.add_column('int_column', :integer)
        end
        it 'will add a Type Description LONG to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :LONG)
        end
      end
      context('datetime') do
        before do
          orc_schema.add_column('datetime_column', :datetime)
        end
        it 'will add a Type Description TIMESTAMP to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :TIMESTAMP)
        end
      end
      context('time') do
        before do
          orc_schema.add_column('time_column', :datetime)
        end
        it 'will add a Type Description TIMESTAMP to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :TIMESTAMP)
        end
      end
      context('date') do
        before do
          orc_schema.add_column('date_column', :date)
        end
        it 'will add a Type Description DATE to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :DATE)
        end
      end
      context('float') do
        before do
          orc_schema.add_column('float_column', :float)
        end
        it 'will add a Type Description FLOAT to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :FLOAT)
        end
      end
      context('double') do
        before do
          orc_schema.add_column('double_column', :double)
        end
        it 'will add a Type Description DOUBLE to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :DOUBLE)
        end
      end
      context('string') do
        before do
          orc_schema.add_column('datetime_column', :string)
        end
        it 'will add a Type Description STRING to the orc_schema struct' do
          expect(orc_schema.schema.children.first.category).to eq(TypeDescription::Category.const_get :STRING)
        end
      end      
      context('invalid data type') do
        it 'will throw an ArgumentError for an invalid data type' do
          data_type = :invalid
          expect{orc_schema.add_column('datetime_column', data_type)}.
              to raise_error(ArgumentError, "column data type #{data_type} not defined")
        end
      end
    end
  end

  context 'OrcOptions' do
    # let(:orc_options) {OrcOptions.new}
    before(:each) do
      @orc_options = OrcOptions.new
    end
    context 'initialize' do
      it 'will initialize instance variable orc_schema as an OrcSchema object' do
        expect(@orc_options.orc_schema).to be_a_kind_of(OrcSchema)
      end

      it 'will initialize instance variable orc as an OrcFile::WriterOptions object' do
        expect(@orc_options.orc).to be_a_kind_of(OrcFile::WriterOptions)
      end
    end

    context 'define_table_schema' do
      before(:each) do
        @orc_options.define_table_schema(@table_schema)
      end

      it 'will raise a TypeError when table_schema is not a hash' do
        expect{@orc_options.define_table_schema('not_a_hash')}.
            to raise_error(TypeError, 'table_schema must be a Hash of {column_name: data_type}')
      end

      it 'will raise an ArgumentError when table_schema hash is empty' do
        empty_hash = Hash.new
        expect{@orc_options.define_table_schema(empty_hash)}.
            to raise_error(ArgumentError, 'table_schema cannot be an empty hash')
      end

      it 'will set the schema for the orc instance object based on the table schema sent to the function' do
        expect(@orc_options.orc.schema.children.size).to eq @table_schema.size
      end
    end

    context 'define_stripe_size' do
      before(:each) do
        @orc_options.define_stripe_size(100)
      end

      it 'will set orc stripe size as 100' do
        expect(@orc_options.orc.get_stripe_size).to eq 100
      end
    end

    context 'define_row_index_stride' do
      before(:each) do
        @orc_options.define_row_index_stride(500)
      end

      it 'will set orc row index stride as 500' do
        expect(@orc_options.orc.get_row_index_stride).to eq 500
      end
    end

    context 'define_buffer_size' do
      before do
        @orc_options.define_buffer_size(10000)
      end

      it 'will set orc buffer size as 10000' do
        expect(@orc_options.orc.get_buffer_size).to eq 10000
      end
    end

    context 'define_compression' do
      context 'NONE' do
        before do
          @orc_options.define_compression('NONE')
        end

        it 'will set orc compression as NONE' do
          expect(@orc_options.orc.get_compress).to eq(CompressionKind.const_get :NONE)
        end
      end
      context 'ZLIB' do
        before do
          @orc_options.define_compression('ZLIB')
        end

        it 'will set orc compression as ZLIB' do
          expect(@orc_options.orc.get_compress).to eq(CompressionKind.const_get :ZLIB)
        end
      end
      context 'SNAPPY' do
        before do
          @orc_options.define_compression('SNAPPY')
        end

        it 'will set orc compression as SNAPPY' do
          expect(@orc_options.orc.get_compress).to eq(CompressionKind.const_get :SNAPPY)
        end
      end
      context 'LZO' do
        before do
          @orc_options.define_compression('LZO')
        end

        it 'will set orc compression as LZO' do
          expect(@orc_options.orc.get_compress).to eq(CompressionKind.const_get :LZO)
        end
      end
      context 'Invalid compression type' do
        it 'will throw an ArgumentError for an invalid compression type' do
          compression_type = 'WRONG'
          expect{@orc_options.define_compression(compression_type)}.
              to raise_error(ArgumentError,
                             "#{compression_type} is not a valid CompressionKind. Must be one of the following: \n#{CompressionKind.constants}")
        end
      end
    end

    context 'set_options' do
      pending 'need to rethink this method'
    end
  end

  context 'OrcReaderOptions' do
    let(:orc_reader_options) {OrcReaderOptions.new}
    context 'initialize' do
      it 'will initialize instance variable orc_schema as an OrcSchema object' do
        expect(orc_reader_options.orc_schema).to be_a_kind_of(OrcSchema)
      end

      it 'will initialize instance variable orc as an OrcFile::ReaderOptions object' do
        expect(orc_reader_options.orc).to be_a_kind_of(OrcFile::ReaderOptions)
      end
    end

  end

  context 'OrcFileWriter' do
    let(:orc_file_writer) {OrcFileWriter.new(@table_schema, @data_set, @orc_file_path)}
    context 'initialize' do
      it 'will initialize instance variable writer as an WriterImpl object' do
        expect(orc_file_writer.writer).to be_a_kind_of(WriterImpl)
      end

      it 'will initialize instance variable orc_options as an OrcOptions object' do
        expect(orc_file_writer.orc_options).to be_a_kind_of(OrcOptions)
      end

      it 'will set the instance variable table_schema as the schema provided' do
        expect(orc_file_writer.table_schema).to eq @table_schema
      end

      it 'will set the instance variable data_set as the data provided' do
        expect(orc_file_writer.data_set).to eq @data_set
      end
    end

    context 'create_row' do
      let(:orc_row) {orc_file_writer.create_row(@data_set)}

      it 'will create a vectorized row batch for storing the data set' do
        expect(orc_row).to be_a_kind_of(VectorizedRowBatch)
      end

      # it 'will convert an Integer to a Java Long and add to the associated row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for an Integer value' do
        expect(orc_row.cols[0]).to be_a_kind_of(LongColumnVector)
        expect(orc_row.cols[0].vector.first).to eq @data_set[:column1]
      end

      # it 'will convert a DateTime to Java Timestamp and add to the row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for a DateTime value in epoch millisecond format' do
        expect(orc_row.cols[1]).to be_a_kind_of(TimestampColumnVector)
        expect(orc_row.cols[1].time.first).to eq @data_set[:column2].strftime('%Q').to_i
      end

      # it 'will convert a Time to Java Timestamp and add to the row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for a Time value in epoch millisecond format' do
        expect(orc_row.cols[2]).to be_a_kind_of(TimestampColumnVector)
        expect(orc_row.cols[2].time.first).to eq @data_set[:column3].strftime('%s%3N').to_i
      end

      # it 'will convert a Date to Java Date and add to the row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for a Date value in epoch day format' do
        expect(orc_row.cols[3]).to be_a_kind_of(LongColumnVector)
        expect(orc_row.cols[3].vector.first).to eq ((@data_set[:column4] - Date.new(1970,1,1)).to_i)
      end

      # it 'will convert a Decimal to HiveDecimal and add to the row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for a Decimal value' do
        expect(orc_row.cols[4]).to be_a_kind_of(DecimalColumnVector)
        expect(orc_row.cols[4].vector.first.get_hive_decimal).to eq HiveDecimal.create(@data_set[:column5].to_d.to_java)
      end

      # it 'will convert a Float to Java Double and add to the row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for a Float value' do
        expect(orc_row.cols[5]).to be_a_kind_of(DoubleColumnVector)
        expect(orc_row.cols[5].vector.first).to eq @data_set[:column6]
      end

      # it 'will convert a Double to Java Double and add to the row batch defined by the schema' do
      # it 'will populate the row batch column defined by the schema for a Double value' do
      #   expect(orc_row.cols[6]).to be_a_kind_of(DoubleColumnVector)
      #   expect(orc_row.cols[6].vector.first).to eq @data_set[:column7]
      # end

      # it 'will convert a String to Java Long and add to the row batch defined by the schema' do
      it 'will populate the row batch column defined by the schema for a String value' do
        expect(orc_row.cols[6]).to be_a_kind_of(BytesColumnVector)
        expect(orc_row.cols[6].vector.first.to_s).to eq @data_set[:column7]
      end
    end

    context 'write_to_orc' do
      before do
        Dir.glob("#{@orc_file_path}*").each {|file| File.delete(file)}
        orc_file_writer.write_to_orc
      end

      it 'will write the single row to an orc file' do
        expect(orc_file_writer.writer.number_of_rows).to eq 1
        expect(File).to exist @orc_file_path
      end
    end
  end

  context 'OrcFileReader' do
    # let(:orc_file_writer) {OrcFileWriter.new(@table_schema, @data_set, @orc_file_path)}
    # let(:orc_file_reader) {OrcFileReader.new(@table_schema, @orc_file_path)}
    # let(:orc_row) do
    #   Dir.glob("#{orc_file_path}*").each {|file| File.delete(file)}
    #   orc_file_writer.write_to_orc
    #   batch = orc_file_reader.reader.get_schema.createRowBatch()
    #   orc_file_reader.reader.rows.next_batch(batch)
    #   orc_file_reader.read_row batch
    # end
    before(:all) do
      orc_file_writer = OrcFileWriter.new(@table_schema, @data_set, @orc_file_path)
      @orc_file_reader = OrcFileReader.new(@table_schema, @orc_file_path)
      Dir.glob("#{@orc_file_path}*").each {|file| File.delete(file)}
      orc_file_writer.write_to_orc
      batch = @orc_file_reader.reader.get_schema.createRowBatch()
      @orc_file_reader.reader.rows.next_batch(batch)
      @orc_row = @orc_file_reader.read_row batch
    end

    context 'initialize' do
      it 'will initialize instance variable writer as an ReaderImpl object' do
        expect(@orc_file_reader.reader).to be_a_kind_of(ReaderImpl)
      end

      it 'will initialize instance variable orc_options as an OrcReaderOptions object' do
        expect(@orc_file_reader.orc_options).to be_a_kind_of(OrcReaderOptions)
      end

      it 'will set the instance variable table_schema as the schema provided' do
        expect(@orc_file_reader.table_schema).to eq @table_schema
      end
    end

    context 'read_row' do

      it 'will return a hash with the key column1 matching the original data_set integer value' do
        expect(@orc_row[:column1]).to be_a_kind_of Integer
        expect(@orc_row[:column1]).to eq @data_set[:column1]
      end

      it 'will return a hash with the key column2 matching the original data_set datetime value' do
        expect(@orc_row[:column2]).to be_a_kind_of DateTime
        expect(@orc_row[:column2].to_s).to eq @data_set[:column2].to_s
      end

      it 'will return a hash with the key column3 matching the original data_set time value' do
        expect(@orc_row[:column3]).to be_a_kind_of Time
        expect(@orc_row[:column3].to_s).to eq @data_set[:column3].to_s
      end

      it 'will return a hash with the key column4 matching the original data_set date value' do
        expect(@orc_row[:column4]).to be_a_kind_of Date
        expect(@orc_row[:column4]).to eq @data_set[:column4]
      end

      it 'will return a hash with the key column5 matching the original data_set decimal value' do
        expect(@orc_row[:column5]).to be_a_kind_of BigDecimal
        expect(@orc_row[:column5]).to eq @data_set[:column5]
      end

      it 'will return a hash with the key column6 matching the original data_set float value' do
        expect(@orc_row[:column6]).to be_a_kind_of Float
        expect(@orc_row[:column6]).to eq @data_set[:column6]
      end

      # it 'will return a hash with the key column7 matching the original data_set double value' do
      #   expect(@orc_row[:column7]).to be_a_kind_of Double
      #   expect(@orc_row[:column7]).to eq @data_set[:column7]
      # end

      it 'will return a hash with the key column8 matching the original data_set string value' do
        expect(@orc_row[:column7]).to be_a_kind_of String
        expect(@orc_row[:column7]).to eq @data_set[:column7]
      end
    end

    # context 'read_from_orc' do
    #   before do
    #     Dir.glob("#{orc_file_path}*").each {|file| File.delete(file)}
    #     orc_file_writer.write_to_orc
    #     orc_file_reader.read_from_orc
    #   end
    #
    #   it 'will read the row from an orc file' do
    #     expect(orc_file_reader.reader.number_of_rows).to eq 1
    #   end
    #
    # end
  end
end