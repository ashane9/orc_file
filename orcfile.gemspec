Gem::Specification.new do |spec|
  spec.name = 'orcfile'
  spec.version = '1.0.0'
  spec.authors = ['Andrew Shane']
  spec.email = ['ashane9@gmail.com']
  spec.metadata    = { "source_code_uri" => "https://github.com/ashane9/orc_file" }

  spec.summary = 'Reader/writer of Hive ORC files'
  spec.description = 'This gem allows for the creation and reading of Apache Hive Optimized Row Columnar (ORC) files.'

  spec.require_paths = ['lib']
  spec.files = Dir.glob("lib/**/*.rb") + Dir.glob("lib/jars/*.jar") + ['README.rdoc']

  spec.add_dependency('rspec')
  spec.add_dependency('java')
end