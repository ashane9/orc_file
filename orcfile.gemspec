Gem::Specification.new do |spec|
  spec.name = 'orcfile'
  spec.version = '0.0.1'
  spec.authors = ['Andrew Shane']
  spec.email = ['andrew.shane@nationwide.com']

  spec.summary = 'Reader/writer of Hive ORC files'
  spec.description = 'This gem allows for the creation and reading of Apache Hive Optimized Row Columnar (ORC) files.'

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "http://repo.nwie.net/nexus/content/repositories/gems-internal/"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.require_paths = ['./lib']
  spec.files = Dir.glob("./lib/**/*.rb") + ['README.md']

  spec.add_dependency('rspec')
  spec.add_dependency('java')
end