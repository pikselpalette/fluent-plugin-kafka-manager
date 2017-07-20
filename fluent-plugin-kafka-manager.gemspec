# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ["Stephen Gran"]
  gem.email         = ["stephen.gran@piksel.com"]
  gem.description   = %q{Fluentd plugin for Apache Kafka-manager > 0.8}
  gem.summary       = %q{Fluentd plugin for Apache Kafka-manager > 0.8}
  gem.homepage      = "https://github.com/fluent/fluent-plugin-kafka-manager"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "fluent-plugin-kafka-manager"
  gem.require_paths = ["lib"]
  gem.version       = '0.0.2'
  gem.required_ruby_version = ">= 2.1.0"

  gem.add_dependency "fluentd"
  gem.add_dependency "faraday"
  gem.add_dependency "fluent-plugin-kafka", ">= 0.3.1"
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "test-unit", ">= 3.0.8"
end
