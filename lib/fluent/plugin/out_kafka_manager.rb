require 'faraday'
require 'json'

require 'fluent/output'
require 'fluent/plugin/out_kafka_buffered'

class Fluent::ManagerKafkaOutput < Fluent::KafkaOutputBuffered
  Fluent::Plugin.register_output('kafka_manager', self)

  config_param :broker_discovery,
               :desc => <<-DESC
Discover brokers via kafka-manager API
http://kafka-mesos.example.com:9000/api/brokers/list
http://kafkamanager.example.com/api/status/my-cluster/brokers/extended
DESC

  def enumerate_brokers
    log.info "getting brokers from #{@broker_discovery}"
    response = Faraday.get @broker_discovery
    ret = JSON.load(response.body)
    @seed_brokers = ret['brokers'].each.map { |x| "#{x['host']}:#{x['port']}" }
    @brokers = @seed_brokers.join(',')
  end

  def refresh_client(raise_error = true)
    enumerate_brokers
    super
  end

  def configure(conf)
    super
    enumerate_brokers
    log.info "brokers have been discovered: #{@brokers}"
  end
end
