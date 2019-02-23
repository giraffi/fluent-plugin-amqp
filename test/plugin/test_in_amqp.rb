
require_relative '../helper'
require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_amqp'

class AMPQInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end


  CONFIG = %[
    type amqp
    format json
    host amqp.example.com
    port 5672
    vhost /
    user guest
    pass guest
    queue logs
  ]

  TLS_CONFIG = CONFIG + %[
    tls true
    tls_key "/etc/fluent/ssl/client.key.pem"
    tls_cert "/etc/fluent/ssl/client.crt.pem"
    tls_ca_certificates ["/etc/fluent/ssl/server.cacrt.pem", "/another/ca/cert.file"]
    tls_verify_peer true
  ]

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::AMQPInput).configure(conf)
  end

  sub_test_case 'configuration' do
    test 'basic configuration' do
      configs = {'basic' => CONFIG}
      configs.merge!('tls' => TLS_CONFIG)

      configs.each_pair { |k, v|
        d = create_driver(v)
        assert_equal "amqp.example.com", d.instance.host
        assert_equal 5672, d.instance.port
        assert_equal "guest", d.instance.user
        assert_equal "/", d.instance.vhost
      }
    end

    test 'array of hosts' do
      conf = CONFIG + %[
        type amqp
        format json
        hosts ["bob","fred"]
        ]

      d = create_driver(conf)
      assert_equal ["bob", "fred"], d.instance.hosts
      assert_equal 5672, d.instance.port
      assert_equal "guest", d.instance.user
      assert_equal "/", d.instance.vhost
    end

    test 'invalid host / queue configuration' do
      assert_raise_message(/'host\(s\)' and 'queue' must be all specified./) do
        create_driver(%[
          type amqp
          format json
          host bob
          ])
      end
    end
  end
  sub_test_case 'connection handling' do

    test 'queue is created' do
      omit("Need to replace bunny-mock with proper mocking - it cant cope with q.subscribe and sensible replay/verification")
      plugin = create_driver(CONFIG).instance
      plugin.connection = BunnyMock.new

      # Start the driver and wait while it initialises the threads etc
      plugin.start
      10.times { sleep 0.05 }

      # Should have created the 'logs' queue
      assert_equal true, plugin.connection.queue_exists?('logs')
    end

  end
end
