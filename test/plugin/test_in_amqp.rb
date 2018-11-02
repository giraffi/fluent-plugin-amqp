
require_relative '../helper'
require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'fluent/plugin/in_amqp'

class AMPQInputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  CONFIG = %(
    type amqp
    format json
    host amqp.example.com
    port 5672
    vhost /
    user guest
    pass guest
    queue logs
  ).freeze
  
  TLS_CONFIG = CONFIG + %q(
    tls true
    tls_key "/etc/fluent/ssl/client.key.pem"
    tls_cert "/etc/fluent/ssl/client.crt.pem"
    tls_ca_certificates ["/etc/fluent/ssl/server.cacrt.pem", "/another/ca/cert.file"]
    tls_verify_peer true
  ).freeze

  setup do
    Fluent::Test.setup
    @d = Fluent::Test::Driver::Input.new(Fluent::Plugin::AMQPInput)
  end

  sub_test_case 'configuration' do
    test 'basic configuration' do
      configs = {'basic' => CONFIG}
      configs.merge!('tls' => TLS_CONFIG)

      configs.each_pair { |k, v|
        @d = Fluent::Test::Driver::Input.new(Fluent::Plugin::AMQPInput).configure(v)
        assert_equal "amqp.example.com", @d.instance.host
        assert_equal 5672, @d.instance.port
        assert_equal "guest", @d.instance.user
        assert_equal "/", @d.instance.vhost
      }
    end

    test 'are available with multi worker configuration in default' do
      assert @d.instance.multi_workers_ready?
    end

    test 'array of hosts' do
      conf = CONFIG + %q(
        type amqp
        format json
        hosts ["bob","fred"]
        )

      @d = @d.configure(conf)
      assert_equal ["bob", "fred"], @d.instance.hosts
      assert_equal 5672, @d.instance.port
      assert_equal "guest", @d.instance.user
      assert_equal "/", @d.instance.vhost
    end


    test 'non exclusive and multi worker shouldnt change queue name' do
      conf = CONFIG + %(
        type amqp
        format json
        exclusive false
        )

      with_worker_config(workers: 2, worker_id: 0) do
        @d = @d.configure(conf)
        omit("BunnyMock is not avaliable") unless Object.const_defined?("BunnyMock")
        @d.instance.connection = BunnyMock.new
      
        # Start the driver and wait while it initialises the threads etc
        @d.instance.start
        10.times { sleep 0.05 }    
      end
      assert_true @d.instance.connection.queue_exists?("logs")
    end

    test 'exclusive without multiple workers, shouldnt change queue name' do
      conf = CONFIG + %(
        type amqp
        format json
        exclusive true
        )

      @d = @d.configure(conf)
      omit('BunnyMock is not avaliable') unless Object.const_defined?('BunnyMock')
      @d.instance.connection = BunnyMock.new
      # Start the driver and wait while it initialises the threads etc
      @d.instance.start
      10.times { sleep 0.05 }
      assert_true @d.instance.connection.queue_exists?('logs')
    end

    test 'exclusive with multiple workers doesnt update first queue name' do
      conf = CONFIG + %(
        type amqp
        format json
        exclusive true
        )

      with_worker_config(workers: 4, worker_id: 0) do
        @d = @d.configure(conf)
        omit('BunnyMock is not avaliable') unless Object.const_defined?('BunnyMock')
        @d.instance.connection = BunnyMock.new
        # Start the driver and wait while it initialises the threads etc
        @d.instance.start
        10.times { sleep 0.05 }    
      end
      assert_true @d.instance.connection.queue_exists?('logs')
    end

    test 'exclusive with multiple workers changes queue name for workers >=1 ' do
      conf = CONFIG + %(
        type amqp
        format json
        exclusive true
        )

      with_worker_config(workers: 4, worker_id: 3) do
        @d = @d.configure(conf)
        omit('BunnyMock is not avaliable') unless Object.const_defined?('BunnyMock')
        @d.instance.connection = BunnyMock.new
        # Start the driver and wait while it initialises the threads etc
        @d.instance.start
        10.times { sleep 0.05 }    
      end
      assert_true @d.instance.connection.queue_exists?('logs.3')
    end

    test 'invalid tls configuration' do
      assert_raise_message(/'tls_key' and 'tls_cert' must be all specified if tls is enabled./) do
        @d.configure(CONFIG + %(tls true))
      end
    end

    test 'invalid host / queue configuration' do
      assert_raise_message(/'host\(s\)' and 'queue' must be all specified./) do
        @d.configure(%(
          type amqp
          format json
          host bob
          ))
      end
    end
  end

  sub_test_case 'connection handling' do

    test 'queue is created' do
      omit("Need to replace bunny-mock with proper mocking - it cant cope with q.subscribe and sensible replay/verification")
      plugin = @d.configure(CONFIG).instance
      plugin.connection = BunnyMock.new

      # Start the driver and wait while it initialises the threads etc
      plugin.start
      10.times { sleep 0.05 }

      # Should have created the 'logs' queue
      assert_equal true, plugin.connection.queue_exists?('logs')
    end

  end
end
