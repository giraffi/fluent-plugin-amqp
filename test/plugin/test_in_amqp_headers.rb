require_relative '../helper'
require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'fluent/plugin/in_amqp'
require 'bunny-mock'

class AMPQInputTestForHeaders < Test::Unit::TestCase
  include Fluent::Test::Helpers

  CONFIG = %(
    type amqp
    format json
    host localhost
    port 5672
    vhost /
    user guest
    pass guest
    queue test_in_fanout
  )

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::AMQPInput).configure(conf)
  end

  def get_plugin(configuration = CONFIG)
    omit('BunnyMock is not avaliable') unless Object.const_defined?('BunnyMock')
    @driver = create_driver(configuration)
    plugin = @driver.instance
    plugin.connection = BunnyMock.new
    # Start the driver and wait while it initialises the threads etc
    plugin.start
    5.times { sleep 0.05 }
    return plugin
  end

  sub_test_case 'headers' do
    test 'test_omit_headers' do
      plugin = get_plugin
      # Should have created the 'test_in_fanout' queue
      assert_equal true, plugin.connection.queue_exists?('test_in_fanout')
      # bind a testing queue to the exchange
      queue = plugin.connection.channel.queue('test_in_fanout')
      hash = { 'foo' => 'bar' }
      expect_hash = hash.dup
      @driver.run(expect_emits: 1) do
        queue.publish(hash.to_json, headers: { 'GroupId' => 'H123' })
      end
      @driver.events.each do |event|
        assert_equal expect_hash, event[2]
      end
    end

    test 'test_emit_headers' do
      conf = CONFIG.clone
      conf << "\ninclude_headers true\n"
      plugin = get_plugin(conf)
      # Should have created the 'test_in_fanout' queue
      assert_equal true, plugin.connection.queue_exists?('test_in_fanout')
      # bind a testing queue to the exchange
      queue = plugin.connection.channel.queue('test_in_fanout')
      hash = { 'foo' => 'bar' }
      headers = { 'GroupId' => 'H123' }
      expect_hash = hash.dup
      expect_hash['headers'] = headers
      # Emit an event through the plugins driver
      @driver.run(expect_emits: 1) do
        queue.publish(hash.to_json, headers: headers)
      end
      @driver.events.each do |event|
        assert_equal expect_hash, event[2]
      end
    end
  end
end
