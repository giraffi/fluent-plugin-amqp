# encoding: utf-8

require_relative '../helper'
require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_amqp'

require 'bunny-mock'

class AMPQOutputTestForHeaders < Test::Unit::TestCase

  attr_reader :driver

  def setup
    Fluent::Test.setup
    BunnyMock.use_bunny_queue_pop_api = true
  end

  def get_plugin(configuration = CONFIG)
    omit("BunnyMock is not avaliable") unless Object.const_defined?("BunnyMock")

    @driver = create_driver(configuration)
    plugin = @driver.instance
    plugin.connection = BunnyMock.new

    # Start the driver and wait while it initialises the threads etc
    plugin.start
    10.times { sleep 0.05 }
    return plugin
  end

  CONFIG = %[
    type amqp
    format json
    host amqp.example.com
    port 5672
    vhost /
    user guest
    pass guest
    exchange my_exchange
    exchange_type fanout
    tag_key true
  ]

  TLS_CONFIG = CONFIG + %[
    tls true
    tls_key "/etc/fluent/ssl/client.key.pem"
    tls_cert "/etc/fluent/ssl/client.crt.pem"
    tls_ca_certificates ["/etc/fluent/ssl/server.cacrt.pem", "/another/ca/cert.file"]
    tls_verify_peer true
  ]

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::AMQPOutput).configure(conf)
  end


  def test_header_message_helper(config, event)
    plugin = get_plugin( config )

    # Should have created the 'logs' queue
    assert_equal true, plugin.connection.exchange_exists?('my_exchange')

    # bind a testing queue to the exchange
    queue = plugin.channel.queue 'my.test.queue'
    queue.bind plugin.exch, routing_key: 'test'
    # queue.test is now bound to the configured exchange

    # v0.14 test driver does not permit to specify String object into #feed args.
    es = Fluent::OneEventStream.new(Time.now.to_i, event)
    # Emit an event through the plugins driver
    @driver.run(default_tag: 'test') do
      @driver.feed(es)
    end

    # Validate the message was delivered
    assert_equal 1, queue.message_count

    queue.pop
  end


  sub_test_case 'message_writing' do

    sub_test_case 'routing_key' do
      test 'Use hardcoded key when tag_key false' do

        #Note that routing keys are igored when using fanout exchange types
        # so the dl object shows 'my.test.queue' as thats what we setup in
        # the helper function - but the message metadata shows the expected key
          config = CONFIG + %[
            tag_key false
            key my.hardcoded.route
            ]

          message = 'This is a simple string, not json'
          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'my.hardcoded.route', meta[:key]

      end
    end

    sub_test_case 'message_headers' do

      test 'Default headers are set even if message isnt json' do

          config = CONFIG + %[
              <header>
                name expect-default-header-value
                source this.doesnt.exist
                default expectMe
              </header>
            ]
          message = 'This is a simple string, not json'
          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'expectMe', headers["expect-default-header-value"]

      end

      test 'Always use default when source is omitted' do

          config =  CONFIG + %[
              <header>
                name unmatched_source_return_default
                default expectMe
              </header>
          ]

          message = 'This is a simple string, not json'

          dl, meta, message = test_header_message_helper(config, message)
          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'expectMe', headers["unmatched_source_return_default"]

      end

      test 'Dont set header if source is missing and default undefined' do

          config =  CONFIG + %[
              <header>
                name dont-set-this
                source missing.from.input
              </header>
          ]

          message = {"aValue" => "Custard"}

          dl, meta, message = test_header_message_helper(config, message)
          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal nil, headers["dont-set-this"], "Did not expect to find 'dont-set-this' header"

      end

      test 'Headers are set when sending json object' do

        config = CONFIG + %[
            <header>
              name unmatched_source_return_default
              source this.doesnt.exist
              default expectMe
            </header>
            <header>
              name matched_key
              source aValue
              default Rhubarb
            </header>
        ]

        message = { "aValue" => "Custard"}

        dl, meta, message = test_header_message_helper(config, message)

        headers = meta[:headers]
        assert_not_nil headers, "Did not find any headers"
        assert_equal 'Custard', headers["matched_key"]
        assert_equal 'expectMe', headers["unmatched_source_return_default"]


      end

      sub_test_case 'nested_headers' do

        test 'Can get headers from nested keys by defining an array in "source"' do

            config = CONFIG + %[
                <header>
                  name nested_header_value
                  source [ "this", "is", "nested" ]
                  default spanishInquisition
                </header>
            ]

            message = { "this" => { "is" => { "nested" => "nobody" }}}

            dl, meta, message = test_header_message_helper(config, message)

            headers = meta[:headers]
            assert_not_nil headers, "Did not find any headers"
            assert_equal 'nobody', headers["nested_header_value"]
        end

        test 'Can get headers from nested keys by defining a dot separated string in "source"' do

          config = CONFIG + %[
              <header>
                name nested_header_value
                source this.is.nested
                default spanishInquisition
              </header>
          ]

          message = { "this" => { "is" => { "nested" => "nobody" }}}

          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'nobody', headers["nested_header_value"]
        end

      end # subection - nested headers
      sub_test_case 'overloading' do

        test 'Last Set with no default Wins' do
          config = CONFIG + %[
              <header>
                name CorrelationID
                source requestid
              </header>
              <header>
                name CorrelationID
                source request.id
              </header>
              ]

          message = {
                      "requestid" => "top-level",
                      "request" => { "id" => "nested" }
                    }

          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'nested', headers["CorrelationID"]
        end
        test 'Last defined default will be used even if previous elements discovered' do
          config = CONFIG + %[
              <header>
                name CorrelationID
                source requestid
              </header>
              <header>
                name CorrelationID
                source request.id
                default expectMe
              </header>
              ]

          message = { "requestid" => "top-level" }

          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'expectMe', headers["CorrelationID"]
        end

        test 'Last discovered value is used when no defaults defined ' do
          config = CONFIG + %[
              <header>
                name CorrelationID
                source requestid
              </header>
              <header>
                name CorrelationID
                source request.id
              </header>
              ]

          message = {
                  "requestid" => "expectMe"
                }
          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'expectMe', headers["CorrelationID"]
        end

        test 'Failsafe default can be set' do
          config = CONFIG + %[
              <header>
                name CorrelationID
                default lastResort
              </header>
              <header>
                name CorrelationID
                source requestid
              </header>
              ]

          message = { }

          dl, meta, message = test_header_message_helper(config, message)

          headers = meta[:headers]
          assert_not_nil headers, "Did not find any headers"
          assert_equal 'lastResort', headers["CorrelationID"]
        end
      end # subection - nested headers
    end # Subsection - message_headers
  end
end
