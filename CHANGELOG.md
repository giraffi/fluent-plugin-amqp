
## 0.12.x

## 0.12.0 - Unreleased

* giraffi/fluent-plugin-amqp#48 - Added support for Multi-Workers

## 0.11.x

## 0.11.0 - March 10, 2017

* Added `content_type` and `content_encoding` configuration for
   the Matcher plugin - #37
* Added header options to set message headers based on the message content on
  the Matcher plugin - #35
* Enabled support for Coveralls code-coverage reporting

# 0.10.x

## 0.10.1 - 2016-11-17

* BugFix: Failed to set ruby_version properly and broke 0.10.0

## 0.10.0 - 2016-11-17

* *Breaking Change* - fluent-amqp-plugin only compatible with fluent >= 0.14.8 and ruby >= 2.1
* Feature: Updated to use new FluentD 0.14 plugin format
* Feature: Support for nanosecond precision

# 0.9.x

## 0.9.3

* BugFix: `:tls => false` hangs connections for some reason #32 HT: @mrkurt

## 0.9.2

* Better error handling in AMQP Matcher to deal with byte range errors, and
any other failure which would prevent buffers from being replayed effectivly.

## 0.9.1

* BugFix: giraffi/fluent-plugin-amqp#25 - Wrapped JSON.dump with simple catch which logs error but does not attempt to fix encoding errors

## 0.9.0

* Added support for Fluent 0.14.x
    * travis-ci build validates plugin against fluent 0.10, 0.12 and 0.14
* Compatibility: Use old json library when building for Ruby 1.9 and Fluent <= 0.12

# 0.8.x

## 0.8.2

* Miscs: *no effect* Remove fixed date from gemspec #22

## 0.8.1

* Fix bug: TypeError with single RabbitMQ. #21

## 0.8.0

* Add automatic failover support for multiple hosts on same cluster. #19

# 0.7.x

## 0.7.1

* Sources can now automatically bind to exchanges [Issue #16](https://github.com/giraffi/fluent-plugin-amqp/issues/16)
* Refactored Docker container to use docker-compose and use multiple containers for testing source and matches concurrently

## 0.7.0

* TLS configuration supported [#15](https://github.com/giraffi/fluent-plugin-amqp/pull/15)
* Docker container created for testing [#13](https://github.com/giraffi/fluent-plugin-amqp/pull/13)

# 0.6.x

## 0.6.1

* Heartbeat added to outbound connections [#11](https://github.com/giraffi/fluent-plugin-amqp/pull/11)

## 0.6.0

*Breaking Change:* Ruby 2.0+ now required

* Updated dependencies for Bunny and Fluent to latest
* Changed default Heartbeat from 0 to 60 seconds

# < 0.5.x

* See commit history for breakdown of changes
