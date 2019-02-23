# fluent-plugin-amqp

This plugin provides both a Source and Matcher which uses RabbitMQ as its transport.

[![Build Status](https://travis-ci.org/giraffi/fluent-plugin-amqp.svg?branch=master)](https://travis-ci.org/giraffi/fluent-plugin-amqp)
[![Gem Version](https://badge.fury.io/rb/fluent-plugin-amqp.svg)](https://badge.fury.io/rb/fluent-plugin-amqp)


# Table of contents

1. [Requirements](#requirements)
1. [Features](#features)
    1. [Highly Available Failover](#feat-failover)
1. [Configuration](#configuration)
    1. [Common parameters](#conf-common)
    1. [Source](#conf-source)
    1. [Matcher](#conf-matcher)
1. [Example Use Cases](#usecases)
    1. [Using AMQP instead of Fluent TCP forwarders](#uc-forwarder)
    1. [Enable TLS Authentication](#uc-tls)
1. [Contributing](#contributing)
1. [Copyright](#copyright)

# Requirements <a name="requirements"></a>


|fluent-amqp-plugin|fluent|ruby|
|----|----|----|
|>= 0.10.0 | >= 0.14.8 | >= 2.1 |
| < 0.10.0 | > 0.10.0, < 2 <sup>*</sup> | >= 1.9  |

* May not support all future fluentd features

# Features <a name="features"></a>

## Highly Available Failover <a name="feat-failover"></a>

You can use the `hosts` parameter to provide an array of rabbitmq hosts which
are in your cluster. This allows for highly avaliable configurations where a
node in your cluster may become inaccessible and this plugin will attempt a reconnection
on another host in the array.

> *WARNING:* Due to limitations in the library being used for connecting to RabbitMQ
each node of the cluster must use the same port, vhost and other configuration.

### Example

```
<source>
  type amqp
  hosts ["amqp01.example.com","amqp02.example.com"]
  port 5672
  vhost /
  user guest
  pass guest
  queue logs
  format json
</source>
```



# Configuration <a name="configuration"></a>

## A note on routing keys

If you would like to filter events from certain sources, you can make use of the
`key`, `tag_key` and `tag_header` configuration options.

The RabbitMQ [routing key](http://www.rabbitmq.com/tutorials/tutorial-four-ruby.html)
that is set for the message on the broker determines what you may be able to
filter against when consuming messages.

For example, if you want a 'catch-all' consumer that gets all messages from a
direct exchange, you should set `tag_key true` on both `source` and `matcher`. This
will then recreate the original event's tag ready for processing by the consumers
matchers.

If you want to have selective control over the messages that are consumed, you
can set `tag_key true` on the matcher, but `key some.tag` on the source. Only
messages with the given tag will be consumed, however its recommended that you
understand the difference between the different exchange types, and how multiple
consumers may impact message delivery.

## Common parameters <a name="conf-common"></a>

The following parameters are common to both matcher and source
plugins, and can be used as required.

|param|type|default|description|
|----|----|----|---|
|:host|:string|nil| *Required (if hosts unset)* Hostname of RabbitMQ server |
|:hosts|:array|nil| *Required (if host unset)* An array of hostnames of RabbitMQ server in a common cluster (takes precidence over `host`)|
|:user|:string|"guest"| Username to connect |
|:pass|:string|"guest"| Password to authenticate with (Secret) |
|:vhost|:string|"/"| RabbitMQ Virtual Host|
|:port|:integer|5672| RabbitMQ listening port|
|:durable|:bool|false| Should the queue or exchange be durable? |
|:passive|:bool|false| If true, will fail if queue or exchange does not exist |
|:auto_delete|:bool|false| Should the queue be deleted when all consumers have closed? |
|:heartbeat|:integer|60| Frequency of heartbeats to ensure quiet connections are kept open|
|:ssl|:bool|false| Is SSL enabled for this connection to RabbitMQ|
|:verify_ssl|:bool|false| Verify the SSL certificate presented by RabbitMQ |
|:tls|:bool|false| Should TLS be used for authentication |
|:tls_cert|:string|nil| Path (or content) of TLS Certificate |
|:tls_key|:string|nil| Path (or content) of TLS Key |
|:tls_ca_certificates|:array|nil| Array of paths to CA certificates |
|:tls_verify_peer|:bool|true| Verify the servers TLS certificate |
|:tag_key|:bool|false| Should the routing key be used for the event tag |
|:tag_header|:string|nil| What header should be used for the event tag |
|:time_header|:string|nil| What header should be used for the events timestamp |


## Source - Obtain events from a RabbitMQ queue <a name="conf-source"></a>


Using the amqp as a source allows you to read messages from RabbitMQ and handle
them in the same manner as a locally generated event.

It can be used in isolation; reading (well formed) events generated by other
applications and published onto a queue, or used with the amqp matcher, which
can replace the use of the fluent forwarders.


### Source specific parameters

Note: The following are in addition to the common parameters shown above.

|param|type|default|description|
|----|----|----|---|
|:tag|:string|"hunter.amqp"| Accepted events are tagged with this string (See also tag_key)|
|:queue|:string|nil| What queue contains the events to read |
|:exclusive|:bool|false| Should we have exclusive use of the queue? |
|:payload_format|:string|"json"| Deprecated - Use `format`|
|:bind_exchange|:boolean|false| Should the queue automatically bind to the exchange |
|:exchange|:string|nil| What exchange should the queue bind to? |
|:routing_key|:string|nil| What exchange should the queue bind to? |

### Example

```
<source>
  type amqp
  host amqp.example.com
  port 5672
  vhost /
  user guest
  pass guest
  queue logs
  format json
</source>
```

## Matcher - output events from RabbitMQ <a name="conf-matcher"></a>

### out
=======
### Matcher specific parameters

|param|type|default|description|
|----|----|----|----|
|:exchange|:string|""| Name of the exchange to send events to |
|:exchange_type|:string|"direct"| Type of exchange ( direct, fanout, topic, headers )|
|:persistent|:bool|false| | Are messages kept on the exchange even if RabbitMQ shuts down |
|:key|:string|nil| Routing key to attach to events (Only applies when `exchange_type topic`) See also `tag_key`|

### Example

```
<match **.**>
  type amqp
  key my_routing_key
  exchange amq.direct
  host amqp.example.com
  port 5672
  vhost /
  user guest
  pass guest
</match>
```

# Example Use Cases <a name="usecases"></a>

## Using AMQP instead of Fluent TCP forwarders <a name="uc-forwarder"></a>

One particular use case of the AMQP plugin is as an alternative to the built-in
fluent forwarders.

You can simply setup each client to output events to a RabbitMQ exchange
which is then consumed by one or more input agents.

The example configuration below shows how to setup a direct exchange, with
multiple consumers each receiving events.

### Matcher - Writes to Exchange

```
<match **>
  type amqp
  exchange amq.direct
  host amqp.example.com
  port 5672
  vhost /
  user guest
  pass guest
  format json
  tag_key true
</match>
```

### Source - Reads from queues

```
<source>
  type amqp
  host amqp.example.com
  port 5672
  vhost /
  user guest
  pass guest
  queue my_queue
  format json
  tag_key true
</source>
```

## Enable TLS Authentication <a name="uc-tls"></a>

The example below shows how you can configure TLS authentication using signed encryption keys
which will be validated by your appropriately configured RabbitMQ installation.

For more information on setting up TLS encryption, see the [Bunny TLS documentation](http://rubybunny.info/articles/tls.html)

Note: The 'source' configuration accepts the same arguments.

```
<match **.**>
  type amqp
  key my_routing_key
  exchange amq.direct
  host amqp.example.com
  port 5671              # Note that your port may change for TLS auth
  vhost /
  user guest
  pass guest

  tls true
  tls_key "/etc/fluent/ssl/client.key.pem"
  tls_cert "/etc/fluent/ssl/client.crt.pem"
  tls_ca_certificates ["/etc/fluent/ssl/server.cacrt.pem", "/another/ca/cert.file"]
  tls_verify_peer true

</match>
```

## Docker Container

A docker container is included in this project to help with testing and debugging.

You can simply build the docker container's ready for use with the following;
```
docker-compose build
```

Start the cluster of three containers with;
```
docker-compose up
```

And finally, submit test events, one a second, to the built in tcp.socket source
with;

```
while [ true ] ; do echo "{ \"test\": \"$(date)\" }" | nc ${DOCKER_IP} 20001; sleep 1; done
```


# Contributing to fluent-plugin-amqp <a name="contributing"></a>

- Check out the latest master to make sure the feature hasn't been implemented or the bug hasn't been fixed yet
- Check out the issue tracker to make sure someone already hasn't requested it and/or contributed it
- Fork the project
- Start a feature/bugfix branch
- Commit and push until you are happy with your contribution
- Make sure to add tests for it. This is important so I don't break it in a future version unintentionally.
- Please try not to mess with the Rakefile, version, or history. If you want to have your own version, or is otherwise necessary, that is fine, but please isolate to its own commit so I can cherry-pick around it.

# Copyright <a name="copyright"></a>

Copyright (c) 2011 Hiromi Ishii. See LICENSE.txt for
Copyright (c) 2013- github/giraffi. See LICENSE.txt for
further details.
