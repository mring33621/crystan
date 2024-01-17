# CRYSTAN
![Crystan logo image](./crystan_logo.png)
1. The *Crystan* Java library is intended for building small, separate components that talk via [NATS](https://nats.io/) messaging.
2. I've been enjoying Java interface default methods, so I'm (over)using them here.
3. I already have a converter for JSON message formats, but may soon add support for [Fury](https://fury.apache.org/), too.
4. I'm using Java 21, but *Crystan* will probably work with Java 8.
5. Crystan has 85% test coverage, with the automated unit and integration tests.

## High Level Usage
1. Servers: Choose from SingleThreadServer or ServerCore (uses [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)).
2. Clients: ClientCore
3. The high-level clients and servers pass around a 'jobId' to correlate request and response messages.
4. See the Integration Tests (*.IT.java) for examples of usage.

## Low Level Usage
1. Implement the *BusConnector* interface.
2. Override the *getOptions* method to configure the NATS connection, if needed.
3. If you want to send messages, implement the *Publisher* interface.
4. If you want to receive messages, implement the *Subscriber* interface.
5. You will need to provide functions to convert between your message objects and the NATS byte[] payloads.

## TODO:
1. High-level level parts should support a file-based NATS connection configuration.
2. What to do with high-level server performSideWork() method?
3. Fix integration tests to run under Linux. Currently, they only run under Windows.
4. Add support for [Fury](https://fury.apache.org/) or other message formats.

License: [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)