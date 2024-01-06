# CRYSTAN
![Crystan logo image](./crystan_logo.png)
1. The *Crystan* Java library is intended for building small, separate components that talk via [NATS](https://nats.io/) messaging.
1. I've been enjoying Java interface default methods, so I'm using them here.
2. I already have a converter for JSON message formats, but may soon add support for [Fury](https://fury.apache.org/), too.
2. I'm using Java 21, but *Crystan* will probably work with Java 8.

## Usage
1. Implement the *BusConnector* interface.
2. Override the *getOptions* method to configure the NATS connection, if needed.
3. If you want to send messages, implement the *Publisher* interface.
4. If you want to receive messages, implement the *Subscriber* interface.
5. You will need to provide functions to convert between your message objects and the NATS byte[] payloads.

License: [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)