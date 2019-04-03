# KLIP 0 - Add Protobuf SerDe Support

Author: [Stephen Powis](http://github.com/crim)

Release target: 5.1

Status: In Discussion 

Discussion: _link to the design discussion PR_

## tl;dr

Add support to KSQL to process messages stored using the [ProtocolBuffers](https://developers.google.com/protocol-buffers/) serialization format.   

## Motivation and background

[ProtocolBuffers](https://developers.google.com/protocol-buffers/) is a language and platform neutral serialization format that is commonly used with Apache Kafka. Adding out-of-the-box support for this message format would expand the environments in which KSQL can be utilized.

## What is in scope

Adding SerDe support for ProtocolBuffers Version 3 to KSQL.

## What is not in scope

Supporting streams/topics containing multiple ProtocolBuffer schema types.  _See [Design](#Design) section below for more details._

## Value/Return

Out of the box support for running KSQL against Apache Kafka topics containing messages encoded with ProtocolBuffers.

## Public APIS

Proposed extending the `CREATE STREAM` grammar to include a new _value_format_ type **PROTOBUF** as well as adding a new _tableProperty_ named **protobuf_class**.

Example:
```genericsql
ksql> CREATE STREAM stream_name (....) WITH (kafka_topic='TopicName', value_format='PROTOBUF', protobuf_class='org.whatever.protobufs.MyProtoBuf$EncodedMessageType');
```

## Design  

Before digging into the design, it's important to understand how ProtocolBuffers work. A ProtocolBuffer encoded message does **not** include the schema definition. A consumer attempting to deserialize a ProtocolBuffer encoded message must know in advance the schema of that message. Because ProtocolBuffer messages lack this identifying information, services like SchemaRegistry do not exist for ProtocolBuffers.   

**Important:** Because of the limitations in being unable to identify ProtocolBuffer schemas from an encoded message, the proposed design will be limited to only support processing messages from a stream encoded with a single message type/schema.

When defining a new stream, the user would define the following _tableProperties_:
- value_format: **PROTOBUF**
- protobuf_class: fully qualified classname for the ProtocolBuffer class represented in the stream's messages.


The class defined by the _protobuf_class_ tableProperty would need to already exist at runtime within KSQL's classpath.

When the Serializer/Deserializer instances are created the _protobuf_class_ property would be passed to the KsqlProtobufSerializer/Deserializer via the `public void configure(final Map<String, ?> properties, ...)` method. The Serializer/Deserializer would then be able to instantiate an instance of the configured ProtocolBuffer class as needed. 

The Deserializer `public GenericRow deserialize(final String topic, final byte[] bytes)` implementation would accept a byte array representing a serialized ProtocolBuffer. This method would deserialize the byte array into the configured ProtocolBuffer instance, and apply the appropriate transformation into its equivalent `GenericRow` representation.

The Serializer `public byte[] serialize(final String topic, final GenericRow data)` implementation would accept a GenericRow instance and apply the correct transformation into the appropriate ProtocolBuffer representation. The ProtocolBuffer instance would then be serialized into a byte array and returned.

## Test plan

- Unit tests covering Serialization of Protobufs.
- Unit tests covering Deserialization of Protobufs.
- Unit tests covering new grammar.
- Integration tests covering....TBD. 

## Documentation Updates

* Update KSQL syntax reference to include new grammar and message format.
* Documentation updated to show how to package and place ProtocolBuffer classes onto KSQL's classpath.

# Compatibility implications

Only adding new functionality.  Changes should not break backwards compatibility.

## Performance implications

Adding support for ProtocolBuffer only adds overhead in the actual Serialization/Deserialization process itself. ProtocolBuffers is touted as being extremely performant, in many cases beating JSON serialization.
