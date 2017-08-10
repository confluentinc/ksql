A Kafka broker is oblivious to the contents of the messages it handles and knows nothing of their contents. Indeed all the broker knows is that a message has a byte-array representing the 'key' portion and another byte-array representing the message 'value'. This is, in fact, one of the secrets of Kafka's blazing speed.
To process a Kafka message with a _structured_ query language statement, we of course need to describe a more detailed structure on top of these byte-arrays. 

(notes)
- serde (json/avro/delimited, avro is special-case)
- project schema (partial, optional bits)

Stream/Table duality
(link blog)

Continuous Query (transformation)


