package io.confluent.ksql.serde.avro.subject;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Map;

/**
 * {@link SubjectNameStrategy} to register the values schema under just subject name &lt;topic&gt;.
 *
 * <p>Default {@link SubjectNameStrategy} is {@link io.confluent.kafka.serializers.subject.TopicNameStrategy}: for any
 * messages published to &lt;topic&gt;, the schema of the message key is registered under the subject name
 * &lt;topic&gt;-key, and the message value is registered under the subject name &lt;topic&gt;-value.</p>
 *
 * <p>In some cases it is not desired have '-key', '-value' value suffixes. For example in case you are do <b>not</b>
 * use schemas for keys (or rarely), and assume subject name in schema-registry equal to &lt;topic&gt; name is for
 * values and &lt;topic&gt;-key for keys</p>
 *
 * <p>
 * That may be configured in properties file like:
 * <code>
 * value.subject.name.strategy=io.confluent.ksql.serde.avro.subject.TopicNameStrategyPlain
 * </code>
 * Please refer to the <a href="https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#configuration-details">documentation</a> for the details.
 * </p>
 *
 * Implementation notes:
 * Do NOT extend {@link io.confluent.kafka.serializers.subject.TopicNameStrategy}! Because that implements old interface
 * too and in {@link io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig#subjectNameStrategyInstance(java.lang.String)}
 * it will be substituted by {@link io.confluent.kafka.serializers.subject.SubjectNameStrategy} silently!
 */
public class TopicNameStrategyPlain implements SubjectNameStrategy  {
    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        return isKey ? topic + "-key" : topic;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
