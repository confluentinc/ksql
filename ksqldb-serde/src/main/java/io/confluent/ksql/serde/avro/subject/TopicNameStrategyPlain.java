/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.avro.subject;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Map;

/**
 * {@link SubjectNameStrategy} to register the values schema under just subject name &lt;topic&gt;.
 *
 * <p>Default {@link SubjectNameStrategy} is
 * {@link io.confluent.kafka.serializers.subject.TopicNameStrategy}: for any
 * messages published to &lt;topic&gt;, the schema of the message key is registered under the
 * subject name &lt;topic&gt;-key, and the message value is registered under the subject name
 * &lt;topic&gt;-value.</p>
 *
 * <p>In some cases it is not desired have '-key', '-value' value suffixes. For example in case you
 * are do <b>not</b> use schemas for keys (or rarely), and assume subject name in schema-registry
 * equal to &lt;topic&gt; name is for values and &lt;topic&gt;-key for keys</p>
 *
 * <p>
 * That may be configured in properties file like:
 * <code>
 * value.subject.name.strategy=io.confluent.ksql.serde.avro.subject.TopicNameStrategyPlain
 * </code>
 * Please refer to the <a href="https://u.to/_ut4Gg">documentation</a> for the details.
 * </p>
 *
 * <p>Implementation notes:
 * Do NOT extend {@link io.confluent.kafka.serializers.subject.TopicNameStrategy}!
 * Because that implements old interface too and in
 * {@link io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig#subjectNameStrategyInstance}
 * it will be substituted by {@link io.confluent.kafka.serializers.subject.SubjectNameStrategy}
 * silently!
 * </p>
 */
public class TopicNameStrategyPlain implements SubjectNameStrategy  {
  @Override
  public String subjectName(final String topic, final boolean isKey, final ParsedSchema schema) {
    return isKey ? topic + "-key" : topic;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
  }
}
