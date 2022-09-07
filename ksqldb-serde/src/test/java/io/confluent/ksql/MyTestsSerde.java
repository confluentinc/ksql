package io.confluent.ksql;

import io.confluent.ksql.logging.processing.DeserializationErrorTest;
import io.confluent.ksql.logging.processing.LoggingDeserializerTest;
import io.confluent.ksql.logging.processing.LoggingSerializerTest;
import io.confluent.ksql.logging.processing.SerializationErrorTest;
import io.confluent.ksql.schema.ksql.PhysicalSchemaTest;
import io.confluent.ksql.serde.avro.AvroDataTranslatorTest;
import io.confluent.ksql.serde.avro.AvroFormatTest;
import io.confluent.ksql.serde.avro.AvroPropertiesTest;
import io.confluent.ksql.serde.avro.AvroSRSchemaDataTranslatorTest;
import io.confluent.ksql.serde.avro.AvroSchemaTranslatorTest;
import io.confluent.ksql.serde.avro.KsqlAvroDeserializerTest;
import io.confluent.ksql.serde.avro.KsqlAvroSerializerTest;
import io.confluent.ksql.serde.connect.ConnectDataTranslatorTest;
import io.confluent.ksql.serde.connect.ConnectFormatSchemaTranslatorTest;
import io.confluent.ksql.serde.connect.ConnectFormatTest;
import io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslatorTest;
import io.confluent.ksql.serde.connect.ConnectSRSchemaDataTranslatorTest;
import io.confluent.ksql.serde.connect.ConnectSchemasTest;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializerTest;
import io.confluent.ksql.serde.connect.SchemaTranslationPoliciesTest;
import io.confluent.ksql.serde.connect.SchemaTranslationPolicyTest;
import io.confluent.ksql.serde.delimited.KsqlDelimitedDeserializerTest;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerializerTest;
import io.confluent.ksql.serde.json.JsonPropertiesTest;
import io.confluent.ksql.serde.json.JsonSchemaPropertiesTest;
import io.confluent.ksql.serde.json.JsonSerdeUtilsTest;
import io.confluent.ksql.serde.json.KsqlJsonDeserializerTest;
import io.confluent.ksql.serde.json.KsqlJsonSchemaDeserializerTest;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactoryTest;
import io.confluent.ksql.serde.json.KsqlJsonSerializerTest;
import io.confluent.ksql.serde.kafka.KafkaSerdeFactoryTest;
import io.confluent.ksql.serde.none.NoneFormatTest;
import io.confluent.ksql.serde.protobuf.KsqlProtobufDeserializerTest;
import io.confluent.ksql.serde.protobuf.KsqlProtobufNoSRDeserializerTest;
import io.confluent.ksql.serde.protobuf.KsqlProtobufNoSRSerializerTest;
import io.confluent.ksql.serde.protobuf.KsqlProtobufSerializerTest;
import io.confluent.ksql.serde.protobuf.ProtobufDataTranslatorTest;
import io.confluent.ksql.serde.protobuf.ProtobufFormatTest;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRFormatTest;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRSerdeFactoryTest;
import io.confluent.ksql.serde.protobuf.ProtobufPropertiesTest;
import io.confluent.ksql.serde.protobuf.ProtobufSchemaTranslatorTest;
import io.confluent.ksql.serde.protobuf.ProtobufSchemasTest;
import io.confluent.ksql.serde.protobuf.ProtobufSerdeFactoryTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        DeserializationErrorTest.class,
        LoggingDeserializerTest.class,
        LoggingSerializerTest.class,
        SerializationErrorTest.class,
        PhysicalSchemaTest.class,
        AvroDataTranslatorTest.class,
        AvroFormatTest.class,
        AvroPropertiesTest.class,
        AvroSchemaTranslatorTest.class,
        AvroSRSchemaDataTranslatorTest.class,
        KsqlAvroDeserializerTest.class,
        KsqlAvroSerializerTest.class,
        ConnectDataTranslatorTest.class,
        ConnectFormatSchemaTranslatorTest.class,
        ConnectFormatTest.class,
        ConnectKsqlSchemaTranslatorTest.class,
        ConnectSchemasTest.class,
        ConnectSRSchemaDataTranslatorTest.class,
        KsqlConnectDeserializerTest.class,
        SchemaTranslationPoliciesTest.class,
        SchemaTranslationPolicyTest.class,
        KsqlDelimitedDeserializerTest.class,
        KsqlDelimitedSerializerTest.class,
        JsonPropertiesTest.class,
        JsonSchemaPropertiesTest.class,
        JsonSerdeUtilsTest.class,
        KsqlJsonDeserializerTest.class,
        KsqlJsonSchemaDeserializerTest.class,
        KsqlJsonSchemaDeserializerTest.class,
        KsqlJsonSerdeFactoryTest.class,
        KsqlJsonSerializerTest.class,
        KafkaSerdeFactoryTest.class,
        NoneFormatTest.class,
        KsqlProtobufDeserializerTest.class,
        KsqlProtobufNoSRDeserializerTest.class,
        KsqlProtobufNoSRSerializerTest.class,
        KsqlProtobufSerializerTest.class,
        ProtobufDataTranslatorTest.class,
        ProtobufFormatTest.class,
        ProtobufNoSRFormatTest.class,
        ProtobufNoSRSerdeFactoryTest.class,
        ProtobufPropertiesTest.class,
        ProtobufSchemasTest.class,
        ProtobufSchemaTranslatorTest.class,
        ProtobufSerdeFactoryTest.class,

})
public class MyTestsSerde {
}