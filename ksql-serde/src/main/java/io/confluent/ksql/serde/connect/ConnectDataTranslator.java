/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectDataTranslator implements DataTranslator {
  private static final String PATH_SEPARATOR = "->";

  private final Schema schema;

  public ConnectDataTranslator(final Schema schema) {
    this.schema = schema;
  }

  @Override
  public GenericRow toKsqlRow(final Schema connectSchema,
                              final Object connectData) {
    if (!schema.type().equals(Schema.Type.STRUCT)) {
      throw new KsqlException("Schema for a KSQL row should be a struct");
    }




    final Struct rowStruct = (Struct) toKsqlValue(schema, connectSchema, connectData, "");

    System.out.printf("ROW STRUCT: %s", rowStruct == null ? "FUCK" :  rowStruct.toString());


    if (rowStruct == null) {
      return null;
    }

    GenericRow result = null;

    try {
      result = new GenericRow(
              schema.fields()
                      .stream()
                      .map(f -> rowStruct.get(f.name()))
                      .collect(Collectors.toList())
      );
    } catch (Exception ex) {
       ex.printStackTrace();
    }

    System.out.printf("RETURNING ROW: %s", result == null ? "null" : result.toString());

    return result == null ?
            new GenericRow(schema.fields().stream().map(x -> "N/A").collect(Collectors.toList()))
            : result;
  }

  private RuntimeException createTypeMismatchException(final String pathStr,
                                                       final Schema schema,
                                                       final Schema connectSchema) {
    throw new DataException(
        String.format(
            "Cannot deserialize type %s as type %s for field %s",
            connectSchema.type().getName(),
            schema.type().getName(),
            pathStr));
  }

  private void validateType(final String pathStr,
                            final Schema schema,
                            final Schema connectSchema,
                            final Schema.Type... validTypes) {
    Arrays.stream(validTypes)
        .filter(connectSchema.type()::equals)
        .findFirst()
        .orElseThrow(
            () -> createTypeMismatchException(pathStr, schema, connectSchema));
  }

  private void validateSchema(final String pathStr,
                              final Schema schema,
                              final Schema connectSchema) {
    switch (schema.type()) {
      case BOOLEAN:
      case ARRAY:
      case MAP:
      case STRUCT:
        validateType(pathStr, schema, connectSchema, schema.type());
        break;
      case STRING:
        validateType(pathStr, schema, connectSchema,
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
            Schema.Type.BOOLEAN, Schema.Type.STRING);
        break;
      case INT64:
        validateType(
            pathStr, schema, connectSchema,
            Schema.Type.INT64, Schema.Type.INT32, Schema.Type.INT16, Schema.Type.INT8);
        break;
      case INT32:
        validateType(
            pathStr, schema, connectSchema,
            Schema.Type.INT32, Schema.Type.INT16, Schema.Type.INT8);
        break;
      case FLOAT64:
        validateType(pathStr, schema, connectSchema, Schema.Type.FLOAT32, Schema.Type.FLOAT64);
        break;
      default:
        throw new RuntimeException(
            "Unexpected data type seen in schema: " + schema.type().getName());
    }
  }

  private Object maybeConvertLogicalType(final Schema connectSchema, final Object connectValue) {
    System.out.printf("CONVERTING VALUE WITH SCHEMA [%s]: %s\n", connectSchema.name(), connectValue == null ? "FUCK" : connectValue.toString());
    if (connectSchema.name() == null) {
      return connectValue;
    }
    switch  (connectSchema.name()) {
      case Date.LOGICAL_NAME:
        return Date.fromLogical(connectSchema, (java.util.Date) connectValue);
      case Time.LOGICAL_NAME:
        return Time.fromLogical(connectSchema, (java.util.Date) connectValue);
      case Timestamp.LOGICAL_NAME:
        return Timestamp.fromLogical(connectSchema, (java.util.Date) connectValue);
      default:
        return connectValue;
    }
  }

  @SuppressWarnings("unchecked")
  private Object toKsqlValue(final Schema schema,
                             final Schema connectSchema,
                             final Object connectValue,
                             final String pathStr) {
    // Map a connect value+schema onto the schema expected by KSQL. For now this involves:
    // - handling case insensitivity for struct field names
    // - setting missing values to null
    if (connectSchema == null) {
      return null;
    }
    validateSchema(pathStr, schema, connectSchema);
    if (connectValue == null) {
      return null;
    }
    final Object convertedValue = maybeConvertLogicalType(connectSchema, connectValue);
    System.out.printf("CONVERTED VALUE: %s WITH SCHEMA [%s]\n", connectValue, schema.type());

    switch (schema.type()) {
      case INT64:
        return ((Number) convertedValue).longValue();
      case INT32:
        return ((Number) convertedValue).intValue();
      case FLOAT64:
        return ((Number) convertedValue).doubleValue();
      case ARRAY:
        return toKsqlArray(
            schema.valueSchema(), connectSchema.valueSchema(), (List) convertedValue, pathStr);
      case MAP:
        return toKsqlMap(
            schema.keySchema(), connectSchema.keySchema(),
            schema.valueSchema(), connectSchema.valueSchema(), (Map) convertedValue, pathStr);
      case STRUCT:
        System.out.printf("CREATING STRUCT ...[%s]\n", pathStr);
        return toKsqlStruct(schema, connectSchema, (Struct) convertedValue, pathStr);
      case STRING:
        // use String.valueOf to convert various int types and Boolean to string
        System.out.printf("CONVERTED VALUE IS STRING: %s\n", String.valueOf(convertedValue));
        return String.valueOf(convertedValue);
      default:
        return convertedValue;
    }
  }

  private List toKsqlArray(final Schema valueSchema, final Schema connectValueSchema,
                           final List<Object> connectArray, final String pathStr) {
    return connectArray.stream()
        .map(o -> toKsqlValue(
            valueSchema, connectValueSchema, o, pathStr + PATH_SEPARATOR + "ARRAY"))
        .collect(Collectors.toList());
  }

  private Map toKsqlMap(final Schema keySchema, final Schema connectKeySchema,
                        final Schema valueSchema, final Schema connectValueSchema,
                        final Map<String, Object> connectMap, final String pathStr) {
    return connectMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                e -> toKsqlValue(
                    keySchema,
                    connectKeySchema,
                    e.getKey(),
                    pathStr + PATH_SEPARATOR + "MAP_KEY"),
                e -> toKsqlValue(
                    valueSchema,
                    connectValueSchema,
                    e.getValue(),
                    pathStr + PATH_SEPARATOR + "MAP_VAL")
            ));
  }

  private Struct toKsqlStruct(final Schema schema,
                              final Schema connectSchema,
                              final Struct connectStruct,
                              final String pathStr) {
    // todo: check name here? e.g. what if the struct gets changed to a union?
    final Struct ksqlStruct = new Struct(schema);

    Struct newConnectStruct = connectStruct;
    Schema newConnectSchema = connectSchema;
    if(connectSchema.name() == "io.confluent.connect.avro.Union") {
      Field innerField = connectSchema.fields().stream().filter(f -> connectStruct.getStruct(f.name()) != null)
              .findFirst()
              .get();
      newConnectStruct = connectStruct.getStruct(innerField.name());
      newConnectSchema = innerField.schema();
      //new Field()
    }

    System.out.printf("TO KSQL STRUCT STARTING... [%s]\n", newConnectSchema.schema().name());

    final Map<String, String> caseInsensitiveFieldNameMap
        = getCaseInsensitiveFieldMap(newConnectSchema.schema());

    System.out.println("FIELDS IN MAP: ");
    for(String f: caseInsensitiveFieldNameMap.keySet()) {
      System.out.println(f + " -> " + caseInsensitiveFieldNameMap.getOrDefault(f, "FUCK"));
    }

    System.out.println("SCHEMA FIELDS:");
    schema.fields().stream().forEach(f -> System.out.printf("%s [%s]\n", f.name(), f.schema().type()));




    for (Field field : schema.fields()) {
      final String fieldNameUppercase = field.name().toUpperCase();
      // TODO: should we throw an exception if this is not true? this means the schema changed
      //       or the user declared the source with a schema incompatible with the registry schema

      System.out.printf("LOOKING FOR FIELD [%s] IN STRUCT [%s][%s]\n",
              fieldNameUppercase,
              newConnectStruct == null ? "N/A" : newConnectStruct.toString(),
              caseInsensitiveFieldNameMap.containsKey(fieldNameUppercase)
      );

      if (caseInsensitiveFieldNameMap.containsKey(fieldNameUppercase)) {
        String fieldName = caseInsensitiveFieldNameMap.get(fieldNameUppercase);
        Object fieldValue = null;
        try {
          fieldValue = newConnectSchema.field(fieldName) == null ? null : newConnectStruct.get(fieldName);
        } catch (Exception ex) { ex.printStackTrace(); }

        final Schema fieldSchema =  newConnectSchema.field(fieldName) != null ?
                newConnectSchema.field(fieldName).schema() :
                schema.field(fieldNameUppercase).schema();

        ksqlStruct.put(
            field.name(),
            toKsqlValue(
                field.schema(),
                fieldSchema,
                fieldValue,
                pathStr + PATH_SEPARATOR + field.name()));
      }
    }


    //schema.fields().add(new Field("ROWTYPE", schema.fields().size(),  SchemaBuilder.STRING_SCHEMA));
    //ConnectSchema s = (ConnectSchema)newConnectSchema.schema();
    //new ConnectSchema(s.type(), s.isOptional(), s.defaultValue(), s.name(), s.version(), s.doc(), s.parameters(), s.fields(), s.keySchema(), s.valueSchema());

    System.out.printf("RESULT STRUCT: %s\n", ksqlStruct);

    return ksqlStruct;
  }

  private Map<String, String> getCaseInsensitiveFieldMap(final Schema schema) {
    return schema.fields()
        .stream()
        .collect(
            Collectors.toMap(
                f -> f.name().toUpperCase(),
                Field::name));
  }

  public Struct toConnectRow(final GenericRow row) {
    final Struct struct = new Struct(schema);
    for (int i = 0; i < schema.fields().size(); i++) {
      struct.put(schema.fields().get(i).name(), row.getColumns().get(i));
    }
    return struct;
  }
}
