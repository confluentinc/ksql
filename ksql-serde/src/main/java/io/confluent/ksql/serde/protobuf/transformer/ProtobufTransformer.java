package io.confluent.ksql.serde.protobuf.transformer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Internal;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Given a Schema definition, this class will transform a given ProtocolBuffer into the corresponding
 * GenericRow representation.
 *
 * WIP / Extremely rough.
 */
public class ProtobufTransformer {
  private final Schema schema;
  public int loopCount = 0;

  /**
   * Constructor.
   * @param schema Schema definition.
   */
  public ProtobufTransformer(final Schema schema) {
    this.schema = schema;
  }

  /**
   * Converts a ProtocolBuffer message into its corresponding GenericRow representation.
   * @param protobuf ProtocolBuffer message to convert.
   * @return GenericRow representing the ProtocolBuffer as defined by Schema.
   */
  public GenericRow convert(final MessageOrBuilder protobuf) {
    final List<Object> values = new ArrayList<>();

    // Loop over each field
    for (final Field field : schema.fields()) {
      values.add(convertField(field, protobuf));
    }

    return new GenericRow(values);
  }

  private Object convertField(final Field field, final MessageOrBuilder protobuf) {
    // Get name of field
    final String name = field.name();

    // Find the field.
    // TODO cache this?
    for (final Map.Entry<Descriptors.FieldDescriptor, Object> fieldEntry : protobuf.getAllFields().entrySet()) {
      loopCount++;
      final Descriptors.FieldDescriptor fieldDescriptor = fieldEntry.getKey();
      if (!fieldDescriptor.getName().equalsIgnoreCase(name)) {
        continue;
      }

      // Get the field's value.
      final Object value = fieldEntry.getValue();

      // Handle null case.
      if (value == null) {
        return null;
      }

      // If this field is an ENUM...
      if (fieldDescriptor.getType().name().equals("ENUM")) {
        // Determine if we should return the name (string) or ordinal value (number)
        switch (field.schema().type()) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            return ((Internal.EnumLite) value).getNumber();

          case STRING:
            return ((Descriptors.GenericDescriptor) value).getName();

          default:
            // TODO - ERROR!
            throw new RuntimeException("Type mismatch?");
        }
      }

      // Based on the type of the field..
      switch (field.schema().type()) {
        case INT8:
        case INT16:
        case INT32:
        case INT64:
        case FLOAT32:
        case FLOAT64:
        case BOOLEAN:
        case STRING:
        case ARRAY:
          return value;

        case BYTES:
          return ((ByteString)value).toByteArray();

        case MAP:
          final Map mapValue = new HashMap<>();
          for (final MapEntry<Object, Object> mapEntry: (Collection<MapEntry>) value) {
             mapValue.put(mapEntry.getKey(), mapEntry.getValue());
          }
          return mapValue;

        case STRUCT:
          final List values = new ArrayList<>();
          for (final Field subField : field.schema().fields()) {
            values.add(
              convertField(subField, (MessageOrBuilder) value)
            );
          }
          return values;
      }
    }
    return null;
  }

  public Message convert(final GenericRow genericRow, final Message.Builder builder) {
    // Loop over each field
    final Iterator<Object> fieldValueIterator = genericRow.getColumns().iterator();
    for (final Field field : schema.fields()) {
      final Object value = fieldValueIterator.next();
      buildField(field, builder, value);
    }

    return builder.build();
  }

  private void buildField(final Field field, final Message.Builder builder, final Object value) {
    // Find matching field
    for (final Descriptors.FieldDescriptor fieldDescriptor: builder.getDescriptorForType().getFields()) {
      if (fieldDescriptor.getName().equalsIgnoreCase(field.name())) {

        // If this field is an ENUM...
        if (fieldDescriptor.getType().name().equals("ENUM")) {
          // Determine if we should return the name (string) or ordinal value (number)
          switch (field.schema().type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
              builder.setField(
                fieldDescriptor,
                fieldDescriptor.getEnumType().findValueByNumber((Integer) value)
              );
              return;

            case STRING:
              builder.setField(
                fieldDescriptor,
                fieldDescriptor.getEnumType().findValueByName((String) value)
              );
              return;

            default:
              // TODO - ERROR!
              throw new RuntimeException("Type mismatch?");
          }
        }

        // Based on the type of the field..
        switch (field.schema().type()) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
          case FLOAT32:
          case FLOAT64:
          case BOOLEAN:
          case STRING:
          case ARRAY:
            builder.setField(fieldDescriptor, value);
            return;

          case BYTES:
            builder.setField(
              fieldDescriptor,
              ByteString.copyFrom((byte[])value)
            );
            return;

          case MAP:
            final Message.Builder mapBuilder = builder.newBuilderForField(fieldDescriptor);
            final Descriptors.FieldDescriptor keyField = mapBuilder.getDescriptorForType().getFields().get(0);
            final Descriptors.FieldDescriptor valueField = mapBuilder.getDescriptorForType().getFields().get(1);

            for (final Map.Entry entry : ((Map<Object, Object>)value).entrySet()) {
              builder.addRepeatedField(fieldDescriptor,
                builder
                  .newBuilderForField(fieldDescriptor)
                  .setField(keyField, entry.getKey())
                  .setField(valueField, entry.getValue())
                  .build()
              );
            }
            return;

          case STRUCT:
            final Message.Builder structBuilder = builder.newBuilderForField(fieldDescriptor);
            final Iterator<Object> subValueIterator = ((Collection)value).iterator();
            for (final Field subField : field.schema().fields()) {
                buildField(subField, structBuilder, subValueIterator.next());
            }
            builder.setField(fieldDescriptor, structBuilder.build());
            return;
        }
      }
    }
  }
}
