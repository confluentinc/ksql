package io.confluent.ksql.serde.xml;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Field;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KsqlGenericRowXmlDeserializer implements Deserializer<GenericRow> {


  private Schema schema;

  KsqlGenericRowXmlDeserializer(Schema schema) {
    this.schema = schema;
  }
  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public GenericRow deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    GenericRow genericRow;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.parse(new ByteArrayInputStream(bytes));

      List columns = new ArrayList();


      for (Field field : schema.fields()) {
        String xmlValue =  getXmlValue(document, field.name());
        if (xmlValue == null) {
          columns.add(null);
        } else {
          columns.add(enforceFieldType(field.schema(), xmlValue));
        }
      }
      genericRow = new GenericRow(columns);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
    return genericRow;
  }

  String getXmlValue(Document document, String fieldPathToXml) {
    String[] fieldPath = fieldPathToXml.split("\\.");

    Element previousElement = null;

    for (int i = 0; i < fieldPath.length; i++) {
      String field = fieldPath[i];
      if (previousElement == null) {
        previousElement = (Element) document.getChildNodes().item(0);
      } else {
        // atribute
        if (field.startsWith("@")) {
          return previousElement.getAttribute(field.substring(1));
        } else {
          NodeList elementsByTagName = previousElement.getElementsByTagName(field);
          if (elementsByTagName.getLength() == 0) {
            return null;
          } else {

            // is leaf value
            if (i == fieldPath.length -1) {
              return elementsByTagName.item(0).getTextContent();
            }
            previousElement = (Element) elementsByTagName.item(0);
          }

        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Object enforceFieldType(Schema fieldSchema, Object value) {

    switch (fieldSchema.type()) {
      // TODO = do we need to force the type for streams?
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT64:
        return value;
      case STRING:
        if (value != null) {
          return value.toString();
        } else {
          return value;
        }
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.schema());

    }
  }


  @Override
  public void close() {

  }
}
