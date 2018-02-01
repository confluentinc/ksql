/**
 * Copyright 2017 Confluent Inc.
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
package io.confluent.ksql.serde.xml;

import io.confluent.ksql.GenericRow;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KsqlGenericRowXmlSerializer implements Serializer<GenericRow> {

  private final Schema schema;
  private final DocumentBuilder docBuilder;

  public KsqlGenericRowXmlSerializer(Schema schema) {
    this.schema = schema;

    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    try {
      docBuilder = docFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String topic, GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }
    try {
      return buildXmlModelOld(genericRow); // use avro-schema names and values to build the XML structure and store as strings

    } catch (Exception e) {
      e.printStackTrace();
      throw new SerializationException(e);
    }
  }

  byte[] buildXmlModel( GenericRow genericRow) {
    // iterate over the fields, building the DOM
    //record.
    return null;
  }
  byte[] buildXmlModelOld(GenericRow avroRecord) {

    Document doc = docBuilder.newDocument();

    List<Field> fields = schema.fields();
    for (Field field : fields) {
      String fieldName = field.name();

      System.out.println("\nFIELD:" + fieldName);
      String[] split = fieldName.split("\\.");
      boolean attribute = false;
      Element previousElement = null;
      for (String currentName : split) {
        System.out.println("NODE:" + currentName);
        if (currentName.startsWith("@")) {
          currentName = currentName.substring(1);
          attribute = true;
        }
        if (attribute) {
          if (previousElement == null) {
            throw new RuntimeException("Cannot create XmlAttribute without XmlElement");
          }
          previousElement.setAttribute(currentName, avroRecord.getColumns().get(field.index()).toString());

        } else {

          NodeList nodeList = doc.getElementsByTagName(currentName);
          if (nodeList.getLength() == 0) {
            Element element = doc.createElement(currentName);
            // are we at the leaf node?
            if (fieldName.endsWith(currentName)) {
              element.setTextContent(avroRecord.getColumns().get(field.index()).toString());
            }
            // extend the existing node structure OR we are at the root
            if (previousElement != null) {
              previousElement.appendChild(element);
              previousElement = element;
            } else {
              // At the root - check that root exists?
              NodeList childNodes = doc.getChildNodes();
              if (childNodes.getLength() == 0) {
                doc.appendChild(element);
                previousElement = element;
              } else{
                // pre-existing, use existing root node
                previousElement = (Element) childNodes.item(0);
              }
            }
          } else {
            previousElement = (Element) nodeList.item(0);
          }
        }
      }
    }

    OutputFormat format    = new OutputFormat(doc);
    // as a String
    StringWriter stringOut = new StringWriter ();
    XMLSerializer serial   = new XMLSerializer (stringOut, format);
    try {
      serial.serialize(doc);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stringOut.toString().getBytes();
  }

  @Override
  public void close() {

  }
}
