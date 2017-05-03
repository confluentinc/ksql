/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.server.resources;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

@XmlAccessorType(XmlAccessType.FIELD)
public class KQLJsonRequest {
  public String kql;

  public String getKql() {
    return kql;
  }

  // In case this is ever useful for debugging
  @Override
  public String toString() {
    return String.format("KQLRequest {\"kql\":\"%s\"}", kql);
  }
}
