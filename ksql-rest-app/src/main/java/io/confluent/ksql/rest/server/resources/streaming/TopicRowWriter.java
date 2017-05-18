/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

public abstract class TopicRowWriter<K, V> implements ForeachAction {

  private static final Logger log = LoggerFactory.getLogger(TopicRowWriter.class);

  long interval;
  OutputStream out;
  long recordIndex = 0;
  ObjectMapper objectMapper;

  public TopicRowWriter(OutputStream out, long interval) {
    this.out = out;
    this.interval = interval;
    objectMapper = new ObjectMapper();
  }

  public void writeRow(final byte[] rowBytes) {
    if (interval > 0) {
      if (recordIndex % interval == 0) {
        printRowToOut(rowBytes);
      }
    } else {
      printRowToOut(rowBytes);
    }
    recordIndex++;
  }

  private void printRowToOut(final byte[] rowBytes) {
    try {
      synchronized (out) {
        if (rowBytes != null) {
          out.write(rowBytes);
          out.write("\n".getBytes());
        } else {
          out.write("null\n".getBytes());
        }
        out.flush();
      }
    } catch (Exception exception) {
      // Log the exception and ignore this row and contnue.
      log.error("Exception occurred while writing row: ", exception);
    }
  }
}
