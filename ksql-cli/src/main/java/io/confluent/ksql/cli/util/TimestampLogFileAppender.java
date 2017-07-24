/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.FileAppender;

public class TimestampLogFileAppender extends FileAppender {

  @Override
  public void setFile(String fileName) {
    if (fileName.contains("%timestamp")) {
      Date d = new Date();
      SimpleDateFormat format = new SimpleDateFormat("yyMMdd-HHmmss");
      fileName = fileName.replaceAll("%timestamp", format.format(d));
    }
    super.setFile(fileName);
  }

}