/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class KQLUtil {

  public static String readQueryFile(String queryFilePah) throws KQLException {
    try {
      RandomAccessFile queryRandomAccessFile = new RandomAccessFile(queryFilePah, "r");
      StringBuilder stringBuilder = new StringBuilder("");
      String queryLine = null;
      while ((queryLine = queryRandomAccessFile.readLine()) != null) {
        stringBuilder.append(queryLine + "   ");
      }
      queryRandomAccessFile.close();
      return stringBuilder.toString();
    } catch (FileNotFoundException fnf) {
      throw new KQLException("Could not load the query file from " + queryFilePah, fnf);
    } catch (IOException ioex) {
      throw new KQLException("Problem in reading from the query file " + queryFilePah, ioex);
    }
  }
}
