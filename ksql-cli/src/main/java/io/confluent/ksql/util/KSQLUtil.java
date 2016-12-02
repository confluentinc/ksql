package io.confluent.ksql.util;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class KSQLUtil {

  public static String readQueryFile(String queryFilePah) throws KSQLException {
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
      throw new KSQLException("Could not load the query file from " + queryFilePah, fnf);
    } catch (IOException ioex) {
      throw new KSQLException("Problem in reading from the query file " + queryFilePah, ioex);
    }
  }
}
