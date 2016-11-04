package io.confluent.ksql.util;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class KSQLUtil {

    public static String readQueryFile(String queryFilePah) throws IOException {
        RandomAccessFile queryRandomAccessFile = new RandomAccessFile(queryFilePah, "r");
        StringBuilder stringBuilder = new StringBuilder("");
        String queryLine = null;
        while ((queryLine = queryRandomAccessFile.readLine()) != null) {
          stringBuilder.append(queryLine.toUpperCase()+"   ");
        }
        queryRandomAccessFile.close();
        return stringBuilder.toString();
    }
}
