/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

public class StringUtil {
    public static String cleanQuotes(final String propertyValue) {
        // TODO: move the check to grammer
        if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
            throw new KsqlException(propertyValue + " value is string and should be enclosed between "
                    + "\"'\".");
        }
        // TODO: [BUG] one side quote
        return propertyValue.substring(1, propertyValue.length() - 1);
    }
}
