package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilTest {

  @Test
  public void testGetJavaType() {
    Class booleanClazz = SchemaUtil.getJavaType(Schema.BOOLEAN_SCHEMA);
    Class intClazz = SchemaUtil.getJavaType(Schema.INT32_SCHEMA);
    Class longClazz = SchemaUtil.getJavaType(Schema.INT64_SCHEMA);
    Class doubleClazz = SchemaUtil.getJavaType(Schema.FLOAT64_SCHEMA);
    Class StringClazz = SchemaUtil.getJavaType(Schema.STRING_SCHEMA);
    Class arrayClazz = SchemaUtil.getJavaType(SchemaBuilder.array(Schema.FLOAT64_SCHEMA));
    Class mapClazz = SchemaUtil.getJavaType(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));

    Assert.assertTrue(booleanClazz.getCanonicalName().equals("java.lang.Boolean"));
    Assert.assertTrue(intClazz.getCanonicalName().equals("java.lang.Integer"));
    Assert.assertTrue(longClazz.getCanonicalName().equals("java.lang.Long"));
    Assert.assertTrue(doubleClazz.getCanonicalName().equals("java.lang.Double"));
    Assert.assertTrue(StringClazz.getCanonicalName().equals("java.lang.String"));
    Assert.assertTrue(arrayClazz.getCanonicalName().equals("java.lang.Double[]"));
    Assert.assertTrue(mapClazz.getCanonicalName().equals("java.util.HashMap"));

  }

}
