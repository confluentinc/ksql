/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.function.KsqlFunctionException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArrayJoinTest {

  private static final String CUSTOM_DELIMITER = "|";
  private static Struct STRUCT_DATA;

  private final ArrayJoin arrayJoinUDF = new ArrayJoin();

  @BeforeClass
  public static void initializeComplexStructTypeSampleData() {

    Schema structSchema = SchemaBuilder.struct()
        .field("f1", Schema.STRING_SCHEMA)
        .field("f2", Schema.INT32_SCHEMA)
        .field("f3", Schema.BOOLEAN_SCHEMA)
        .field("f4", SchemaBuilder.struct()
            .field("f4-1", Schema.STRING_SCHEMA)
            .build()
        )
        .field("f5", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("f6", SchemaBuilder.array(SchemaBuilder.struct()
            .field("k", Schema.STRING_SCHEMA)
            .field("v", Schema.INT32_SCHEMA)
            .build())
        )
        .field("f7",
            SchemaBuilder.array(SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA))))
        .field("f8", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .field("f9", SchemaBuilder
            .map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).build()).build())
        .field("f10", SchemaBuilder
            .map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
        .build();

    STRUCT_DATA = new Struct(structSchema)
        .put("f1", "ksqldb UDF sample data")
        .put("f2", 42)
        .put("f3", true)
        .put("f4", new Struct(structSchema.field("f4").schema())
            .put("f4-1", "hello ksqldb")
        )
        .put("f5", Arrays.asList("str_1", "str_2", "...", "str_N"))
        .put("f6", Arrays.asList(
            new Struct(structSchema.field("f6").schema().valueSchema())
                .put("k", "a").put("v", 1),
            new Struct(structSchema.field("f6").schema().valueSchema())
                .put("k", "b").put("v", 2),
            new Struct(structSchema.field("f6").schema().valueSchema())
                .put("k", "c").put("v", 3)
            )
        )
        .put("f7", Arrays.asList(
            Arrays.asList(Arrays.asList(0,1), Arrays.asList(2,3,4), Arrays.asList(5, 6),
                Arrays.asList(7,8,9)),
            Arrays.asList(Arrays.asList(9,8,7),Arrays.asList(6,5), Arrays.asList(4,3,2),
                Arrays.asList(1,0)
            )
        ))
        .put("f8", new LinkedHashMap<String, Integer>() {{
          put("k1", 6);
          put("k2", 5);
          put("k3", 4);
        }})
        .put("f9", new LinkedHashMap<String, List<String>>() {{
          put("k1", Arrays.asList("v1-a", "v1-b"));
          put("k2", Arrays.asList("v2-a","v2-b", "v2-c", "v2-d"));
          put("k3", Arrays.asList("v3-a","v3-b","v3-c"));
        }})
        .put("f10", new LinkedHashMap<String, List<Integer>>() {{
          put("k1", Arrays.asList(12, 21));
          put("k2", Arrays.asList(23, 32));
          put("k3", Arrays.asList(24, 42));
        }});
  }

  @Test
  public void shouldReturnNullForNullInput() {
    assertNull(arrayJoinUDF.join(null));
    assertNull(arrayJoinUDF.join(null,CUSTOM_DELIMITER));
  }

  @Test
  public void shouldReturnEmptyStringForEmptyArrays() {
    assertTrue(arrayJoinUDF.join(Collections.emptyList()).isEmpty());
    assertTrue(arrayJoinUDF.join(Collections.emptyList(),CUSTOM_DELIMITER).isEmpty());
  }

  @Test
  public void shouldReturnCorrectStringForFlatArraysWithPrimitiveTypes() {

    assertEquals("true,null,false",
        arrayJoinUDF.join(Arrays.asList(true, null, false)));
    assertEquals("true"+CUSTOM_DELIMITER+"null"+CUSTOM_DELIMITER+"false",
        arrayJoinUDF.join(Arrays.asList(true,null,false),CUSTOM_DELIMITER));

    assertEquals("1,23,-42,0",
        arrayJoinUDF.join(Arrays.asList(1,23,-42,0)));
    assertEquals("1"+CUSTOM_DELIMITER+"23"+CUSTOM_DELIMITER+"-42"+CUSTOM_DELIMITER+"0",
        arrayJoinUDF.join(Arrays.asList(1,23,-42,0),CUSTOM_DELIMITER));

    assertEquals("-4294967297,8589934592",
        arrayJoinUDF.join(Arrays.asList(new BigInteger("-4294967297"),
                                        new BigInteger("8589934592")))
    );
    assertEquals("-4294967297"+CUSTOM_DELIMITER+"8589934592",
        arrayJoinUDF.join(Arrays.asList(
            new BigInteger("-4294967297"), new BigInteger("8589934592")
            ), CUSTOM_DELIMITER)
    );

    assertEquals("1.23,-23.42,0.0",
        arrayJoinUDF.join(Arrays.asList(1.23,-23.42,0.0)));
    assertEquals("1.23"+CUSTOM_DELIMITER+"-23.42"+CUSTOM_DELIMITER+"0.0",
        arrayJoinUDF.join(Arrays.asList(1.23,-23.42,0.0),CUSTOM_DELIMITER));

    assertEquals("hello,from,,ksqldb,udf,null",
        arrayJoinUDF.join(Arrays.asList("hello","from","","ksqldb","udf",null)));
    assertEquals("hello"+CUSTOM_DELIMITER+"from"+CUSTOM_DELIMITER+CUSTOM_DELIMITER
            +"ksqldb"+CUSTOM_DELIMITER+"udf"+CUSTOM_DELIMITER+"null",
        arrayJoinUDF.join(Arrays.asList("hello","from","","ksqldb","udf",null),CUSTOM_DELIMITER));

  }

  @Test
  public void shouldReturnCorrectStringForNestedArraysWithPrimitiveTypes() {

    assertEquals("true,false,null,null,true",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(true,false),null,Arrays.asList(null,true))));
    assertEquals("true"+CUSTOM_DELIMITER+"false"+CUSTOM_DELIMITER+"null"
                  +CUSTOM_DELIMITER+"null"+CUSTOM_DELIMITER+"true",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(true,false), null, Arrays.asList(null,true)),
            CUSTOM_DELIMITER));

    assertEquals("0,0,7,null,100,-10",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(0,0,7),null,Arrays.asList(100,-10))));
    assertEquals("0"+CUSTOM_DELIMITER+"0"+CUSTOM_DELIMITER+"7"
            +CUSTOM_DELIMITER+"null"+CUSTOM_DELIMITER+"100"+CUSTOM_DELIMITER+"-10",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(0,0,7),null,Arrays.asList(100,-10)),
            CUSTOM_DELIMITER));

    assertEquals("-4294967297,0,8589934592,null,1",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(new BigInteger("-4294967297"),
            BigInteger.ZERO,new BigInteger("8589934592")),Arrays.asList(null,BigInteger.ONE))));
    assertEquals("-4294967297"+CUSTOM_DELIMITER+"0"+CUSTOM_DELIMITER+"8589934592"
            +CUSTOM_DELIMITER+"null"+CUSTOM_DELIMITER+"1",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(new BigInteger("-4294967297"),
            BigInteger.ZERO,new BigInteger("8589934592")),Arrays.asList(null,BigInteger.ONE)),
            CUSTOM_DELIMITER));

    assertEquals("1.23,-23.42,0.0,1.0",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(1.23,-23.42),Arrays.asList(0.0,1.0))));
    assertEquals("1.23"+CUSTOM_DELIMITER+"-23.42"+CUSTOM_DELIMITER+"0.0"+CUSTOM_DELIMITER+"1.0",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList(1.23,-23.42),Arrays.asList(0.0,1.0)),
            CUSTOM_DELIMITER));

    assertEquals("hello,from,,ksqldb,udf,null",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList("hello","from"),
            Arrays.asList("","ksqldb", "udf",null))));
    assertEquals("hello"+CUSTOM_DELIMITER+"from"+CUSTOM_DELIMITER+CUSTOM_DELIMITER
            +"ksqldb"+CUSTOM_DELIMITER+"udf"+CUSTOM_DELIMITER+"null",
        arrayJoinUDF.join(Arrays.asList(Arrays.asList("hello","from"),
            Arrays.asList("","ksqldb", "udf",null)),CUSTOM_DELIMITER));

  }

  @Test
  public void shouldReturnCorrectStringForNestedComplexTypes() {

    Map<String,Integer> mapData1 = new LinkedHashMap<>();
    mapData1.put("k1",12);
    mapData1.put("k2",34);
    mapData1.put("k3",0);

    Map<String,Integer> mapData2 = new LinkedHashMap<>();
    mapData2.put("k4",null);
    mapData2.put("k5",-100);

    assertEquals("k1,12,k2,34,k3,0,k4,null,k5,-100",
        arrayJoinUDF.join(Arrays.asList(mapData1,mapData2)));

    Map<String,List<String>> mapData3 = new LinkedHashMap<>();
    mapData3.put("k1",Arrays.asList("hello","from"));
    mapData3.put("k2",Arrays.asList("ksqldb",""));

    Map<String,List<String>> mapData4 = new LinkedHashMap<>();
    mapData4.put("k3",Arrays.asList(null,"",""));

    assertEquals("k1,hello,from,k2,ksqldb,,k3,null,,",
        arrayJoinUDF.join(Arrays.asList(mapData3,mapData4)));

    Map<String,Map<String,Integer>> mapData5 = new LinkedHashMap<>();
    mapData5.put("k1",mapData1);
    mapData5.put("k2",mapData2);

    assertEquals("k1,k1,12,k2,34,k3,0,k2,k4,null,k5,-100",
        arrayJoinUDF.join(Collections.singletonList(mapData5)));

    assertEquals("f1,ksqldb UDF sample data,f2,42,f3,true,f4,f4-1,hello ksqldb," +
            "f5,str_1,str_2,...,str_N,f6,k,a,v,1,k,b,v,2,k,c,v,3,f7,0,1,2,3,4,5,6,7,8,9," +
            "9,8,7,6,5,4,3,2,1,0,f8,k1,6,k2,5,k3,4,f9,k1,v1-a,v1-b,k2,v2-a,v2-b,v2-c,v2-d," +
            "k3,v3-a,v3-b,v3-c,f10,k1,12,21,k2,23,32,k3,24,42",
        arrayJoinUDF.join(Collections.singletonList(STRUCT_DATA)));

  }

  @Test
  public void shouldThrowExceptionForExamplesOfUnsupportedElementTypes() {
    assertThrows(KsqlFunctionException.class,
        () -> arrayJoinUDF.join(Arrays.asList('a','b')));
    assertThrows(KsqlFunctionException.class,
        () -> arrayJoinUDF.join(Arrays.asList(BigDecimal.ZERO,BigDecimal.ONE)));
    assertThrows(KsqlFunctionException.class,
        () -> arrayJoinUDF.join(Arrays.asList(-23.0f,42.42f,0.0f)));
    assertThrows(KsqlFunctionException.class,
        () -> arrayJoinUDF.join(Arrays.asList(
            new HashSet<>(Arrays.asList("foo", "blah")),
            new HashSet<>(Arrays.asList("ksqlDB", "UDF"))
        ))
    );
  }

}
