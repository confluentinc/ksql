/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.KsqlFunctionException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

public class ArrayJoinTest {

  private static final String CUSTOM_DELIMITER = "|";

  private final ArrayJoin arrayJoinUDF = new ArrayJoin();

  @Test
  public void shouldReturnNullForNullInput() {
    assertThat(arrayJoinUDF.join(null), nullValue());
    assertThat(arrayJoinUDF.join(null,CUSTOM_DELIMITER), nullValue());
  }

  @Test
  public void shouldReturnEmptyStringForEmptyArrays() {
    assertThat(arrayJoinUDF.join(Collections.emptyList()).isEmpty(),is(true));
    assertThat(arrayJoinUDF.join(Collections.emptyList(),CUSTOM_DELIMITER).isEmpty(),is(true));
  }

  @Test
  public void shouldReturnCorrectStringForFlatArraysWithPrimitiveTypes() {

    assertThat(arrayJoinUDF.join(Arrays.asList(true, null, false),""),
        is("truenullfalse")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(true, null, false)),
        is("true,null,false")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(true,null,false),CUSTOM_DELIMITER),
        is("true"+CUSTOM_DELIMITER+"null"+CUSTOM_DELIMITER+"false")
    );

    assertThat(arrayJoinUDF.join(Arrays.asList(1,23,-42,0),null), is("123-420"));
    assertThat(arrayJoinUDF.join(Arrays.asList(1,23,-42,0)), is("1,23,-42,0"));
    assertThat(arrayJoinUDF.join(Arrays.asList(1,23,-42,0),CUSTOM_DELIMITER),
        is("1"+CUSTOM_DELIMITER+"23"+CUSTOM_DELIMITER+"-42"+CUSTOM_DELIMITER+"0")
    );

    assertThat(arrayJoinUDF.join(Arrays.asList(-4294967297L, 8589934592L),""),
        is("-42949672978589934592")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(-4294967297L, 8589934592L)),
        is("-4294967297,8589934592")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(-4294967297L, 8589934592L), CUSTOM_DELIMITER),
        is("-4294967297"+CUSTOM_DELIMITER+"8589934592")
    );

    assertThat(arrayJoinUDF.join(Arrays.asList(1.23,-23.42,0.0),null),
        is("1.23-23.420.0")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(1.23,-23.42,0.0)),
        is("1.23,-23.42,0.0")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(1.23,-23.42,0.0),CUSTOM_DELIMITER),
        is("1.23"+CUSTOM_DELIMITER+"-23.42"+CUSTOM_DELIMITER+"0.0")
    );

    assertThat(arrayJoinUDF.join(
        Arrays.asList(new BigDecimal("123.45"), new BigDecimal("987.65")),null
        ),
        is("123.45987.65")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList(new BigDecimal("123.45"), new BigDecimal("987.65"))),
        is("123.45,987.65")
    );
    assertThat(arrayJoinUDF.join(
        Arrays.asList(new BigDecimal("123.45"), new BigDecimal("987.65")),CUSTOM_DELIMITER),
        is("123.45"+CUSTOM_DELIMITER+"987.65")
    );

    assertThat(arrayJoinUDF.join(Arrays.asList("Hello","From","","Ksqldb","Udf"),""),
        is("HelloFromKsqldbUdf")
    );
    assertThat(arrayJoinUDF.join(Arrays.asList("Hello","From","","Ksqldb","Udf")),
        is("Hello,From,,Ksqldb,Udf")
    );
    assertThat(
        arrayJoinUDF.join(Arrays.asList("hello","from","","ksqldb","udf",null),CUSTOM_DELIMITER),
        is("hello"+CUSTOM_DELIMITER+"from"+CUSTOM_DELIMITER+CUSTOM_DELIMITER
            +"ksqldb"+CUSTOM_DELIMITER+"udf"+CUSTOM_DELIMITER+"null")
    );

  }

  @Test
  public void shouldThrowExceptionForExamplesOfUnsupportedElementTypes() {
    assertThrows(KsqlFunctionException.class,
        () -> arrayJoinUDF.join(Arrays.asList('a','b')));
    assertThrows(KsqlFunctionException.class,
        () -> arrayJoinUDF.join(Arrays.asList(BigInteger.ONE,BigInteger.ZERO)));
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
