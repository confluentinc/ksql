/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ToBytesTest {
    private ToBytes udf;

    @Before
    public void setUp() {
        udf = new ToBytes();
    }


    @Test
    public void shouldConvertHexStringToBytes() {
        assertThat(udf.toBytes("21", "hex"),
            is(ByteBuffer.wrap(new byte[]{33})));
        assertThat(udf.toBytes("", "hex"),
            is(ByteBuffer.wrap(new byte[]{})));
        assertThat(udf.toBytes("1a2B", "hex"),
            is(ByteBuffer.wrap(new byte[]{26, 43})));
        assertThat(udf.toBytes("1A2B", "hex"),
            is(ByteBuffer.wrap(new byte[]{26, 43})));
    }

    @Test
    public void shouldConvertUtf8StringToBytes() {
        assertThat(udf.toBytes("!", "utf8"),
            is(ByteBuffer.wrap(new byte[]{33})));
        assertThat(udf.toBytes("", "utf8"),
            is(ByteBuffer.wrap(new byte[]{})));
    }

    @Test
    public void shouldConvertAsciiStringToBytes() {
        assertThat(udf.toBytes("!", "ascii"),
            is(ByteBuffer.wrap(new byte[]{33})));
        assertThat(udf.toBytes("", "ascii"),
            is(ByteBuffer.wrap(new byte[]{})));
    }

    @Test
    public void shouldConvertBase64StringToBytes() {
        assertThat(udf.toBytes("IQ==", "base64"),
            is(ByteBuffer.wrap(new byte[]{33})));
        assertThat(udf.toBytes("", "base64"),
            is(ByteBuffer.wrap(new byte[]{})));
    }

    @Test
    public void shouldReturnNullOnNullBytes() {
        assertThat(udf.toBytes(null, "base64"), nullValue());
    }

    @Test
    public void shouldReturnNullOnNullEncoding() {
        assertThat(udf.toBytes("IQ==", null), nullValue());
    }

    @Test
    public void shouldThrowOnUnknownEncodingType() {
        final Exception e = assertThrows(IllegalArgumentException.class,
            () -> udf.toBytes("AAEC", "base5000"));

        assertThat(e.getMessage(), containsString("Unknown encoding type 'base5000'"));
    }
}
