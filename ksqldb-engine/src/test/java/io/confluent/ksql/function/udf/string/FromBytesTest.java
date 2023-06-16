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

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class FromBytesTest {
    private FromBytes udf;

    @Before
    public void setUp() {
        udf = new FromBytes();
    }

    @Test
    public void shouldConvertBytesToHexString() {
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{33}), "hex"),
            is("21"));
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{}), "hex"),
            is(""));
    }

    @Test
    public void shouldConvertBytesToUtf8String() {
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{33}), "utf8"),
            is("!"));
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{}), "utf8"),
            is(""));
    }

    @Test
    public void shouldConvertAsciiStringToBytes() {
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{33}), "ascii"),
            is("!"));
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{}), "ascii"),
            is(""));
    }

    @Test
    public void shouldConvertBase64StringToBytes() {
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{33}), "base64"),
            is("IQ=="));
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{}), "base64"),
            is(""));
    }

    @Test
    public void shouldNotAddNewLinesToBase64EncodedBytes() {
        final byte[] b = new byte[200];
        Arrays.fill(b, (byte) 33);

        assertThat(udf.fromBytes(ByteBuffer.wrap(b), "base64"),
            is("ISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEh"
                + "ISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISE"
                + "hISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISE"
                + "hISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISE="));
    }

    @Test
    public void shouldReturnNullOnNullBytes() {
        assertThat(udf.fromBytes(null, "base64"), nullValue());
    }

    @Test
    public void shouldReturnNullOnNullEncoding() {
        assertThat(udf.fromBytes(ByteBuffer.wrap(new byte[]{0, 1, 2}), null), nullValue());
    }

    @Test
    public void shouldThrowOnUnknownEncodingType() {
        final Exception e = assertThrows(IllegalArgumentException.class,
            () -> udf.fromBytes(ByteBuffer.wrap(new byte[]{0, 1, 2}), "base5000"));

        assertThat(e.getMessage(), containsString("Unknown encoding type 'base5000'"));
    }
}
