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

package io.confluent.ksql.util;

import io.confluent.ksql.util.BytesUtils.Encoding;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BytesUtilTest {
    @Test
    public void shouldReturnByteArrayOnReadOnlyByteBuffer() {
        // Given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{5}).asReadOnlyBuffer();

        // When
        final byte[] bytes = BytesUtils.getByteArray(buffer);

        // Then
        assertThat(bytes, is(new byte[]{5}));
    }

    @Test
    public void shouldReturnByteArrayOnWritableByteBuffer() {
        // Given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{5});

        // When
        final byte[] bytes = BytesUtils.getByteArray(buffer);

        // Then
        assertThat(bytes, is(new byte[]{5}));
    }

    @Test
    public void shouldReturnFullByteArrayWhenByteBufferPositionIsNotZero() {
        // Given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{5, 10, 15}).asReadOnlyBuffer();

        // This moves the internal array position to the next element and affects when we get
        // bytes from read-only buffers
        buffer.get();

        // When
        final byte[] bytes = BytesUtils.getByteArray(buffer);

        // Then
        assertThat(bytes, is(new byte[]{5, 10, 15}));
    }

    @Test
    public void shouldReturnSubArray() {
        // Given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});

        // When
        final byte[] bytes = BytesUtils.getByteArray(buffer, 1, 3);

        // Then
        assertThat(bytes, is(new byte[]{2, 3}));
    }

    @Test
    public void shouldReturnSubArrayWhenByteBufferPositionIsNotZero() {
        // Given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4}).asReadOnlyBuffer();

        // This moves the internal array position to the next element and affects when we get
        // bytes from read-only buffers
        buffer.get();

        // When
        final byte[] bytes = BytesUtils.getByteArray(buffer, 1, 3);

        // Then
        assertThat(bytes, is(new byte[]{2, 3}));
    }

    @Test
    public void shouldHandleNullEncoding() {
        assertThat(BytesUtils.encode(null, Encoding.UTF8), nullValue());
        assertThat(BytesUtils.encode(null, Encoding.BASE64), nullValue());
        assertThat(BytesUtils.encode(null, Encoding.ASCII), nullValue());
        assertThat(BytesUtils.encode(null, Encoding.HEX), nullValue());
    }

    @Test
    public void shouldHandleNullDecoding() {
        assertThat(BytesUtils.decode(null, Encoding.UTF8), nullValue());
        assertThat(BytesUtils.decode(null, Encoding.BASE64), nullValue());
        assertThat(BytesUtils.decode(null, Encoding.ASCII), nullValue());
        assertThat(BytesUtils.decode(null, Encoding.HEX), nullValue());
    }

    @Test
    public void shouldSplitOnSingleByteDelimiter() {
        // Given
        final byte[] value = new byte[]{1, 9, 2, 9, 3};
        final byte[] delim = new byte[]{9};

        // When
        final List<byte[]> parts = BytesUtils.split(value, delim);

        // Then
        assertThat(parts, contains(new byte[]{1}, new byte[]{2}, new byte[]{3}));
    }

    @Test
    public void shouldSplitOnMultiByteDelimiter() {
        // Given
        final byte[] value = new byte[]{1, 8, 9, 2, 8, 9, 3};
        final byte[] delim = new byte[]{8, 9};

        // When
        final List<byte[]> parts = BytesUtils.split(value, delim);

        // Then
        assertThat(parts, contains(new byte[]{1}, new byte[]{2}, new byte[]{3}));
    }

    @Test
    public void shouldNotMatchMultiByteDelimiterOnPartialMatch() {
        // Given: the delimiter's first byte (8) appears repeatedly but the full two-byte
        // delimiter [8, 9] never does, so nothing should be split off.
        final byte[] value = new byte[]{1, 8, 7, 8, 7, 8};
        final byte[] delim = new byte[]{8, 9};

        // When
        final List<byte[]> parts = BytesUtils.split(value, delim);

        // Then
        assertThat(parts, contains(value));
    }

    @Test
    public void shouldFindMultiByteDelimiterPastDelimiterLength() {
        // Given: the only full delimiter match starts at an index >= delim.length, which the
        // previous buggy comparison reported as a match at every first-byte occurrence.
        final byte[] value = new byte[]{1, 2, 3, 4, 5, 6};
        final byte[] delim = new byte[]{4, 5};

        // When
        final int idx = BytesUtils.indexOf(value, delim, 0);

        // Then
        assertThat(idx, is(3));
    }
}
