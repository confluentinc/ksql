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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
}
