/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.tls;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.serialization.Deserializer;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ThreadLocalDeserializerTest {
  @Test
  @SuppressWarnings("unchecked")
  public void shouldUseAThreadLocalDeserializer() throws InterruptedException {
    final List<Deserializer<GenericRow>> serializers = new LinkedList<>();

    final ThreadLocalDeserializer serializer = new ThreadLocalDeserializer(
        () -> {
          final Deserializer<GenericRow> local = mock(Deserializer.class);
          serializers.add(local);
          expect(local.deserialize(anyString(), anyObject(byte[].class)))
              .andReturn(new GenericRow(Collections.emptyList()))
              .times(1);
          replay(local);
          return serializers.get(serializers.size() - 1);
        }
    );

    for (int i = 0; i < 3; i++) {
      final Thread t = new Thread(
          () -> serializer.deserialize("foo", new byte[32])
      );
      t.start();
      t.join();
      assertThat(serializers.size(), equalTo(i + 1));
      serializers.forEach(EasyMock::verify);
    }
  }
}
