/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.tls;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.GenericRow;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.easymock.EasyMock;
import org.easymock.internal.LastControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ThreadLocalSerializerTest {

  @Before
  public void before(){
    LastControl.pullMatchers();
  }

  @After
  public void after(){
    LastControl.pullMatchers();
  }

  @Test
  public void shouldUseAThreadLocalSerializer() throws InterruptedException {
    final List<Serializer<GenericRow>> serializers = new LinkedList<>();

    final ThreadLocalSerializer<GenericRow> serializer = new ThreadLocalSerializer<>(
        () -> {
          final Serializer<GenericRow> local = mock(Serializer.class);
          serializers.add(local);
          expect(local.serialize(anyString(), anyObject(GenericRow.class)))
              .andReturn(new byte[32])
              .times(1);
          replay(local);
          return serializers.get(serializers.size() - 1);
        }
    );

    for (int i = 0; i < 3; i++) {
      final Thread t = new Thread(
          () -> serializer.serialize("foo", new GenericRow())
      );
      t.start();
      t.join();
      assertThat(serializers.size(), equalTo(i + 1));
      serializers.forEach(EasyMock::verify);
    }
  }
}
