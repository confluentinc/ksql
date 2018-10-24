/*
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
 */

package io.confluent.ksql.util;

import io.confluent.ksql.KsqlEngine;
import java.util.Collections;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class EngineActiveQueryStatusSupplierTest {

  @Test
  public void shouldSuplyTrueIFThereIsActiveQuery() {
    //Given
    final KsqlEngine ksqlEngine = EasyMock.mock(KsqlEngine.class);
    EasyMock.expect(ksqlEngine.getLivePersistentQueries()).andReturn(Collections.singleton(EasyMock.mock(QueryMetadata.class)));
    final EngineActiveQueryStatusSupplier engineActiveQueryStatusSupplier = new EngineActiveQueryStatusSupplier(ksqlEngine);
    EasyMock.replay(ksqlEngine);

    //When
    final boolean hasActiveQuery = engineActiveQueryStatusSupplier.get();

    //Then
    Assert.assertTrue(hasActiveQuery);

  }

  @Test
  public void shouldSupplyFalseIfThereIsNoActiveQueries() {
    //Given
    final KsqlEngine ksqlEngine = EasyMock.mock(KsqlEngine.class);
    EasyMock.expect(ksqlEngine.getLivePersistentQueries()).andReturn(Collections.emptySet());
    final EngineActiveQueryStatusSupplier engineActiveQueryStatusSupplier = new EngineActiveQueryStatusSupplier(ksqlEngine);
    EasyMock.replay(ksqlEngine);

    //When
    final boolean hasActiveQuery = engineActiveQueryStatusSupplier.get();

    //Then
    Assert.assertFalse(hasActiveQuery);

  }
}
