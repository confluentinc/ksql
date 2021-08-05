/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.TypeRegistry.CustomType;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ListTypes;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.TypeList;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListTypesExecutorTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  @Mock
  private KsqlExecutionContext context;
  @Mock
  private MetaStore metaStore;
  @Mock
  private DistributingExecutor distributingExecutor;
  @Mock
  private KsqlSecurityContext ksqlSecurityContext;

  @Before
  public void setUp() {
    when(context.getMetaStore()).thenReturn(metaStore);
    when(metaStore.types())
        .thenReturn(
            ImmutableList.of(
                new CustomType("foo", SqlPrimitiveType.of(SqlBaseType.STRING))
            ).iterator());
  }

  @Test
  public void shouldListTypes() {
    // When:
    final Optional<KsqlEntity> entity = ListTypesExecutor.execute(
        ConfiguredStatement.of(PreparedStatement.of("statement", new ListTypes(Optional.empty())),
            SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())),
        mock(SessionProperties.class),
        context,
        null
    ).getEntity();

    // Then:
    assertThat("expected a response", entity.isPresent());
    assertThat(((TypeList) entity.get()).getTypes(), is(ImmutableMap.of(
        "foo", EntityUtil.schemaInfo(SqlPrimitiveType.of(SqlBaseType.STRING))
    )));
  }

}