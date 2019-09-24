/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.rewrite.StatementRewriteForStruct;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.util.function.BiConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EngineContextTest {

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private QueryIdGenerator queryIdGenerator;
  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private BiConsumer<ServiceContext, QueryMetadata> onQueryCloseCallback;
  @Mock
  private KsqlParser parser;
  @Mock
  private StatementRewriteForStruct rewriter;
  @Mock
  private ParsedStatement parsed;
  @Mock
  private PreparedStatement<?> prepared;
  @Mock
  private Statement statement;
  @Mock
  private Statement rewrittenStatement;
  private EngineContext engineContext;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    engineContext = new EngineContext(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        onQueryCloseCallback,
        parser,
        rewriter
    );

    when((parsed).getStatementText()).thenReturn("some sql");
    when(((PreparedStatement)prepared).getStatement()).thenReturn(statement);
    when(parser.prepare(any(), any())).thenReturn((PreparedStatement)prepared);
    when(rewriter.rewriteForStruct(any())).thenReturn(rewrittenStatement);
  }

  @Test
  public void shouldRewritePreparedForStruct() {
    // When:
    final PreparedStatement<?> prepared = engineContext.prepare(parsed);

    // Then:
    verify(rewriter).rewriteForStruct(statement);
    assertThat(prepared.getStatement(), is(sameInstance(rewrittenStatement)));
  }
}