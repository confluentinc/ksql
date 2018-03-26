/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.parser;

import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Assert;
import org.junit.Test;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlFormatterTest {
  @Test
  public void testFormatSql() throws Exception {

    ArrayList<TableElement> tableElements = new ArrayList<>();
    tableElements.add(new TableElement("GROUP","STRING"));
    tableElements.add(new TableElement("NOLIT","STRING"));
    tableElements.add(new TableElement("Having","STRING"));

    CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        tableElements,
        false,
        Collections.singletonMap(
            DdlConfig.TOPIC_NAME_PROPERTY,
            new StringLiteral("topic_test")
        ));
    String sql = SqlFormatter.formatSql(createStream);
    Assert.assertTrue("literal escaping failure", sql.contains("`GROUP` STRING"));
    Assert.assertTrue("not literal escaping failure", sql.contains("NOLIT STRING"));
    Assert.assertTrue("lowercase literal escaping failure", sql.contains("`Having` STRING"));
    List<Statement> statements = new KsqlParser().buildAst(sql, MetaStoreFixture.getNewMetaStore());
    Assert.assertTrue("formatted sql parsing error", statements != null && ! statements.isEmpty());
  }

}

