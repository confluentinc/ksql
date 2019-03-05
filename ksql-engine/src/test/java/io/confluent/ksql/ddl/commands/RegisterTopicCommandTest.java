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
package io.confluent.ksql.ddl.commands;

import static org.easymock.MockType.NICE;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.HashMap;
import java.util.Map;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class RegisterTopicCommandTest {

    @Mock(NICE)
    private RegisterTopic registerTopicStatement;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

  private final MutableMetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());

    @Test
    public void testRegisterAlreadyRegisteredTopicThrowsException() {
        final RegisterTopicCommand cmd;

        // Given:
        givenProperties(propsWith(ImmutableMap.of()));
        cmd = createCmd();
        cmd.run(metaStore);

        // Then:
        expectedException.expectMessage("A topic with name 'name' already exists");

        // When:
        cmd.run(metaStore);
    }

    private RegisterTopicCommand createCmd() {
        return new RegisterTopicCommand(registerTopicStatement);
    }

    private static Map<String, Expression> propsWith(final Map<String, Expression> props) {
        Map<String, Expression> valid = new HashMap<>(props);
        valid.putIfAbsent(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("Json"));
        valid.putIfAbsent(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("some-topic"));
        return valid;
    }

    private void givenProperties(final Map<String, Expression> props) {
        EasyMock.expect(registerTopicStatement.getProperties()).andReturn(props).anyTimes();
        EasyMock.expect(registerTopicStatement.getName()).andReturn(QualifiedName.of("name")).anyTimes();
        EasyMock.replay(registerTopicStatement);
    }
}
