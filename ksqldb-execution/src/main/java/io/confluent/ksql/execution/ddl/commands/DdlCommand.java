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

package io.confluent.ksql.execution.ddl.commands;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.PROPERTY
)
@JsonSubTypes({
    @Type(value = CreateStreamCommand.class, name = "createStreamV1"),
    @Type(value = CreateTableCommand.class, name = "createTableV1"),
    @Type(value = RegisterTypeCommand.class, name = "registerTypeV1"),
    @Type(value = DropSourceCommand.class, name = "dropSourceV1"),
    @Type(value = DropTypeCommand.class, name = "dropTypeV1"),
    @Type(value = AlterSourceCommand.class, name = "alterSourceV1")
})
public interface DdlCommand {
  DdlCommandResult execute(Executor executor);
}
