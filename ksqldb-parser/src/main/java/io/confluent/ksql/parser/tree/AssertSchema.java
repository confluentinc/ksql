/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class AssertSchema extends AssertResource {

  private final Optional<String> subject;
  private final Optional<Integer> id;

  public AssertSchema(
      final Optional<NodeLocation> location,
      final Optional<String> subject,
      final Optional<Integer> id,
      final Optional<WindowTimeClause> timeout,
      final boolean exists
  ) {
    super(location, timeout, exists);
    this.subject = Objects.requireNonNull(subject, "subject");
    this.id = Objects.requireNonNull(id, "id");
  }

  public Optional<String> getSubject() {
    return subject;
  }

  public Optional<Integer> getId() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AssertSchema that = (AssertSchema) o;
    return subject.equals(that.subject)
        && id.equals(that.id)
        && timeout.equals(that.timeout)
        && exists == that.exists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, id, timeout, exists);
  }

  @Override
  public String toString() {
    return "AssertSchema{"
        + "subject=" + subject
        + ",id=" + id
        + ",timeout=" + timeout
        + ",exists=" + exists
        + '}';
  }
}
