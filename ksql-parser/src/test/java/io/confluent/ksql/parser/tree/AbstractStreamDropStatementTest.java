package io.confluent.ksql.parser.tree;

import com.google.common.testing.EqualsTester;
import java.util.Optional;
import org.junit.Test;

public class AbstractStreamDropStatementTest {

  @Test
  public void testHashAndEquals() {
    new EqualsTester()
        // equals should ignore NodeLocation
        .addEqualityGroup(
            new DropStream(Optional.empty(), QualifiedName.of("a"), true, true),
            new DropStream(Optional.empty(), QualifiedName.of("a"), true, true),
            new DropStream(Optional.of(new NodeLocation(0, 0)), QualifiedName.of("a"), true, true),
            new DropStream(Optional.of(new NodeLocation(0, 1)), QualifiedName.of("a"), true, true))
        // equals should check qualified names
        .addEqualityGroup(new DropStream(Optional.empty(), QualifiedName.of("b"), true, true))
        // equals should check ifExists
        .addEqualityGroup(new DropStream(Optional.empty(), QualifiedName.of("a"), false, true))
        // equals should check deleteTopic
        .addEqualityGroup(new DropStream(Optional.empty(), QualifiedName.of("a"), true, false))
        .testEquals();
  }

}