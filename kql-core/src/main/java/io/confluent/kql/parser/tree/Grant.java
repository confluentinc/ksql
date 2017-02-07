/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Grant
    extends Statement {

  private final Optional<List<String>> privileges; // missing means ALL PRIVILEGES
  private final boolean table;
  private final QualifiedName tableName;
  private final String grantee;
  private final boolean withGrantOption;

  public Grant(Optional<List<String>> privileges, boolean table, QualifiedName tableName,
               String grantee, boolean withGrantOption) {
    this(Optional.empty(), privileges, table, tableName, grantee, withGrantOption);
  }

  public Grant(NodeLocation location, Optional<List<String>> privileges, boolean table,
               QualifiedName tableName, String grantee, boolean withGrantOption) {
    this(Optional.of(location), privileges, table, tableName, grantee, withGrantOption);
  }

  private Grant(Optional<NodeLocation> location, Optional<List<String>> privileges, boolean table,
                QualifiedName tableName, String grantee, boolean withGrantOption) {
    super(location);
    requireNonNull(privileges, "privileges is null");
    this.privileges = privileges.map(ImmutableList::copyOf);
    this.table = table;
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.grantee = requireNonNull(grantee, "grantee is null");
    this.withGrantOption = withGrantOption;
  }

  public Optional<List<String>> getPrivileges() {
    return privileges;
  }

  public boolean isTable() {
    return table;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public String getGrantee() {
    return grantee;
  }

  public boolean isWithGrantOption() {
    return withGrantOption;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGrant(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(privileges, table, tableName, grantee, withGrantOption);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Grant o = (Grant) obj;
    return Objects.equals(privileges, o.privileges) &&
           Objects.equals(table, o.table) &&
           Objects.equals(tableName, o.tableName) &&
           Objects.equals(grantee, o.grantee) &&
           Objects.equals(withGrantOption, o.withGrantOption);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("privileges", privileges)
        .add("table", table)
        .add("tableName", tableName)
        .add("grantee", grantee)
        .add("withGrantOption", withGrantOption)
        .toString();
  }
}
