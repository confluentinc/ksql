/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

public class CreateFunction
    extends Statement implements ExecutableDdlStatement {
  private final QualifiedName name;
  private final List<TableElement> elements;
  private final String language;
  private final String script;
  private final Schema returnType;
  private final Map<String, Expression> properties;
  private final Boolean replace;
  private static final String[] SUPPORTED_LANGUAGES = {
    "java"
  };

  private static final class Config {
    public static final String AUTHOR_PROPERTY = "AUTHOR";
    public static final String DESCRIPTION_PROPERTY = "DESCRIPTION";
    public static final String OVERVIEW_PROPERTY = "OVERVIEW";
    public static final String VERSION_PROPERTY = "VERSION";

    private Config() {
      // this utility class should not be instantiated
    }
  }

  public CreateFunction(
      final QualifiedName name,
      final List<TableElement> elements,
      final String language,
      final String script,
      final Schema returnType,
      final Map<String, Expression> properties,
      final Boolean replace) {
    this(Optional.empty(), name, elements, language, script, returnType, properties, replace);
  }

  public CreateFunction(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final List<TableElement> elements,
      final String language,
      final String script,
      final Schema returnType,
      final Map<String, Expression> properties,
      final Boolean replace) {
    super(location);
    this.name = requireNonNull(name, "function name is null");
    this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
    this.language = formatLanguage(requireNonNull(language, "language name is null"));
    this.script = requireNonNull(script, "udf script is null");
    this.returnType = requireNonNull(returnType, "return type is null");
    this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    this.replace = requireNonNull(replace, "replace is null");
  }

  public String formatLanguage(final String lang) {
    final String language = lang.toLowerCase().trim();
    if (!Arrays.stream(SUPPORTED_LANGUAGES).anyMatch(language::equals)) {
      final String errorMessage =
            String.format("Unsupported language '%s'. Please choose from the following: %s",
                language, Arrays.toString(SUPPORTED_LANGUAGES));
      throw new KsqlException(errorMessage);
    }
    return language;
  }

  public List<TableElement> getElements() {
    return elements;
  }

  public List<Schema> getArguments() {
    final List<Schema> arguments = new ArrayList<>();
    for (TableElement element : getElements()) {
      arguments.add(LogicalSchemas.fromSqlTypeConverter().fromSqlType(element.getType()));
    }
    return arguments;
  }

  public String[] getArgumentNames() {
    final List<TableElement> elements = getElements();
    final int size = elements.size();
    final String[] argumentNames = new String[size];
    for (int i = 0; i < size; i++) {
      argumentNames[i] = elements.get(i).getName();
    }
    return argumentNames;
  }

  public Class[] getArgumentTypes() {
    final List<TableElement> elements = getElements();
    final int size = elements.size();
    final Class[] argumentTypes = new Class[size];
    for (int i = 0; i < size; i++) {
      final Schema schema = LogicalSchemas
          .fromSqlTypeConverter()
          .fromSqlType(elements.get(i).getType());
      argumentTypes[i] = SchemaUtil.getJavaType(schema);
    }
    return argumentTypes;
  }

  public String getAuthor() {
    if (properties.containsKey(Config.AUTHOR_PROPERTY)) {
      return properties.get(Config.AUTHOR_PROPERTY).toString();
    }
    return "";
  }

  public String getDescription() {
    if (properties.containsKey(Config.DESCRIPTION_PROPERTY)) {
      return properties.get(Config.DESCRIPTION_PROPERTY).toString();
    }
    return "";
  }

  public String getOverview() {
    if (properties.containsKey(Config.OVERVIEW_PROPERTY)) {
      return properties.get(Config.OVERVIEW_PROPERTY).toString();
    }
    return "";
  }

  public String getVersion() {
    if (properties.containsKey(Config.VERSION_PROPERTY)) {
      return properties.get(Config.VERSION_PROPERTY).toString();
    }
    return "";
  }

  public String getName() {
    return name.toString();
  }

  public String getLanguage() {
    return language;
  }

  public Schema getReturnType() {
    return returnType;
  }

  public String getScript() {
    return script;
  }

  public Boolean shouldReplace() {
    return replace;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateFunction(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, elements);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateFunction o = (CreateFunction) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(elements, o.elements)
           && Objects.equals(language, o.language)
           && Objects.equals(script, o.script);
  }

  @Override
  public String toString() {
    return getName();
  }
}