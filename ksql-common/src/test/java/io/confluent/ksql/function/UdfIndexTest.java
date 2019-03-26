package io.confluent.ksql.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class UdfIndexTest {

  private static final Schema STRING_VARARGS = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA);
  private static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;
  private static final Schema INT = Schema.OPTIONAL_INT32_SCHEMA;
  private static final Schema STRUCT1 = SchemaBuilder.struct().field("a", STRING).build();
  private static final Schema STRUCT2 = SchemaBuilder.struct().field("b", INT).build();
  private static final Schema MAP1 = SchemaBuilder.map(STRING, STRING).build();
  private static final Schema MAP2 = SchemaBuilder.map(STRING, INT).build();

  private static final String EXPECTED = "expected";

  private UdfIndex udfIndex;

  @Before
  public void setUp() {
    udfIndex = new UdfIndex(new UdfMetadata("name", "description", "confluent", "1", "", false));
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Parameter(0) public String name;
  @Parameter(1) public KsqlFunction[] functions;
  @Parameter(2) public Schema[] parameter;
  @Parameter(3) public boolean shouldFind;

  @Parameters(name="{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        // non-vararg tests
        new Object[]{
            "shouldFindNoArgs",
            new KsqlFunction[]{
                function(EXPECTED, false)
            },
            new Schema[]{},
            true
        },
        new Object[]{
            "shouldFindOneArg",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING)
            },
            new Schema[]{STRING},
            true
        },
        new Object[]{
            "shouldFindTwoDifferentArgs",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, INT)
            },
            new Schema[]{STRING, INT},
            true
        },
        new Object[]{
            "shouldFindTwoSameArgs",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, STRING)
            },
            new Schema[]{STRING, STRING},
            true
        },
        new Object[]{
            "shouldFindOneArgConflict",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING),
                function("other", false, INT)
            },
            new Schema[]{STRING},
            true
        },
        new Object[]{
            "shouldFindTwoArgSameFirstConflict",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, STRING),
                function("other", false, STRING, INT)
            },
            new Schema[]{STRING, STRING},
            true
        },
        new Object[]{
            "shouldChooseCorrectStruct",
            new KsqlFunction[]{
                function("other", false, STRUCT2),
                function(EXPECTED, false, STRUCT1),
            },
            new Schema[]{STRUCT1},
            true
        },
        new Object[]{
            "shouldChooseCorrectMap",
            new KsqlFunction[]{
                function("other", false, MAP2),
                function(EXPECTED, false, MAP1),
            },
            new Schema[]{MAP1},
            true
        },

        // vararg tests
        new Object[]{
            "shouldFindVarargsEmpty",
            new KsqlFunction[]{
                function(EXPECTED, true, STRING_VARARGS)
            },
            new Schema[]{},
            true
        },
        new Object[]{
            "shouldFindVarargsOne",
            new KsqlFunction[]{
                function(EXPECTED, true, STRING_VARARGS)
            },
            new Schema[]{STRING},
            true
        },
        new Object[]{
            "shouldFindVarargsTwo",
            new KsqlFunction[]{
                function(EXPECTED, true, STRING_VARARGS)
            },
            new Schema[]{STRING, STRING},
            true
        },
        new Object[]{
            "shouldFindVarargWithStruct",
            new KsqlFunction[]{
                function(EXPECTED, true, SchemaBuilder.array(STRUCT1).build()),
            },
            new Schema[]{STRUCT1, STRUCT1},
            true
        },
        new Object[]{
            "shouldFindVarargWithList",
            new KsqlFunction[]{
                function(EXPECTED, true, STRING_VARARGS),
            },
            new Schema[]{STRING_VARARGS},
            true
        },

        // precedence tests
        new Object[]{
            "shouldChooseSpecificOverVarArgs",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING),
                function("other", true, STRING_VARARGS)
            },
            new Schema[]{STRING},
            true
        },
        new Object[]{
            "shouldChooseSpecificOverMultipleVarArgs",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING),
                function("other", true, STRING_VARARGS),
                function("other1", true, STRING, STRING_VARARGS)
            },
            new Schema[]{STRING},
            true
        },
        new Object[]{
            "shouldChooseVarArgsIfSpecificDoesntMatch",
            new KsqlFunction[]{
                function("other", false, STRING),
                function(EXPECTED, true, STRING_VARARGS)
            },
            new Schema[]{STRING, STRING},
            true
        },

        // null value tests
        new Object[]{
            "shouldFindNonVarargWithNullValues",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING),
            },
            new Schema[]{null},
            true
        },
        new Object[]{
            "shouldFindNonVarargWithPartialNullValues",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, STRING),
            },
            new Schema[]{null, STRING},
            true
        },
        new Object[]{
            "shouldChooseFirstAddedWithNullValues",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING),
                function("other", false, INT),
            },
            new Schema[]{null},
            true
        },
        new Object[]{
            "shouldFindVarargWithNullValues",
            new KsqlFunction[]{
                function(EXPECTED, true, STRING_VARARGS),
            },
            new Schema[]{null},
            true
        },
        new Object[]{
            "shouldFindVarargWithSomeNullValues",
            new KsqlFunction[]{
                function(EXPECTED, true, STRING_VARARGS),
            },
            new Schema[]{null, STRING, null},
            true
        },
        new Object[]{
            "shouldChooseNonVarargWithNullValues",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING),
                function("other", true, STRING_VARARGS)
            },
            new Schema[]{null},
            true
        },
        new Object[]{
            "shouldChooseNonVarargWithNullValuesOfDifferingSchemas",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, INT),
                function("other", true, STRING_VARARGS),
            },
            new Schema[]{null, null},
            true
        },
        new Object[]{
            "shouldChooseNonVarargWithNullValuesOfSameSchemas",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, STRING),
                function("other", true, STRING_VARARGS),
            },
            new Schema[]{null, null},
            true
        },
        new Object[]{
            "shouldChooseNonVarargWithNullValuesOfPartialNulls",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, INT),
                function("other", true, STRING_VARARGS),
            },
            new Schema[]{STRING, null},
            true
        },
        new Object[]{
            "shouldChooseCorrectlyInComplicatedTopology",
            new KsqlFunction[]{
                function(EXPECTED, false, STRING, INT, STRING, INT),
                function("one", true, STRING_VARARGS),
                function("two", true, STRING, STRING_VARARGS),
                function("three", true, STRING, INT, STRING_VARARGS),
                function("four", true, STRING, INT, STRING, INT, STRING_VARARGS),
                function("five", true, INT , INT, STRING, INT, STRING_VARARGS)
            },
            new Schema[]{STRING, INT, null, INT},
            true
        },

        // failure tests
        new Object[]{
            "shouldNotMatchIfParamLengthDiffers",
            new KsqlFunction[]{
                function("one", false, STRING)
            },
            new Schema[]{STRING, STRING},
            false
        },
        new Object[]{
            "shouldNotMatchIfNoneFound",
            new KsqlFunction[]{
                function("one", false, STRING)
            },
            new Schema[]{INT},
            false
        },
        new Object[]{
            "shouldNotMatchIfNullAndPrimitive",
            new KsqlFunction[]{
                function("one", false, Schema.INT32_SCHEMA)
            },
            new Schema[]{null},
            false
        },
        new Object[]{
            "shouldNotMatchIfNullAndPrimitiveVararg",
            new KsqlFunction[]{
                function("one", true, SchemaBuilder.array(Schema.INT32_SCHEMA))
            },
            new Schema[]{null},
            false
        },
        new Object[]{
            "shouldNotMatchIfNoneFoundWithNull",
            new KsqlFunction[]{
                function("one", false, STRING, INT)
            },
            new Schema[]{INT, null},
            false
        },
        new Object[]{
            "shouldNotChooseSpecificWhenTrickyVarArgLoop",
            new KsqlFunction[]{
                function("one", false, STRING, INT),
                function("two", true, STRING_VARARGS)
            },
            new Schema[]{STRING, INT, STRING},
            false
        },
        new Object[]{
            "shouldNotMatchWhenNullTypeInArgsIfParamLengthDiffers",
            new KsqlFunction[]{
                function("one", false, STRING)
            },
            new Schema[]{STRING, null},
            false
        },
        new Object[]{
            "shouldNotMatchVarargDifferentStructs",
            new KsqlFunction[]{
                function("one", true, SchemaBuilder.array(STRUCT1).build()),
            },
            new Schema[]{STRUCT1, STRUCT2},
            false
        }
    );
  }

  @Test
  public void test() {
    // Given:
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    if (!shouldFind) {
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage("Function 'name' does not accept parameters");
    }

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(parameter));

    // Then:
    if (shouldFind) {
      assertThat(fun.getFunctionName(), equalTo(EXPECTED));
    }
  }

  private static KsqlFunction function(
      final String name,
      final boolean isVarArgs,
      final Schema... args
  ) {
    return KsqlFunction.createBuiltInVarargs(
        Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(args),
        name,
        MyUdf.class,
        isVarArgs
    );
  }

  private static final class MyUdf implements Kudf {
    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }


}