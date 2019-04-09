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

package io.confluent.ksql.topic;

import static io.confluent.ksql.topic.TopicPropertiesTest.Inject.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class TopicPropertiesTest {

  public static class Tests {

    public @Rule ExpectedException expectedException = ExpectedException.none();

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 1,
        KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 1
    ));

    @Test
    public void shouldUseNameFromWithClause() {
      // Given:
      final Map<String, Expression> withClause = ImmutableMap.of(
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("name")
      );

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withWithClause(withClause)
//          //.withKsqlConfig(config)
          .build();

      // Then:
      assertThat(properties.getTopicName(), equalTo("name"));
    }

    @Test
    public void shouldUseNameFromWithClauseWhenNameIsAlsoPresent() {
      // Given:
      final Map<String, Expression> withClause = ImmutableMap.of(
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("name")
      );

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("oh no!")
          .withWithClause(withClause)
//          //.withKsqlConfig(config)
          .build();

      // Then:
      assertThat(properties.getTopicName(), equalTo("name"));
    }

    @Test
    public void shouldUseNameIfNoWIthClause() {
      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
//          //.withKsqlConfig(config)
          .build();

      // Then:
      assertThat(properties.getTopicName(), equalTo("name"));
    }

    @Test
    public void shouldFailIfNoNameSupplied() {
      // Expect:
      expectedException.expect(NullPointerException.class);
      expectedException.expectMessage("Was not supplied with any valid source for topic name!");

      // When:
      new TopicProperties.Builder()
//          //.withKsqlConfig(config)
          .build();
    }

    @Test
    public void shouldFailIfEmptyNameSupplied() {
      // Expect:
      expectedException.expect(KsqlException.class);
      expectedException.expectMessage("Must have non-empty topic name.");

      // When:
      new TopicProperties.Builder()
          .withName("")
          ////.withKsqlConfig(config)
          .build();
    }

    @Test
    public void shouldFailIfNoPartitionsSupplied() {
      // Given:
      final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
          KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 1
      ));

      // Expect:
      expectedException.expect(NullPointerException.class);
      expectedException.expectMessage("Was not supplied with any valid source for partitions!");

      // When:
      new TopicProperties.Builder()
          .withName("name")
          //.withKsqlConfig(config)
          .build();
    }

    @Test
    public void shouldFailIfNoReplicasSupplied() {
      // Given:
      final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
          KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 1
      ));

      // Expect:
      expectedException.expect(NullPointerException.class);
      expectedException.expectMessage("Was not supplied with any valid source for replicas!");

      // When:
      new TopicProperties.Builder()
          .withName("name")
          //.withKsqlConfig(config)
          .build();
    }

    @Test
    public void shouldNotMakeRemoteCallIfUnnecessary() {
      // Given:
      final Map<String, Expression> withClause = ImmutableMap.of(
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("name"),
          KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(1),
          KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(1)
      );

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withWithClause(withClause)
          //.withKsqlConfig(config)
          .withSource(() -> {
            throw new RuntimeException();
          })
          .build();

      // Then:
      assertThat(properties.getPartitions(), equalTo(1));
      assertThat(properties.getReplicas(), equalTo((short) 1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotMakeMultipleRemoteCalls() {
      // Given:
      final Supplier<TopicDescription> source = mock(Supplier.class);
      when(source.get())
          .thenReturn(
              new TopicDescription(
                  "",
                  false,
                  ImmutableList.of(
                      new TopicPartitionInfo(
                          0,
                          null,
                          ImmutableList.of(new Node(1, "", 1)),
                          ImmutableList.of()))))
          .thenThrow();

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withSource(source)
          .build();

      // Then:
      assertThat(properties.getPartitions(), equalTo(1));
      assertThat(properties.getReplicas(), equalTo((short) 1));
    }

  }

  @RunWith(Parameterized.class)
  public static class PartitionsAndReplicasPrecedence {

    @Parameters(name = "given {0} -> expect({2} partitions, {3} replicas)")
    public static Iterable<Object[]> data() {
      final Object[][] data = new Object[][]{
          // THIS LIST WAS GENERATED BY RUNNING Inject#main
          //
          // Given:     Overrides                        Expect: [Partitions,     Replicas       ]
          {new Inject[]{WITH, OVERRIDES, KSQL_CONFIG          }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES, KSQL_CONFIG_P        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES, KSQL_CONFIG_R        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES, NO_CONFIG            }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, KSQL_CONFIG        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, KSQL_CONFIG_P      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, KSQL_CONFIG_R      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, NO_CONFIG          }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, KSQL_CONFIG        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, KSQL_CONFIG_P      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, KSQL_CONFIG_R      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, NO_CONFIG          }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, KSQL_CONFIG       }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, KSQL_CONFIG_P     }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, KSQL_CONFIG_R     }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, NO_CONFIG         }, WITH           , WITH           },
          {new Inject[]{WITH_P, OVERRIDES, KSQL_CONFIG        }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES, KSQL_CONFIG_P      }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES, KSQL_CONFIG_R      }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES, NO_CONFIG          }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES_P, KSQL_CONFIG      }, WITH_P         , KSQL_CONFIG    },
          {new Inject[]{WITH_P, OVERRIDES_P, KSQL_CONFIG_P    }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_P, OVERRIDES_P, KSQL_CONFIG_R    }, WITH_P         , KSQL_CONFIG_R  },
          {new Inject[]{WITH_P, OVERRIDES_P, NO_CONFIG        }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_P, OVERRIDES_R, KSQL_CONFIG      }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, OVERRIDES_R, KSQL_CONFIG_P    }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, OVERRIDES_R, KSQL_CONFIG_R    }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, OVERRIDES_R, NO_CONFIG        }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, NO_OVERRIDES, KSQL_CONFIG     }, WITH_P         , KSQL_CONFIG    },
          {new Inject[]{WITH_P, NO_OVERRIDES, KSQL_CONFIG_P   }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_P, NO_OVERRIDES, KSQL_CONFIG_R   }, WITH_P         , KSQL_CONFIG_R  },
          {new Inject[]{WITH_P, NO_OVERRIDES, NO_CONFIG       }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_R, OVERRIDES, KSQL_CONFIG        }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES, KSQL_CONFIG_P      }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES, KSQL_CONFIG_R      }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES, NO_CONFIG          }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, KSQL_CONFIG      }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, KSQL_CONFIG_P    }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, KSQL_CONFIG_R    }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, NO_CONFIG        }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, KSQL_CONFIG      }, KSQL_CONFIG    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, KSQL_CONFIG_P    }, KSQL_CONFIG_P  , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, KSQL_CONFIG_R    }, SOURCE         , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, NO_CONFIG        }, SOURCE         , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, KSQL_CONFIG     }, KSQL_CONFIG    , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, KSQL_CONFIG_P   }, KSQL_CONFIG_P  , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, KSQL_CONFIG_R   }, SOURCE         , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, NO_CONFIG       }, SOURCE         , WITH_R         },
          {new Inject[]{NO_WITH, OVERRIDES, KSQL_CONFIG       }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES, KSQL_CONFIG_P     }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES, KSQL_CONFIG_R     }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES, NO_CONFIG         }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES_P, KSQL_CONFIG     }, OVERRIDES_P    , KSQL_CONFIG    },
          {new Inject[]{NO_WITH, OVERRIDES_P, KSQL_CONFIG_P   }, OVERRIDES_P    , SOURCE         },
          {new Inject[]{NO_WITH, OVERRIDES_P, KSQL_CONFIG_R   }, OVERRIDES_P    , KSQL_CONFIG_R  },
          {new Inject[]{NO_WITH, OVERRIDES_P, NO_CONFIG       }, OVERRIDES_P    , SOURCE         },
          {new Inject[]{NO_WITH, OVERRIDES_R, KSQL_CONFIG     }, KSQL_CONFIG    , OVERRIDES_R    },
          {new Inject[]{NO_WITH, OVERRIDES_R, KSQL_CONFIG_P   }, KSQL_CONFIG_P  , OVERRIDES_R    },
          {new Inject[]{NO_WITH, OVERRIDES_R, KSQL_CONFIG_R   }, SOURCE         , OVERRIDES_R    },
          {new Inject[]{NO_WITH, OVERRIDES_R, NO_CONFIG       }, SOURCE         , OVERRIDES_R    },
          {new Inject[]{NO_WITH, NO_OVERRIDES, KSQL_CONFIG    }, KSQL_CONFIG    , KSQL_CONFIG    },
          {new Inject[]{NO_WITH, NO_OVERRIDES, KSQL_CONFIG_P  }, KSQL_CONFIG_P  , SOURCE         },
          {new Inject[]{NO_WITH, NO_OVERRIDES, KSQL_CONFIG_R  }, SOURCE         , KSQL_CONFIG_R  },
          {new Inject[]{NO_WITH, NO_OVERRIDES, NO_CONFIG      }, SOURCE         , SOURCE         },
      };

      // generate the description from the given injections and put it at the beginning
      return Lists.newArrayList(data)
          .stream()
          .map(params -> Lists.asList(
              Arrays.stream((Inject[]) params[0])
                  .map(Objects::toString)
                  .collect(Collectors.joining(", ")),
              params))
          .map(List::toArray)
          .collect(Collectors.toList());
    }

    @Parameter
    public String description;

    @Parameter(1)
    public Inject[] injects;

    @Parameter(2)
    public Inject expectedPartitions;

    @Parameter(3)
    public Inject expectedReplicas;

    private KsqlConfig ksqlConfig = new KsqlConfig(new HashMap<>());
    private Map<String, Object> propertyOverrides = new HashMap<>();
    private Map<String, Expression> withClause = new HashMap<>();

    @Test
    public void shouldInferCorrectPartitionsAndReplicas() {
      // Given:
      Arrays.stream(injects).forEach(this::givenInject);

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withWithClause(withClause)
          .withOverrides(propertyOverrides)
//          .withKsqlConfig(ksqlConfig)
          .withSource(() -> source(SOURCE))
          .build();

      // Then:
      assertThat(properties.getPartitions(), equalTo(expectedPartitions.partitions));
      assertThat(properties.getReplicas(), equalTo(expectedReplicas.replicas));
    }

    private void givenInject(final Inject inject) {
      switch (inject.type) {
        case WITH:
          if (inject.partitions != null) {
            withClause.put(
                KsqlConstants.SINK_NUMBER_OF_PARTITIONS,
                new IntegerLiteral(inject.partitions));
          }
          if (inject.replicas != null) {
            withClause.put(
                KsqlConstants.SINK_NUMBER_OF_REPLICAS,
                new IntegerLiteral(inject.replicas));
          }
          break;
        case OVERRIDES:
          if (inject.partitions != null) {
            propertyOverrides.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, inject.partitions);
          }
          if (inject.replicas != null) {
            propertyOverrides.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, inject.replicas);
          }
          break;
        case KSQL_CONFIG:
          final Map<String, Object> cfg = new HashMap<>();
          if (inject.partitions != null) {
            cfg.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, inject.partitions);
          }
          if (inject.replicas != null) {
            cfg.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, inject.replicas);
          }
          ksqlConfig = new KsqlConfig(cfg);
          break;
        case SOURCE:
        default:
          throw new IllegalArgumentException(inject.type.toString());
      }
    }

    public TopicDescription source(final Inject source) {
      return new TopicDescription(
          "source",
          false,
          Collections.nCopies(source.partitions,
              new TopicPartitionInfo(
                  0,
                  null,
                  Collections.nCopies(source.replicas, new Node(0, "", 0)),
                  ImmutableList.of())
          )
      );
    }
  }

  enum Inject {
    SOURCE(Type.SOURCE, 1, (short) 1),
    SOURCE2(Type.SOURCE, 12, (short) 12),

    WITH(Type.WITH, 2, (short) 2),
    OVERRIDES(Type.OVERRIDES, 3, (short) 3),
    KSQL_CONFIG(Type.KSQL_CONFIG, 4, (short) 4),

    WITH_P(Type.WITH, 5, null),
    OVERRIDES_P(Type.OVERRIDES, 6, null),
    KSQL_CONFIG_P(Type.KSQL_CONFIG, 7, null),

    WITH_R(Type.WITH, null, (short) 8),
    OVERRIDES_R(Type.OVERRIDES, null, (short) 9),
    KSQL_CONFIG_R(Type.KSQL_CONFIG, null, (short) 10),

    NO_WITH(Type.WITH, null, null),
    NO_OVERRIDES(Type.OVERRIDES, null, null),
    NO_CONFIG(Type.KSQL_CONFIG, null, null)
    ;

    final Type type;
    final Integer partitions;
    final Short replicas;

    Inject(final Type type, final Integer partitions, final Short replicas) {
      this.type = type;
      this.partitions = partitions;
      this.replicas = replicas;
    }

    enum Type {
      WITH,
      OVERRIDES,
      KSQL_CONFIG,
      SOURCE
    }

    /**
     * Generates code for all combinations of Injects
     */
    public static void main(String[] args) {
      final List<Inject> withs = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.WITH).collect(Collectors.toList());
      final List<Inject> overrides = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.OVERRIDES).collect(Collectors.toList());
      final List<Inject> ksqlConfigs = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.KSQL_CONFIG).collect(Collectors.toList());

      for (List<Inject> injects : Lists.cartesianProduct(withs, overrides, ksqlConfigs)) {
        // sort by precedence order
        injects = new ArrayList<>(injects);
        injects.sort(Comparator.comparing(i -> i.type));

        final Inject expectedPartitions =
            injects.stream().filter(i -> i.partitions != null).findFirst().orElse(Inject.SOURCE);
        final Inject expectedReplicas =
            injects.stream().filter(i -> i.replicas != null).findFirst().orElse(Inject.SOURCE);

        System.out.println(String.format("{new Inject[]{%-38s}, %-15s, %-15s},",
            injects.stream().map(Objects::toString).collect(Collectors.joining(", ")),
            expectedPartitions,
            expectedReplicas)
        );
      }
    }
  }
}
