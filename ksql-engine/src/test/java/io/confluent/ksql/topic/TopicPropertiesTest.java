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

import static io.confluent.ksql.topic.Inject.KSQL_CONFIG;
import static io.confluent.ksql.topic.Inject.KSQL_CONFIG_P;
import static io.confluent.ksql.topic.Inject.KSQL_CONFIG_R;
import static io.confluent.ksql.topic.Inject.NO_CONFIG;
import static io.confluent.ksql.topic.Inject.NO_OVERRIDES;
import static io.confluent.ksql.topic.Inject.NO_WITH;
import static io.confluent.ksql.topic.Inject.OVERRIDES;
import static io.confluent.ksql.topic.Inject.OVERRIDES_P;
import static io.confluent.ksql.topic.Inject.OVERRIDES_R;
import static io.confluent.ksql.topic.Inject.SOURCE;
import static io.confluent.ksql.topic.Inject.WITH;
import static io.confluent.ksql.topic.Inject.WITH_P;
import static io.confluent.ksql.topic.Inject.WITH_R;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
          .withLegacyKsqlConfig(config)
          .build();

      // Then:
      assertThat(properties.topicName, equalTo("name"));
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
          .withLegacyKsqlConfig(config)
          .build();

      // Then:
      assertThat(properties.topicName, equalTo("name"));
    }

    @Test
    public void shouldUseNameIfNoWIthClause() {
      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withLegacyKsqlConfig(config)
          .build();

      // Then:
      assertThat(properties.topicName, equalTo("name"));
    }

    @Test
    public void shouldFailIfNoNameSupplied() {
      // Expect:
      expectedException.expect(NullPointerException.class);
      expectedException.expectMessage("Was not supplied with any valid source for topic name!");

      // When:
      new TopicProperties.Builder()
          .withLegacyKsqlConfig(config)
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
          .withLegacyKsqlConfig(config)
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
          .withLegacyKsqlConfig(config)
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
          .withLegacyKsqlConfig(config)
          .build();
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
          .withLegacyKsqlConfig(ksqlConfig)
          .withSource(source(SOURCE))
          .build();

      // Then:
      assertThat(properties.partitions, equalTo(expectedPartitions.partitions));
      assertThat(properties.replicas, equalTo(expectedReplicas.replicas));
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

}
