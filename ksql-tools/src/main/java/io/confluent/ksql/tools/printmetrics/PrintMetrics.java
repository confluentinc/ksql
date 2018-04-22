/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.tools.printmetrics;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class PrintMetrics {

  public static void printHelp() {
    System.err.println(
        "usage: PrintMetrics [help] port=<KSQL JMX Port>\n"
            + "\n"
            + "This utility prints the following operational metrics tracked by ksql:\n"
            + "\n"
            + "messages-consumed-per-sec: Messages consumed per second across all queries\n"
            + "messages-consumed-avg:     The average number of messages consumed by a query "
            + "per second\n"
            + "messages-consumed-min:     Messages consumed per second for the query with the "
            + "fewest messages consumed per second\n"
            + "messages-consumed-max:     Messages consumed per second for the query with the "
            + "most messages consumed per second\n"
            + "messages-produced-per-sec: Messages produced per second across all queries\n"
            + "error-rate:                The number of messages which were consumed but not "
            + " processed across all queries\n"
            + "num-persistent-queries:    The number of queries currently executing.\n"
            + "num-active-queries:        The number of queries actively processing messages.\n"
            + "num-idle-queries:          The number of queries with no messages available to "
            + "process.\n");
    System.err.println(
        "To use this tool, when running ksql-server-start you must set the JMX_PORT "
            + "environmnent variable to an open port for the JMX service to listen on.");
  }

  private static void printMetrics(int port) throws IOException {
    JMXServiceURL jmxUrl = new JMXServiceURL(
        String.format("service:jmx:rmi:///jndi/rmi://localhost:%d/jmxrmi", port));
    JMXConnector connector = JMXConnectorFactory.connect(jmxUrl);
    connector.connect();
    MBeanServerConnection connection = connector.getMBeanServerConnection();

    // print out all ksql engine metrics
    Set<ObjectName> names = connection.queryNames(null, null);
    List<ObjectName> ksqlObjects = new LinkedList<>();
    for (ObjectName n : names) {
      if (n.toString().startsWith("io.confluent.ksql.metrics:type=ksql-engine")) {
        ksqlObjects.add(n);
      }
    }
    for (ObjectName n : ksqlObjects) {
      MBeanInfo info;
      try {
        info = connection.getMBeanInfo(n);
      } catch (Exception error) {
        throw new PrintMetricsException(
            "Unexpected error getting mbean info " + error.getMessage()
        );
      }
      MBeanAttributeInfo[] attributes = info.getAttributes();
      for (MBeanAttributeInfo attributeInfo : attributes) {
        Object attribute;
        try {
          attribute = connection.getAttribute(n, attributeInfo.getName());
        } catch (Exception error) {
          throw new PrintMetricsException(
              "Unexpected error getting attribute " + error.getMessage()
          );
        }
        System.out.println(attributeInfo.getName() + ": " + attribute);
      }
    }
  }

  public static void main(String[] progArgs) throws IOException {
    Arguments args = Arguments.parse(progArgs);
    if (args.help) {
      printHelp();
      return;
    }

    printMetrics(args.port);
  }

  static class Arguments {

    public boolean help;
    public int port;

    public Arguments(boolean help, int port) {
      this.help = help;
      this.port = port;
    }

    public static Arguments parse(String[] args) {
      boolean help = false;
      int port = -1;

      for (String arg : args) {
        if ("help".equals(arg)) {
          help = true;
          continue;
        }
        String[] splitOnEquals = arg.split("=");
        if (splitOnEquals.length != 2) {
          throw new ArgumentParseException(String.format(
              "Invalid argument format in '%s'; expected <name>=<value>",
              arg
          ));
        }

        String argName = splitOnEquals[0].trim();
        String argValue = splitOnEquals[1].trim();

        if (argName.isEmpty()) {
          throw new ArgumentParseException(
              String.format("Empty argument name in %s", arg));
        }

        if (argValue.isEmpty()) {
          throw new ArgumentParseException(
              String.format("Empty argument value in '%s'", arg));
        }

        switch (argName) {
          case "port":
            port = Integer.decode(argValue);
            break;
          default: {
            throw new ArgumentParseException(
                String.format("Invalid argument value '%s'", argName));
          }
        }
      }
      return new Arguments(help, port);
    }
  }
}
