#!/bin/bash
# This script generates a run configuration for IntelliJ IDEA
# present jar versions e.g. target/ksqldb-parent-7.7.2-0-tests.jar, target/ksqldb-parent-7.7.2-0.jar
versions=$(find target -name "*.jar" | grep -oE '\d+\.\d+\.\d+\-\d+' | sort -u)
# user can provide as 7.7 or 7.7.2 or 7.7.2-0. If not provided, then use the last version. If provided, then filter the versions and use the last one.
userProvidedVersion=$1
if [ -z "$userProvidedVersion" ]; then
  lastVersion=$(echo "$versions" | tail -1)
  version=$lastVersion
else
  version=$(echo "$versions" | grep -E "$userProvidedVersion" | tail -1)
fi

echo "Using version: $version"


defaultRunConfigPath=".idea/runConfigurations"
# check environment variable IDEA_RUN_CONFIG_PATH
if [ -z "$IDEA_RUN_CONFIG_PATH" ]; then
  echo "IDEA_RUN_CONFIG_PATH is not set. Using default path: $defaultRunConfigPath"
  runConfigPath=$defaultRunConfigPath
else
  runConfigPath=$IDEA_RUN_CONFIG_PATH
fi

defaultConfigName="ksql-server-1"
configName=${2:-$defaultConfigName}

xmlFileContent="<component name=\"ProjectRunConfigurationManager\">
  <configuration default=\"false\" name=\"${configName}\" type=\"Application\" factoryName=\"Application\">
   <option name=\"ALTERNATIVE_JRE_PATH\" value=\"temurin-17\" />
   <option name=\"ALTERNATIVE_JRE_PATH_ENABLED\" value=\"true\" />
   <envs>
     <env name=\"CLASSPATH\" value=\"\$PROJECT_DIR\$/ksqldb-examples/target/ksqldb-examples-${version}/share/java/ksqldb-examples:\$PROJECT_DIR\$/ksqldb-rest-app/target/ksqldb-rest-app-${version}/share/java/ksqldb-rest-app:\$PROJECT_DIR\$/ksqldb-cli/target/ksqldb-cli-${version}/share/java/ksqldb-cli:\$PROJECT_DIR\$/ksqldb-functional-tests/target/ksqldb-functional-tests-${version}/share/java/ksqldb-functional-tests:\$PROJECT_DIR\$/ksqldb-tools/target/ksqldb-tools-${version}/share/java/ksqldb-tools:\$PROJECT_DIR\$/ksqldb-engine/target/ksqldb-engine-${version}/share/java/ksqldb-engine\" />
   </envs>
   <option name=\"MAIN_CLASS_NAME\" value=\"io.confluent.ksql.rest.server.KsqlServerMain\" />
   <module name=\"ksqldb-rest-app\" />
   <option name=\"PROGRAM_PARAMETERS\" value=\"config/ksql-server.properties\" />
   <option name=\"VM_PARAMETERS\" value=\"-Xmx3g -server -Dlog4j.configuration=file:config/log4j-rolling.local.properties -Dksql.log.dir=logs -Dksql.server.install.dir=\$PROJECT_DIR\$\" />
   <extension name=\"coverage\">
     <pattern>
       <option name=\"PATTERN\" value=\"io.confluent.ksql.rest.server.*\" />
       <option name=\"ENABLED\" value=\"true\" />
     </pattern>
   </extension>
   <method v=\"2\">
     <option name=\"Make\" enabled=\"true\" />
   </method>
  </configuration>
</component>"

mkdir -p "$runConfigPath"
echo "$xmlFileContent" > "$runConfigPath/ksql-server-1.xml"
echo "File generated and is present at: $runConfigPath/ksql-server-1.xml"