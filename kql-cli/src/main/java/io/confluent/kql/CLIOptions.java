package io.confluent.kql;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CLIOptions {

  private static final String STREAMS_PROPERTIES_FILE_OPTION_NAME = "--properties-file";
  private static final String QUERY_FILE_OPTION_NAME = "--query-file";
  private static final String CATALOG_FILE_OPTION_NAME = "--catalog-file";
  private static final String QUERIES_OPTION_NAME = "--queries";
  private static final String QUERY_TIME_OPTION_NAME = "--query-time";

  @Parameter
  private List<String> parameters = new ArrayList<>();
  public List<String> getParameters() {
    return parameters;
  }

  @Parameter(
      names = STREAMS_PROPERTIES_FILE_OPTION_NAME,
      required = true,
      converter = FileConverter.class,
      description = "A file specifying properties for KQL and its underlying Kafka Streams instance(s)"
  )
  private File streamsPropertiesFile;
  public File getStreamsPropertiesFile() {
    return streamsPropertiesFile;
  }

  @Parameter(
      names = QUERY_FILE_OPTION_NAME,
      validateWith = QueryMutualExclusionValidator.class,
      converter = FileConverter.class,
      description = "A file to run non-interactive queries from"
  )
  private File queryFile;
  public File getQueryFile() {
    return queryFile;
  }

  @Parameter(
      names = CATALOG_FILE_OPTION_NAME,
      converter = FileConverter.class,
      description = "A file to import metastore data from before execution"
  )
  private File catalogFile;
  public File getCatalogFile() {
    return catalogFile;
  }

  @Parameter(
      names = QUERIES_OPTION_NAME,
      validateWith = QueryMutualExclusionValidator.class,
      description = "One or more non-interactive queries to run"
  )
  private String queries;
  public String getQueries() {
    return queries;
  }

  @Parameter(
      names = QUERY_TIME_OPTION_NAME,
      description = "How long to run non-interactive queries for (ms)"
  )
  private Long queryTime;
  public Long getQueryTime() {
    return queryTime;
  }

  private CLIOptions() {}

  public static CLIOptions parse(String[] ass) {
    CLIOptions result = new CLIOptions();
    JCommander jCommander = new JCommander(result);
    jCommander.setProgramName("KQL");
    try {
      jCommander.parse(ass);
      return result;
    } catch (ParameterException exception) {
      jCommander.usage();
      return null;
    }
  }

  public static Map<String, Object> getPropertiesMap(File propertiesFile) throws IOException {
    Map<String, Object> result = new HashMap<>();

    Properties properties = new Properties();
    properties.load(new FileInputStream(propertiesFile));

    for (Map.Entry<Object, Object> propertyEntry : properties.entrySet()) {
      result.put((String) propertyEntry.getKey(), propertyEntry.getValue());
    }

    return result;
  }

  private class QueryMutualExclusionValidator implements IParameterValidator {
    public void validate(String name, String value) throws ParameterException {
      if (QUERIES_OPTION_NAME.equals(name)) {
        queryFile = null;
      } else if (QUERY_FILE_OPTION_NAME.equals(name)) {
        queries = null;
      }
    }
  }

  /*
    QUERY_FILE_PATH_CONFIG = "kql.query.file";
    CATALOG_FILE_PATH_CONFIG = "kql.catalog.file";
    PROP_FILE_PATH_CONFIG = "kql.properties.file";

    QUERY_CONTENT_CONFIG = "query";
    QUERY_EXECUTION_TIME_CONFIG = "terminate.in";

    DEFAULT_QUERY_FILE_PATH_CONFIG = "cli";
    DEFAULT_SCHEMA_FILE_PATH_CONFIG = "NULL";
    DEFAULT_PROP_FILE_PATH_CONFIG = "";

    DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    DEFAULT_AUTO_OFFSET_RESET_CONFIG = "earliest";

    AVRO_SERDE_SCHEMA_DIRECTORY_CONFIG = "avro.serde.schema";

    AVRO_SCHEMA_FOLDER_PATH_CONFIG = "/tmp/";
    DEFAULT_AVRO_SCHEMA_FOLDER_PATH_CONFIG = "/tmp/";
   */
}
