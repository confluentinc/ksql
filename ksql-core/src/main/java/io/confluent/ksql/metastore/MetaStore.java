package io.confluent.ksql.metastore;

import java.util.Map;

public interface MetaStore {
    public DataSource getSource(String sourceName);
    public void putSource(DataSource dataSource);
    public void deleteSource(String sourceName);
    public Map<String, DataSource> getAllDataSources();
}
