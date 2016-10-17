package io.confluent.ksql.metastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaStoreImpl implements MetaStore {

    Map<String, DataSource> dataSourceMap = new HashMap<>();

    @Override
    public DataSource getSource(String sourceName) {
        return dataSourceMap.get(sourceName);
    }

    @Override
    public void putSource(DataSource dataSource) {
        dataSourceMap.put(dataSource.getName(), dataSource);
    }

    @Override
    public Map<String, DataSource> getAllDataSources() {
        return dataSourceMap;
    }
}
