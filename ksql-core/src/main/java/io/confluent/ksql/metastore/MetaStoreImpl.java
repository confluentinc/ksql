package io.confluent.ksql.metastore;

import io.confluent.ksql.util.KSQLException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaStoreImpl implements MetaStore {

    Map<String, DataSource> dataSourceMap = new HashMap<>();

    @Override
    public DataSource getSource(String sourceName) {
        return dataSourceMap.get(sourceName.toUpperCase());
    }

    @Override
    public void putSource(DataSource dataSource) {
        if (getSource(dataSource.getName()) == null) {
            dataSourceMap.put(dataSource.getName().toUpperCase(), dataSource);
        } else {
            throw new KSQLException("Cannot add the new data source. Another data source with the same name already exists: "+dataSource.getName());
        }

    }

    @Override
    public void deleteSource(String sourceName) {
        dataSourceMap.remove(sourceName);
    }

    @Override
    public Map<String, DataSource> getAllDataSources() {
        return dataSourceMap;
    }
}
