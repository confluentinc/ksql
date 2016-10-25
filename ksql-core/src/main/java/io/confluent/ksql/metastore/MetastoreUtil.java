package io.confluent.ksql.metastore;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MetastoreUtil {

    public DataSource createDataSource(JsonNode node) {

        String name = node.get("name").asText();
        String topicname = node.get("topicname").asText();
        String type = node.get("type").asText();
        SchemaBuilder dataSource = SchemaBuilder.struct().name(name);
        ArrayNode fields = (ArrayNode)node.get("fields");
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).get("name").textValue();
            String fieldType;
            if(fields.get(i).get("type").isArray()) {
                fieldType = fields.get(i).get("type").get(0).textValue();
            } else {
                fieldType = fields.get(i).get("type").textValue();
            }

            dataSource.field(fieldName, getKSQLType(fieldType));
        }

        return new KafkaTopic(name, dataSource, DataSource.DataSourceType.STREAM,topicname);
    }

    private Schema getKSQLType(String sqlType) {
        if (sqlType.equalsIgnoreCase("long")) {
            return Schema.INT64_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("string")) {
            return Schema.STRING_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("double")) {
            return Schema.FLOAT64_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("int") || sqlType.equalsIgnoreCase("integer")) {
            return Schema.INT32_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("bool") || sqlType.equalsIgnoreCase("boolean")) {
            return Schema.BOOLEAN_SCHEMA;
        }
        throw new KSQLException("Unsupported type: "+sqlType);
    }

    public MetaStore loadMetastoreFromJSONFile(String metastoreJsonFilePath) throws IOException {
        MetaStoreImpl metaStore = new MetaStoreImpl();
        byte[] jsonData = Files.readAllBytes(Paths.get(metastoreJsonFilePath));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode root = objectMapper.readTree(jsonData);
        ArrayNode schemaNodes = (ArrayNode)root.get("schemas");
        for (JsonNode schemaNode : schemaNodes) {
            DataSource dataSource = createDataSource(schemaNode);
            metaStore.putSource(dataSource);
        }

        return metaStore;
    }

    public static void main(String args[]) throws IOException {

//        byte[] jsonData = Files.readAllBytes(Paths.get("/Users/hojjat/userschema.json"));
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode root = objectMapper.readTree(jsonData);
//        new MetastoreUtil().createDataSource(root);
        new MetastoreUtil().loadMetastoreFromJSONFile("/Users/hojjat/userschema.json");
        System.out.println("");

    }
}
