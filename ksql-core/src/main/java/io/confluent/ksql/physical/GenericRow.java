package io.confluent.ksql.physical;

import java.util.List;

public class GenericRow {

    public List<Object> columns;
//    public List<AbstractType> columnTypes;

    public GenericRow() {}

    public GenericRow(List<Object> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("[ ");
        int currentIndex = 0;
        for(Object obj: columns) {
            stringBuilder.append(obj);
            currentIndex++;
            if(currentIndex < columns.size()) {
                stringBuilder.append(" | ");
            }
        }
        stringBuilder.append(" ]");
        return stringBuilder.toString();
    }

    public List<Object> getColumns() {
        return columns;
    }

    public void setColumns(List<Object> columns) {
        this.columns = columns;
    }

}
