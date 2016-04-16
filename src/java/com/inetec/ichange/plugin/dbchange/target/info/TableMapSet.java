package com.inetec.ichange.plugin.dbchange.target.info;

import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class TableMapSet {

    private ArrayList listTableMap = new ArrayList();

    public void AddTableMap(TableMap map) {
        listTableMap.add(map);
    }

    public TableMap[] getTableMap() {
        return (TableMap[]) listTableMap.toArray(new TableMap[0]);
    }

    public TableMap[] findBySource(String sourceDb, String sourceTable) {
        ArrayList listValue = new ArrayList();
        int size = listTableMap.size();
        for (int i = 0; i < size; i++) {
            TableMap map = (TableMap) listTableMap.get(i);
            if (sourceDb.equalsIgnoreCase(map.getSourceDb()) &&
                    sourceTable.equalsIgnoreCase(map.getSourceTable())) {
                listValue.add(map);
            }
        }

        return (TableMap[]) listValue.toArray(new TableMap[0]);
    }

    public TableMap[] findBySourceDb(String sourceDb) {
        ArrayList listValue = new ArrayList();
        int size = listTableMap.size();
        for (int i = 0; i < size; i++) {
            TableMap map = (TableMap) listTableMap.get(i);
            if (sourceDb.equalsIgnoreCase(map.getSourceDb())) {
                listValue.add(map);
            }
        }

        return (TableMap[]) listValue.toArray(new TableMap[0]);
    }

    public TableMap[] findByTarget(String targetDb, String targetTable) {
        ArrayList listValue = new ArrayList();
        int size = listTableMap.size();
        for (int i = 0; i < size; i++) {
            TableMap map = (TableMap) listTableMap.get(i);
            if (targetDb.equalsIgnoreCase(map.getTargetDb()) &&
                    targetTable.equalsIgnoreCase(map.getTargetTable())) {
                listValue.add(map);
            }
        }

        return (TableMap[]) listValue.toArray(new TableMap[0]);
    }

    public TableMap[] findByTarget(String targetDb) {
        ArrayList listValue = new ArrayList();
        int size = listTableMap.size();
        for (int i = 0; i < size; i++) {
            TableMap map = (TableMap) listTableMap.get(i);
            if (targetDb.equalsIgnoreCase(map.getTargetDb())) {
                listValue.add(map);
            }
        }

        return (TableMap[]) listValue.toArray(new TableMap[0]);
    }

}
