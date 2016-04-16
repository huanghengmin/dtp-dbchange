package com.inetec.ichange.plugin.dbchange.source.info;

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.common.config.nodes.MergeTable;

import java.util.Map;
import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2008-10-2
 * Time: 12:22:45
 * To change this template use File | Settings | File Templates.
 */
public class MergeTableInfo {
    private String tableName;
    private String mergeTableName;
    private Map fieldMap = new HashMap();

    public String getMergeTableName() {
        return mergeTableName;
    }

    public void setMergeTableName(String mergeTableName) {
        this.mergeTableName = mergeTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFieldNameByName(String name) {
        return (String) fieldMap.get(name);
    }

    public void setFieldMap(String srcColumn, String destColumn) {
        fieldMap.put(srcColumn, destColumn);
    }

    public String[] getAllFieldKey() {
        return (String[]) fieldMap.keySet().toArray(new String[0]);
    }

    public String[] getAllFieldValue() {
        return (String[]) fieldMap.values().toArray(new String[0]);
    }

    /**
     * @param table
     * @return
     */
    public static MergeTableInfo MergeTableInfo(MergeTable table, String tablename) {
        MergeTableInfo info = new MergeTableInfo();
        info.setTableName(tablename);
        info.setMergeTableName(table.getMergeTableName());
        for (int i = 0; i < table.getAllFields().length; i++) {
            info.setFieldMap(table.getAllFields()[i].getFieldName(), table.getAllFields()[i].getMergeFieldName());
        }
        return info;
    }
}
