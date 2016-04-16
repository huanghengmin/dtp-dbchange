package com.inetec.ichange.plugin.dbchange.source.twoway;

import com.inetec.ichange.plugin.dbchange.datautils.db.Operator;

import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;

import org.apache.log4j.Category;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class SourceObject {

    protected Category m_logger = Category.getInstance(SourceObject.class);


    private String dbName;
    private String tableName;
    private Operator operator = Operator.OPERATOR__INSERT;
    private HashMap pkList = new HashMap();
    private long now;


    public SourceObject(String dbName, String tableName, Operator act) {
        this.dbName = dbName.toUpperCase();
        this.tableName = tableName.toUpperCase();
        this.operator = act;
        now = System.currentTimeMillis();
    }

    public long getTime() {
        return now;
    }

    public void addPk(String name, Object value) {
        pkList.put(name.toUpperCase(), value);
    }

    public int getPkSize() {
        return pkList.size();
    }

    public boolean isDeleteAction() {
        return operator.isDeleteAction();
    }


    public boolean equals(Object o) {

        if (this == o) return true;
        if (!(o instanceof SourceObject)) return false;

        final SourceObject sourceObject = (SourceObject) o;

        if (!dbName.equals(sourceObject.dbName)) return false;
        if (!tableName.equals(sourceObject.tableName)) return false;
        if(!operator.toString().equals(sourceObject.operator.toString())) return false;
        final HashMap pkList2 = sourceObject.pkList;
        if (pkList.size() != pkList2.size()) return false;
        Set keySet = pkList2.keySet();
        Iterator it = keySet.iterator();
        while (it.hasNext()) {
            String fieldName2 = (String) it.next();
            Object value2 = pkList2.get(fieldName2);
            Object value1 = pkList.get(fieldName2);

            if (value1 == null) {
                if (value2 != null) {
                    return false;
                } // else {}
            } else {
                if (!value1.equals(value2)) {
                    return false;
                } // else {}
            }

        }

        return true;
    }

    public int hashCode() {
        int result;
        result = dbName.hashCode();
        result = 29 * result + tableName.hashCode();
        Set keySet = pkList.keySet();
        Iterator it = keySet.iterator();
        while (it.hasNext()) {
            String fieldName = (String) it.next();
            Object value = pkList.get(fieldName);
            if (value == null) {
                continue;
            }
            result = 29 * result + fieldName.hashCode() + value.hashCode();
        }

        return result;
    }
}
