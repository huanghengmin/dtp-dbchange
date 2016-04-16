/*=============================================================
 * 文件名称: Row.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-11-12
 * ============================================================
 * <p>版权所有  (c) 2005 杭州网科信息工程有限公司</p>
 * <p>
 * 本源码文件作为杭州网科信息工程有限公司所开发软件一部分，它包涵
 * 了本公司的机密很所有权信息，它只提供给本公司软件的许可用户使用。
 * </p>
 * <p>
 * 对于本软件使用，必须遵守本软件许可说明和限制条款所规定的期限和
 * 条件。
 * </p>
 * <p>
 * 特别需要指出的是，您可以从本公司软件，或者该软件的部件，或者合
 * 作商取得并有权使用本程序。但是不得进行复制或者散发本文件，也不
 * 得未经本公司许可修改使用本文件，或者进行基于本程序的开发，否则
 * 我们将在最大的法律限度内对您侵犯本公司版权的行为进行起诉。
 * </p>
 * ==========================================================*/
package com.inetec.ichange.plugin.dbchange.datautils.db;

import org.w3c.dom.Element;
import org.w3c.dom.Document;

import java.util.ArrayList;

import com.inetec.common.exception.Ex;

public class Row {
    private String guid;
    private String schemaName;
    private String tableName;
    private Operator operator = Operator.OPERATOR__INSERT;


    public long getOptime() {
        return op_time;
    }

    private long op_time;

    private ArrayList listColumns = new ArrayList();

    public Row(String db, String table) {
        schemaName = db;
        tableName = table;
    }


    public void setOp_time(long op_time) {
        this.op_time = op_time;
    }
    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean isDeleteAction() {
        return operator.isDeleteAction();
    }

    public boolean isInsertAction() {
        return operator.isInsertAction();
    }

    public boolean isUpdateAction() {
        return operator.isUpdateAction();
    }

    public void setAction(Operator operator) {
        this.operator = operator;
    }

    public String getId() {
        return guid;
    }

    public void addColumn(Column column) {
        listColumns.add(column);
    }

    public Column[] getColumnArray() {
        return (Column[]) listColumns.toArray(new Column[0]);
    }

    public Column[] getPkColumnArray() {
        ArrayList lists = new ArrayList();
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isPk()) {
                lists.add(c);
            }
        }

        return (Column[]) lists.toArray(new Column[0]);
    }

    public boolean hasLobType() {
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isLobType()) {
                return true;
            }
        }

        return false;
    }

    public boolean hasBlobType() {
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isBlobType()) {
                return true;
            }
        }

        return false;
    }

    public boolean hasClobType() {
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isClobType()) {
                return true;
            }
        }

        return false;
    }


    public Column[] getLobColumn() {
        ArrayList listLob = new ArrayList();
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isLobType()) {
                listLob.add(c);
            }
        }

        return (Column[]) listLob.toArray(new Column[0]);
    }

    public Column[] getClobColumn() {
        ArrayList listLob = new ArrayList();
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isClobType()) {
                listLob.add(c);
            }
        }

        return (Column[]) listLob.toArray(new Column[0]);
    }


    public Column[] getBlobColumn() {
        ArrayList listLob = new ArrayList();
        int size = listColumns.size();
        for (int i = 0; i < size; i++) {
            Column c = (Column) listColumns.get(i);
            if (c.isClobType()) {
                listLob.add(c);
            }
        }

        return (Column[]) listLob.toArray(new Column[0]);
    }

    public Element toElement(Document doc) throws Ex {
        Column[] columns = getColumnArray();
        Element root = doc.createElement("row");
        root.setAttribute("database", schemaName);
        root.setAttribute("table", tableName);
        root.setAttribute("op_time", String.valueOf(op_time));
        root.setAttribute("operator", operator.toString());
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (!column.isLobType()) {
                root.appendChild(column.toElement(doc));
            }
        }
        return root;
    }
}
