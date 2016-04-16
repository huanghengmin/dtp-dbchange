/*=============================================================
 * �ļ�����: Row.java
 * ��    ��: 1.0
 * ��    ��: bluewind
 * ����ʱ��: 2005-11-12
 * ============================================================
 * <p>��Ȩ����  (c) 2005 ����������Ϣ�������޹�˾</p>
 * <p>
 * ��Դ���ļ���Ϊ����������Ϣ�������޹�˾���������һ���֣�������
 * �˱���˾�Ļ��ܺ�����Ȩ��Ϣ����ֻ�ṩ������˾���������û�ʹ�á�
 * </p>
 * <p>
 * ���ڱ����ʹ�ã��������ر�������˵���������������涨�����޺�
 * ������
 * </p>
 * <p>
 * �ر���Ҫָ�����ǣ������Դӱ���˾��������߸�����Ĳ��������ߺ�
 * ����ȡ�ò���Ȩʹ�ñ����򡣵��ǲ��ý��и��ƻ���ɢ�����ļ���Ҳ��
 * ��δ������˾����޸�ʹ�ñ��ļ������߽��л��ڱ�����Ŀ���������
 * ���ǽ������ķ����޶��ڶ����ַ�����˾��Ȩ����Ϊ�������ߡ�
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
