/*=============================================================
 * �ļ�����: Column.java
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
import com.inetec.common.exception.Ex;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;


public class Column {

    protected String name;
    protected int jdbcType;
    protected String dbType;
    protected boolean ispk = false;

    protected Value value = null;

    public Column(String name, String dbType, boolean ispk) {
        this.name = name;
        this.dbType = dbType;
        this.ispk = ispk;
    }

    public Column(String name, int jdbcType, String dbType, boolean ispk) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.dbType = dbType;
        this.ispk = ispk;
    }

    public Column copyColumnWithoutValue() {
        return new Column(this.name, this.jdbcType, this.dbType, this.ispk);
    }

    public String getName() {
        return name;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getJdbcTypeString() {
        return DbopUtil.getJdbcTypeString(jdbcType);
    }

    public void setJdbcType(int type) {
        this.jdbcType = type;
    }

    public String getDbType() {
        return dbType;
    }

    public boolean isPk() {
        return ispk;
    }

    public boolean isNull() {
        return value == null || (value != null && value.isNull());
    }

    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }


    public boolean isLobType() {
        return DbopUtil.isLobType(jdbcType);
    }

    public boolean isBlobType() {
        return DbopUtil.isBlobType(jdbcType);
    }

    public boolean isClobType() {
        return DbopUtil.isClobType(jdbcType);
    }


    public Object getObject() {
        if (value.getType() == Value.Int_Value_Basic) {
            return DbopUtil.getObjectFromString(jdbcType, value.getValueString());
        } else {
            return null;
        }
    }

    public Element toElement(Document doc) throws Ex {
        Element item = doc.createElement("field");
        item.setAttribute("name", name);
        item.setAttribute("jdbctype", getJdbcTypeString());
        item.setAttribute("dbtype", getDbType());
        if (ispk) {
            item.setAttribute("ispk", "true");
        } else {
            item.setAttribute("ispk", "false");
        }

        if (isLobType()) {
            // not used in Exml parser
            if (isBlobType()) {
                item.setAttribute("blob", "true");
            } else if (isClobType()) {
                item.setAttribute("clob", "true");
            }
        }


        if (isNull()) {
            item.setAttribute("isnull", "true");
        } else {
            item.setAttribute("isnull", "false");
            if (value.getType() == Value.Int_Value_Basic) {
                item.appendChild(doc.createCDATASection(value.getValueString()));
            }
        }


        return item;
    }

}
