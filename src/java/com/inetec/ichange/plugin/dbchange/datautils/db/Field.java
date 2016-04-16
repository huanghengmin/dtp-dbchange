package com.inetec.ichange.plugin.dbchange.datautils.db;

import org.w3c.dom.Element;
import org.w3c.dom.Document;
import com.inetec.common.exception.Ex;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;


public class Field {

    protected String name;
    protected int jdbcType;
    protected String dbType;
    protected boolean ispk = false;

    protected Value value = null;

    public Field(String name, String dbType, boolean ispk) {
        this.name = name;
        this.dbType = dbType;
        this.ispk = ispk;
    }

    public Field(String name, int jdbcType, String dbType, boolean ispk) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.dbType = dbType;
        this.ispk = ispk;
    }

    public Field copyColumnWithoutValue() {
        return new Field(this.name, this.jdbcType, this.dbType, this.ispk);
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
