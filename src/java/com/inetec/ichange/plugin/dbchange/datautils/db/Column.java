/*=============================================================
 * 文件名称: Column.java
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
