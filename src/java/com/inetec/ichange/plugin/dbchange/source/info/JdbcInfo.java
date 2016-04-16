/*=============================================================
 * 文件名称: JdbcInfo.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-10-17
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
package com.inetec.ichange.plugin.dbchange.source.info;

import com.inetec.common.exception.Ex;
import com.inetec.common.config.nodes.Jdbc;


public class JdbcInfo {


    private String m_driverClass;
    private String m_dbServerVender;
    private String m_driverUrl;
    private String m_dbHost;
    private String m_dbName;
    private String m_dbUser;
    private String m_dbPassword;
    private String m_dbCharset;
    private String m_jdbcName;
    private String m_dbOwner;
    private String m_dbDesc;

    public JdbcInfo(Jdbc jdbc) throws Ex {
        m_driverClass = jdbc.getDriverClass();
        m_dbServerVender = jdbc.getDbVender();
        m_driverUrl = jdbc.getDbUrl();
        m_dbHost = jdbc.getDbHost();
        m_dbName = jdbc.getJdbcName();
        m_dbUser = jdbc.getDbUser();
        m_dbPassword = jdbc.getPassword();
        m_dbCharset = jdbc.getEncoding();
        m_jdbcName = jdbc.getJdbcName();
        m_dbOwner = jdbc.getDbOwner();
        m_dbDesc = jdbc.getDescription();
    }


    public JdbcInfo(String driverClass, String dbServerVender, String driverUrl,
                    String dbHost, String dbName, String dbUser, String dbPassword, String charset, String jdbcName) {
        m_driverClass = driverClass;
        m_dbServerVender = dbServerVender;
        m_driverUrl = driverUrl;
        m_dbHost = dbHost;
        m_dbName = dbName;
        m_dbUser = dbUser;
        m_dbPassword = dbPassword;
        m_dbCharset = charset;
        m_jdbcName = jdbcName;
    }

    public String getDriverClass() {
        return m_driverClass;
    }

    public String getDbServerVender() {
        return m_dbServerVender;
    }

    public String getDriverUrl() {
        return m_driverUrl;
    }

    public String getDbHost() {
        return m_dbHost;
    }

    public String getDbName() {
        return m_dbName;
    }

    public String getDbUser() {
        return m_dbUser;
    }

    public String getDbPassword() {
        return m_dbPassword;
    }

    public String getDbCharset() {
        return m_dbCharset;
    }

    public String getJdbcName() {
        return m_jdbcName;
    }

    public String getDbOwner() {
        return m_dbOwner;
    }

    public String getDesc() {
        return m_dbDesc;
    }
}
