/*=============================================================
 * �ļ�����: JdbcInfo.java
 * ��    ��: 1.0
 * ��    ��: bluewind
 * ����ʱ��: 2005-10-17
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
