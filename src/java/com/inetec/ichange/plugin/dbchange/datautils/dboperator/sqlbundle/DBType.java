/*=============================================================
 * �ļ�����: DBType.java
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
package com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class DBType {

    public static final DBType C_MSSQL = new DBType("mssql");
    public static final DBType C_Oracle = new DBType("oracle");
    public static final DBType C_DB2 = new DBType("db2");
    public static final DBType C_SYBASE = new DBType("sybase");
    public static final DBType C_MYSQL = new DBType("mysql");
    public static final DBType C_Invalid = new DBType("Invalid");

    protected DBType(String strType) {
        m_strType = strType;
    }
    public void setVersion(String version){
        this.m_version=version;
    }
    public String getStringType() {
        return m_strType;
    }

    public String toString() {
        return m_strType;
    }

    private String m_strType;
    private String m_version;

    public String getVersion(){
       return m_version; 
    } 

}
