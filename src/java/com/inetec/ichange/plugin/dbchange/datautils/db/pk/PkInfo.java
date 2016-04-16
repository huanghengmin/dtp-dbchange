/*=============================================================
 * �ļ�����: PkInfo.java
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
package com.inetec.ichange.plugin.dbchange.datautils.db.pk;

import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;

public class PkInfo {

    private String schemaName;
    private String tableName;
    private String columnName;
    private String dbType;
    private int jdbcType;


    public PkInfo(String schemaName, String tableName, String columnName, String dbType, int jdbcType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dbType = dbType;
        this.jdbcType = jdbcType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getDbType() {
        return dbType;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String toString() {
        return "DatabaseName:" + schemaName +
                ";tableName:" + tableName +
                ";columnName:" + columnName +
                ";dbtype:" + dbType +
                ";jdbcType:" + DbopUtil.getJdbcTypeString(jdbcType);
    }

}
