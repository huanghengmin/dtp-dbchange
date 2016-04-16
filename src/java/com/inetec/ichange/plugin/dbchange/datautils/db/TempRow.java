/*=============================================================
 * �ļ�����: TempRow.java
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

import com.inetec.common.exception.Ex;
import com.inetec.ichange.plugin.dbchange.datautils.db.pk.PkSet;

import java.sql.Timestamp;


public class TempRow {
    private long id;
    private String databaseName;
    private String tableName;

    private PkSet pks;
    private String act;
    private Timestamp m_actTime;

    public TempRow() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public PkSet getPks() {
        return pks;
    }

    public void setPks(String pkString) throws Ex {
        pks = new PkSet(pkString);
    }

    public String getAct() {
        return act;
    }

    public void setAct(String act) {
        this.act = act;
    }

    public Operator getAction() {
        if (act == null) {
            return Operator.OPERATOR__INSERT;
        } else if (act.equalsIgnoreCase("i")) {
            return Operator.OPERATOR__INSERT;
        } else if (act.equalsIgnoreCase("u")) {
            return Operator.OPERATOR__UPDATE;
        } else if (act.equalsIgnoreCase("d")) {
            return Operator.OPERATOR__DELETE;
        }

        return Operator.OPERATOR__INSERT;
    }

    public void setActTime(Timestamp actTime) {
        m_actTime = actTime;
    }

    public Timestamp getActTime() {
        return m_actTime;
    }
}
