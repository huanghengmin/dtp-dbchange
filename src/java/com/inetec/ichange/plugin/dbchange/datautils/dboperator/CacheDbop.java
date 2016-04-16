/*=============================================================
 * �ļ�����: CacheDbop.java
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
package com.inetec.ichange.plugin.dbchange.datautils.dboperator;

import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DBType;

import java.util.ArrayList;

public class CacheDbop {

    private ArrayList lists = new ArrayList();

    public IDbOp findDbms(DBType dbType, String driverClass, String sqlBundleName) {
        for (int i = 0; i < lists.size(); i++) {
            CacheObject co = (CacheObject) lists.get(i);
            if (co.getDbType() == dbType
                    && co.getDriverClass().equals(driverClass)
                    && co.getSqlBundleName().equals(sqlBundleName)) {
                return co.getDbms();
            }
        }

        return null;
    }

    public void addDbms(IDbOp IDbOp, DBType dbType, String driverClass, String sqlBundleName) {
        lists.add(new CacheObject(IDbOp, dbType, driverClass, sqlBundleName));
    }

    class CacheObject {
        private IDbOp IDbOp;
        private DBType dbType;
        private String driverClass;
        private String sqlBundleName;

        public CacheObject(IDbOp IDbOp, DBType dbType, String driverClass, String sqlBundleName) {
            this.IDbOp = IDbOp;
            this.dbType = dbType;
            this.driverClass = driverClass;
            this.sqlBundleName = sqlBundleName;
        }

        public IDbOp getDbms() {
            return IDbOp;
        }

        public DBType getDbType() {
            return dbType;
        }

        public String getDriverClass() {
            return driverClass;
        }

        public String getSqlBundleName() {
            return sqlBundleName;
        }
    }

}
