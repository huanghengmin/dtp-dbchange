/*=============================================================
 * �ļ�����: DbopFactory.java
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
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DefaultDBTypeMapper;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class DbopFactory {


    public final static String Str_DriverClass_TDSMssql = "com.inet.tds.TdsDriver";
    public final static String Str_DriverClass_JTDSMssql = "net.sourceforge.jtds.jdbc.Driver";


    private static CacheDbop dbms = new CacheDbop();

    public static IDbOp getDbms(Connection conn, String driverClass) throws SQLException {
        return getDbms(conn, driverClass, DefaultDbOp.Str_SqlBundleName);
    }

    public static IDbOp getDbms(Connection conn, String driverClass, String sqlBundleName) throws SQLException {
        return findDbms(conn, driverClass, sqlBundleName);
    }

    public static IDbOp findDbms(Connection conn, String driverClass, String sqlBundleName) throws SQLException {
        if (driverClass == null) {
            driverClass = "";
        }

        DBType dbType = DefaultDBTypeMapper.getDBType(conn);
        IDbOp IDbOp = dbms.findDbms(dbType, driverClass, sqlBundleName);
        if (IDbOp == null) {
            if (dbType == DBType.C_MSSQL) {
                /* if (driverClass.equalsIgnoreCase(Str_DriverClass_JTDSMssql)) {
                    IDbOp = new JTDSMssqlIDbms(conn, sqlBundleName);
                } else {
                    IDbOp = new MssqlIDbOp(conn, sqlBundleName);
                }*/
                IDbOp = new MssqlIDbOp(conn, sqlBundleName);
            } else if (dbType == DBType.C_Oracle) {
                IDbOp = new OracleIDbOp(conn, sqlBundleName);
            } else if (dbType == DBType.C_SYBASE) {
                IDbOp = new SybaseIDbOp(conn, sqlBundleName);
            } else if (dbType == DBType.C_DB2) {
                IDbOp = new Db2IDbOp(conn, sqlBundleName);
            } else /*if (dbType == DBType.C_MYSQL) {
                IDbOp = new MysqlIDbms(conn, sqlBundleName);
            } else */ {
                return null;
            }
            dbms.addDbms(IDbOp, dbType, driverClass, sqlBundleName);
        }

        return IDbOp;
    }

}
