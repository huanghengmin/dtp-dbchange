/*=============================================================
 * 文件名称: DbopFactory.java
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
