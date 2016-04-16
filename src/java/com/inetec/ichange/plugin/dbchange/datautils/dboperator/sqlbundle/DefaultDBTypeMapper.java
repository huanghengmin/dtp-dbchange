
package com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class DefaultDBTypeMapper {
    /**
     * Uses an algorithm to derive a token for a support database type from
     * connection metadata.
     *
     * @param databaseProductName    DatabaseProductName from database metadata.
     * @param databaseProductVersion DatabaseProductVersion from database metadata.
     */
    protected static DBType getDBType(String databaseProductName, String databaseProductVersion) {
        DBType dbType = null;
        String strWorking;
        String strDbType = null;


        strWorking = databaseProductName.toLowerCase();
        if (strWorking.indexOf("oracle") > -1) {
            return DBType.C_Oracle;
        } else if (strWorking.indexOf("sql server") > -1) {

            return DBType.C_MSSQL;
        } else if (strWorking.indexOf("db2") > -1) {
            return DBType.C_DB2;
        } else if (strWorking.indexOf("adaptive server enterprise") > -1) {
            return DBType.C_SYBASE;
        } else if (strWorking.indexOf("mysql") > -1) {
            return DBType.C_MYSQL;
        } else {
            return DBType.C_Invalid;
        }
    }

    /**
     * Uses an algorithm to derive a token for a support database type from
     * connection metadata.
     *
     * @param connection Open connection, so that database metadata can obtained.
     */
    public static DBType getDBType(Connection connection) throws SQLException {
        DatabaseMetaData connectionMetaData;
        String databaseProductName, databaseProductVersion;

        // check arguments
        if (connection == null || connection.isClosed()) {
            throw new IllegalArgumentException("Need an open connection");
        }

        // get metadata information
        connectionMetaData = connection.getMetaData();
        databaseProductName = connectionMetaData.getDatabaseProductName();
        databaseProductVersion = connectionMetaData.getDatabaseProductVersion();
        String driverName=connectionMetaData.getDriverName().toLowerCase();
        DBType type= getDBType(databaseProductName, databaseProductVersion);
        if(type.equals(DBType.C_MSSQL)&&driverName.indexOf("jtds")>-1){
            if(databaseProductName.equals("Microsoft SQL Server")){
                type= DBType.C_MSSQL;
            }
            if(databaseProductName.equals("sql server")){
                type=DBType.C_SYBASE;
            }
        }
        type.setVersion(databaseProductVersion);
        return type;
    }


}
