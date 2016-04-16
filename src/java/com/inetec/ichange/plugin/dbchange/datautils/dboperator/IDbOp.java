package com.inetec.ichange.plugin.dbchange.datautils.dboperator;


import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.common.i18n.Message;

import java.sql.*;
import java.io.Reader;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */

public interface IDbOp {

    public int getJdbcTypeFromVenderDb(String type);

    public ResultSet executeQuery(Connection conn, String sqlString) throws SQLException;

    public void executeUpdate(Connection conn, String sqlString) throws SQLException;

    public void call(Connection conn, String sqlString, String[] params) throws SQLException;

    public String getSqlProperty(Connection conn, String sqlKey) throws SQLException;

    public String getSqlProperty(Connection conn, String sqlKey, String[] params) throws SQLException;

    public String formatColumnName(String columnName) throws SQLException;

    public String formatTableName(String schemaName, String tableName) throws SQLException;

    public boolean isFieldEExist(Connection conn, String catalog,String schemaName,
                                 String tableName, String columnName) throws SQLException;

    public boolean isTableEExist(Connection conn, String catalog,String schemaName, String tableName) throws SQLException;



    public long getClobLength(Connection conn, String schemaName,
                              String tableName, String columnName) throws SQLException;

    public String getClobInitializer();

    public long getBlobLength(Connection conn, String schemaName,
                              String tableName, String columnName) throws SQLException;

    public String getBlobInitializer();

    public Column getColumnData(Column column, ResultSet rs, int indeEx) throws SQLException;

    public PreparedStatement getSatement(Connection conn, String sqlString, Column[] columns,Connection connection) throws SQLException;
    public PreparedStatement getReadOnlySatement(Connection conn, String sqlString, Column[] columns,Connection connection) throws SQLException;

    public EXSql sqlToExSql(SQLException e, Message Message);

    /**
     * Update a CLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  A stream that provides the new CLOB value.
     * @param length The length of the new CLOB value.
     * @throws SQLException        if the driver encounters an error.
     * @throws java.io.IOException if an I/O error occurs.
     */
    public void updateClobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, Reader value, int length)
            throws SQLException, IOException;

    /**
     * Update a CLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  The new CLOB value.
     * @throws SQLException if the driver encounters an error.
     */
    public void updateClobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, String value)
            throws SQLException;


    /**
     * Update a BLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  A stream that provides the new BLOB value.
     * @param length The length of the new BLOB value.
     * @throws SQLException if the driver encounters an error.
     * @throws IOException  if an I/O error occurs.
     */
    public void updateBlobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, InputStream value, int length)
            throws SQLException, IOException;

    /**
     * Update a BLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  The new BLOB value.
     * @throws SQLException if the driver encounters an error.
     */
    public void updateBlobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, byte[] value)
            throws SQLException;


}

