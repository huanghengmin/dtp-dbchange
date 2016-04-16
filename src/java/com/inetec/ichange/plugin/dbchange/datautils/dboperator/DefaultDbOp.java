

package com.inetec.ichange.plugin.dbchange.datautils.dboperator;


import java.sql.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.text.MessageFormat;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.SQLBundle;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DefaultDBTypeMapper;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DBType;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.db.Value;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.ichange.plugin.dbchange.utils.ReaderUtil;
import com.inetec.common.i18n.Message;

import org.apache.log4j.Logger;
/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */

/**
 * Shared implementation for classes implementing <CODE>IDbOp</CODE>.
 */

public abstract class DefaultDbOp implements IDbOp {
    public final static Logger logger = Logger.getLogger(DefaultDbOp.class);
    public final static String Str_SqlBundleName = "com.inetec.ichange.plugin.dbchange.sql";
    public final static String Str_SqlProperty_ReservedLeft = "ReservedLeft";
    public final static String Str_SqlProperty_ReservedRight = "ReservedRight";
    public final static String Str_SqlProperty_ClobInitializer = "ClobInitializer";
    public final static String Str_SqlProperty_BlobInitializer = "BlobInitializer";
    public final static String Str_SqlProperty_ReservedWords = "ReservedWords";


    protected String sqlBundleName = "";
    protected String reservedLeftChar = "\"";
    protected String reservedRightChar = "\"";
    protected String clobInitializer = "null";
    protected String blobInitializer = "null";
    protected String[] reservedWords = new String[0];

    protected DefaultDbOp(Connection conn) throws SQLException {
        this(conn, Str_SqlBundleName);
    }

    public DefaultDbOp(Connection conn, String sqlBundleName) throws SQLException {
        this.sqlBundleName = sqlBundleName;
        String tempValue = "";
        tempValue = getSqlProperty(conn, Str_SqlProperty_ReservedLeft);
        if (!tempValue.equals("")) {
            reservedLeftChar = tempValue;
        } // else {}

        tempValue = getSqlProperty(conn, Str_SqlProperty_ReservedRight);
        if (!tempValue.equals("")) {
            reservedRightChar = tempValue;
        } // else {}

        tempValue = getSqlProperty(conn, Str_SqlProperty_ClobInitializer);
        if (!tempValue.equals("")) {
            clobInitializer = tempValue;
        } // else {}

        tempValue = getSqlProperty(conn, Str_SqlProperty_BlobInitializer);
        if (!tempValue.equals("")) {
            blobInitializer = tempValue;
        } // else {}

        tempValue = getSqlProperty(conn, Str_SqlProperty_ReservedWords);
        ArrayList wordList = new ArrayList();
        StringTokenizer st = new StringTokenizer(tempValue, ";");
        while (st.hasMoreTokens()) {
            String next = st.nextToken();
            wordList.add(next);
        }
        reservedWords = (String[]) wordList.toArray(new String[0]);
    }


    public ResultSet executeQuery(Connection conn, String sqlString)
            throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();

            rs = stmt.executeQuery(sqlString);
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
        return rs;
    }

    public void executeUpdate(Connection conn, String sqlString)
            throws SQLException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(sqlString);
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
    }

    public void call(Connection conn, String sqlString, String[] params)
            throws SQLException {
        CallableStatement stmt = null;
        try {
            stmt = conn.prepareCall(sqlString);
            int j = 1;
            for (int i = 0; i < params.length; i++, j++) {
                stmt.setString(j, params[i]);
            }
            stmt.executeUpdate();
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
    }

    public String getSqlProperty(Connection conn, String sqlKey)
            throws SQLException {
        return getSqlProperty(conn, sqlKey, null);
    }

    public String getSqlProperty(Connection conn, String sqlKey, String[] params)
            throws SQLException {
        // Find the sql dialect
        DBType dbType = DefaultDBTypeMapper.getDBType(conn);
        String bundleName = sqlBundleName;    // default bundle name
        String key = sqlKey;
        int iColon = sqlKey.indexOf(':');
        if (iColon > 0) {
            bundleName = sqlKey.substring(0, iColon);
            key = sqlKey.substring(iColon + 1, sqlKey.length());
        }

        SQLBundle bundle = SQLBundle.getSQLBundle(dbType, bundleName);
        if (logger.isDebugEnabled()) {
            logger.debug("Sql bundle name:" + bundleName + dbType);
        }
        String sql = bundle.getSQL(key);
        if (logger.isDebugEnabled()) {
            logger.debug("Sql key:" + sql);
        }
        if (sql == null) {
            throw new SQLException("Failed to retrieve SQL statement for the key: '{0}'." + sqlKey);
        }

        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i++) {
            }
            sql = MessageFormat.format(sql, params);
        }
        return sql;
    }


    public String formatColumnName(String columnName)
            throws SQLException {
        for (int i = 0; i < reservedWords.length; i++) {
            if (reservedWords[i].equalsIgnoreCase(columnName)) {
                return reservedLeftChar + columnName + reservedRightChar;
            }
        }

        return columnName;
    }


    public String formatTableName(String schemaName, String tableName) {
        return schemaName + "." + tableName;
    }


    public boolean isFieldEExist(Connection conn, String schemaName, String tableName, String columnName)
            throws SQLException {
        String[] columns = getFieldList(conn, schemaName, tableName);
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName.trim())) {
                return true;
            }
        }

        return false;
    }

    public boolean isFieldEExist(Connection conn, String catalog, String schemaName, String tableName, String columnName)
            throws SQLException {
        String[] columns = getFieldList(conn, catalog, schemaName, tableName);
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName.trim())) {
                return true;
            }
        }

        return false;
    }

    protected String[] getFieldList(Connection conn, String catalog, String schemaName, String tableName)
            throws SQLException {

        ArrayList columnList = new ArrayList();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet fieldSet = metaData.getColumns(catalog.toUpperCase(), schemaName.toUpperCase(), tableName.toUpperCase(), null);
            while (fieldSet.next()) {
                String column = fieldSet.getString("COLUMN_NAME");
                column.trim();
                columnList.add(column);
            }
            return (String[]) columnList.toArray(new String[0]);
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        }
    }


    public boolean isTableEExist(Connection conn, String catalog, String schemaName, String tableName)
            throws SQLException {
        String[] tables = getTableNameList(conn, catalog, schemaName);
        for (int i = 0; i < tables.length; i++) {
            String t = tables[i];
            if (t.equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    protected String[] getFieldList(Connection conn, String schemaName, String tableName)
            throws SQLException {

        ArrayList columnList = new ArrayList();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet fieldSet = metaData.getColumns(null, schemaName.toUpperCase(), tableName.toUpperCase(), null);
            while (fieldSet.next()) {
                String column = fieldSet.getString("COLUMN_NAME");
                column.trim();
                columnList.add(column);
            }
            return (String[]) columnList.toArray(new String[0]);
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        }
    }


    public boolean isTableEExist(Connection conn, String schemaName, String tableName)
            throws SQLException {
        String[] tables = getTableNameList(conn, schemaName);
        for (int i = 0; i < tables.length; i++) {
            String t = tables[i];
            if (t.equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    protected String[] getTableNameList(Connection conn, String catalog, String schemaName)
            throws SQLException {
        String types[] = {
                "TABLE"
        };
        ArrayList tableList = new ArrayList();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tabelSet = metaData.getTables(null, null, null, types);
            while (tabelSet.next()) {
                String tableName = tabelSet.getString("TABLE_NAME");
                tableName.trim();
                tableList.add(tableName);
                Collections.sort(tableList);
            }
            return (String[]) tableList.toArray(new String[0]);
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        }
    }

    protected String[] getTableNameList(Connection conn, String schemaName)
            throws SQLException {
        String types[] = {
                "TABLE"
        };
        ArrayList tableList = new ArrayList();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tabelSet = metaData.getTables(null, null, null, types);
            while (tabelSet.next()) {
                String tableName = tabelSet.getString("TABLE_NAME");
                tableName.trim();
                tableList.add(tableName);
                Collections.sort(tableList);
            }
            return (String[]) tableList.toArray(new String[0]);
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        }
    }

    /**
     * Get a Clob length
     *
     * @param conn
     * @param schemaName
     * @param tableName
     * @param columnName
     * @return
     * @throws SQLException
     */

    public long getClobLength(Connection conn, String schemaName, String tableName, String columnName)
            throws SQLException {
        long length = 0;
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement("select datalength(" + columnName + ") from " + tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                length = rs.getLong(1);
            }
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        } finally {
            DbopUtil.closeStatement(stmt);
        }

        return length;
    }


    /**
     * Get a Blob length
     *
     * @param conn
     * @param schemaName
     * @param tableName
     * @param columnName
     * @return
     * @throws SQLException
     */
    public long getBlobLength(Connection conn, String schemaName, String tableName, String columnName)
            throws SQLException {
        long length = 0;
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement("select datalength(" + columnName + ") from " + tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                length = rs.getLong(1);
            }
        } catch (SQLException sqlEEx) {
            throw sqlEEx;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
        return length;
    }


    /**
     * Obtain the SQL initializer for a BLOB column.
     *
     * @return The SQL initializer for a BLOB column.
     */
    public String getBlobInitializer() {
        return blobInitializer;
    }


    /**
     * Obtain the SQL initializer for a CLOB column.
     *
     * @return The SQL initializer for a CLOB column.
     */

    public String getClobInitializer() {
        return clobInitializer;
    }


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
            throws SQLException, IOException {

        // todo: test
        /*
        char [] cha = new char[length];
        int t = value.read(cha);
        while (t!=-1) {
            System.out.print(cha);
            t =value.read(cha);
        }

        // todo: test
        logger.info("length=" + length);
        */
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("update " + table + " set " + column + " = ? "
                    + where);
            int n = 1;
            //todo:���readerUtil����
            ReaderUtil readerUtil = new ReaderUtil(value);
            stmt.setCharacterStream(n++, readerUtil.getReader(), (int) readerUtil.getLength());

            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    stmt.setObject(n++, params[i]);
                }
            }

            /*
            try {
                stmt.executeUpdate();
            } catch (Exception eEx) {
                logger.error("Failed to eExecute the preparedstatement for clob!", eEx);
            }
            */

            stmt.executeUpdate();

            stmt.close();
            stmt = null;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
    }

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
            throws SQLException {
        try {
            StringReader in = new StringReader(value);
            updateClobColumn(conn, table, column, where, params, in, value.length());
            in.close();
        } catch (IOException eEx) {
            throw new SQLException("IO error reading Clob value: " + eEx.getMessage());
        }
    }


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
            throws SQLException, IOException {

        PreparedStatement stmt = null;
        logger.info("updata blob cloumn sql:" + "update " + table + " set " + column + " = ? "
                + where);
        try {
            stmt = conn.prepareStatement("update " + table + " set " + column + " = ? "
                    + where);
            int n = 1;

            stmt.setBinaryStream(n++, value, length);
            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    stmt.setObject(n++, params[i]);
                }
            }
            stmt.executeUpdate();
            stmt.close();
            stmt = null;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
    }

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
            throws SQLException {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(value);
            updateBlobColumn(conn, table, column, where, params, in, value.length);
            in.close();
        } catch (IOException eEx) {
            throw new SQLException("IO error reading Blob value: " + eEx.getMessage());
        }
    }

    public Column getColumnData(Column column, ResultSet rs, int indeEx)
            throws SQLException {

        String basicValue = null;
        int jdbcType = column.getJdbcType();
        switch (jdbcType) {
            case Types.BIT:
                basicValue = rs.getBoolean(indeEx) + "";
                break;
            case Types.TINYINT:
                basicValue = rs.getByte(indeEx) + "";
                break;
            case Types.SMALLINT:
                basicValue = rs.getShort(indeEx) + "";
                break;
            case Types.INTEGER:
                basicValue = rs.getInt(indeEx) + "";
                break;
            case Types.BIGINT:
                basicValue = rs.getLong(indeEx) + "";
                break;
            case Types.FLOAT:
                basicValue = rs.getFloat(indeEx) + "";
                break;
            case Types.REAL:
            case Types.DOUBLE:
                basicValue = rs.getDouble(indeEx) + "";
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                basicValue = rs.getBigDecimal(indeEx) + "";
                break;
            // todo: char encoding
            case Types.CHAR:
            case Types.VARCHAR:
                basicValue = rs.getString(indeEx);
                break;
            case Types.DATE:
                if (rs.getDate(indeEx) != null)
                    basicValue = rs.getDate(indeEx).getTime() + "";
                else
                    basicValue = rs.getDate(indeEx) + "";

                break;
            case Types.TIME:
                if (rs.getTime(indeEx) != null)
                    basicValue = rs.getTime(indeEx).getTime() + "";
                else
                    basicValue = rs.getTime(indeEx) + "";
                break;
            case Types.TIMESTAMP:
                if (rs.getTimestamp(indeEx) != null)
                    basicValue = rs.getTimestamp(indeEx).getTime() + "";
                else
                    basicValue = rs.getTimestamp(indeEx) + "";
                break;

            case Types.LONGVARBINARY: {
                InputStream is = rs.getBinaryStream(indeEx);
                if (is == null) {
                    column.setValue(new Value((InputStream) null, (long) 0));
                } else {
                    try {
                        //File tempFile = File.createTempFile("temp_", ".proc");
                        ByteArrayOutputStream fos = new ByteArrayOutputStream();
                        byte[] temp = new byte[1024];
                        int readed1 = -1;
                        readed1 = is.read(temp);
                        while (readed1 != -1) {
                            fos.write(temp, 0, readed1);
                            temp = new byte[1024];
                            readed1 = is.read(temp);
                        }
                        is.close();
                        int len = fos.size();
                        ByteArrayInputStream fis = new ByteArrayInputStream(fos.toByteArray());

                        column.setValue(new Value(fis, len));
                    } catch (IOException e) {
                        logger.error("Incorrect to get inputstream length.", e);
                    }
                }
                break;
            }
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.BLOB: {
                Blob blob = rs.getBlob(indeEx);
                if (blob == null) {
                    column.setValue(new Value((InputStream) null, (long) 0));
                } else {
                    long len = blob.length();
                    column.setValue(new Value(blob.getBinaryStream(), len));
                }
                break;
            }
            case Types.LONGVARCHAR:
            case Types.CLOB: {
                Clob clob = rs.getClob(indeEx);
                if (clob == null) {
                    column.setValue(new Value((Reader) null, (long) 0));
                } else {
                    long len = clob.length();
                    column.setValue(new Value(clob.getCharacterStream(), len));
                }
                break;
            }
            // not supported: belows
            case Types.NULL:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.ARRAY:
            case Types.REF:
            case Types.STRUCT:
            default:
                break;
        }
        if (rs.wasNull()) {
            column.setValue(null);
        } else {
            if (!column.isLobType()) {
                column.setValue(new Value(basicValue));
            }
        }

        return column;
    }


    public PreparedStatement getSatement(Connection conn, String sqlString, Column[] columns,Connection connection) throws SQLException {
        int size = columns.length;
        PreparedStatement prepStmt = null;
        if (size > 0) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sql String: " + sqlString);
                }

                prepStmt = conn.prepareStatement(sqlString);

                if (logger.isDebugEnabled()) {
                    logger.debug("Column size: " + size);
                }

                for (int i = 0; i < size; i++) {
                    Column c = columns[i];
                    int jdbcType = c.getJdbcType();
                    if (c.isNull()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Column is null.");
                        }
                        prepStmt.setNull(i + 1, jdbcType);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Start to get statment.");
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("i: " + i);
                        }
                        String valueString = c.getValue().getValueString();
                        try {
                            switch (jdbcType) {
                                case Types.BIT:
                                    prepStmt.setBoolean(i + 1, Boolean.valueOf(valueString.trim()).booleanValue());
                                    break;
                                case Types.TINYINT:
                                    prepStmt.setByte(i + 1, Byte.valueOf(valueString.trim()).byteValue());
                                    break;
                                case Types.SMALLINT:
                                    prepStmt.setShort(i + 1, Short.valueOf(valueString.trim()).shortValue());
                                    break;
                                case Types.INTEGER:
                                    prepStmt.setInt(i + 1, Integer.valueOf(valueString.trim()).intValue());
                                    break;
                                case Types.BIGINT:
                                    prepStmt.setLong(i + 1, Long.valueOf(valueString.trim()).longValue());
                                    break;
                                case Types.FLOAT:
                                    prepStmt.setFloat(i + 1, Float.valueOf(valueString.trim()).floatValue());
                                    break;
                                case Types.REAL:
                                case Types.DOUBLE:
                                    prepStmt.setDouble(i + 1, Double.valueOf(valueString.trim()).doubleValue());
                                    break;
                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                    prepStmt.setBigDecimal(i + 1, new BigDecimal(valueString.trim()));
                                    break;
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(i + 1, valueString);
                                    break;


                                case Types.DATE:
                                    if (isNumeric(valueString.trim())) {
                                        prepStmt.setDate(i + 1, new Date(Long.parseLong(valueString)));
                                    } else {
                                        prepStmt.setDate(i + 1, Date.valueOf(valueString.trim()));
                                    }

                                    break;
                                case Types.TIME:
                                    if (isNumeric(valueString.trim())) {
                                        prepStmt.setTime(i + 1, new Time(Long.parseLong(valueString)));
                                    } else {
                                        prepStmt.setTime(i + 1, Time.valueOf(valueString.trim()));
                                    }
                                    break;
                                case Types.TIMESTAMP:
                                    if (isNumeric(valueString.trim()))
                                        prepStmt.setTimestamp(i + 1, new Timestamp(Long.parseLong(valueString.trim())));
                                    else
                                        prepStmt.setTimestamp(i + 1, Timestamp.valueOf(valueString.trim()));
                                    break;

                                case Types.BINARY:
                                case Types.LONGVARBINARY:
                                case Types.VARBINARY:
                                case Types.BLOB:
                                case Types.CLOB:
                                case Types.LONGVARCHAR:
                                case Types.NULL:
                                case Types.DISTINCT:
                                case Types.JAVA_OBJECT:
                                case Types.OTHER:
                                case Types.ARRAY:
                                case Types.REF:
                                case Types.STRUCT:
                                default:
                                    logger.error("type " + c.getJdbcTypeString() + " not supported: " + c.getValue());
                                    break;

                            }
                        } catch (NumberFormatException nfe) {
                            throw new SQLException("the format error: " + valueString);
                        }
                    }
                }
            } catch (SQLException sqlEEx) {
                throw sqlEEx;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Succeed to get statment.");
        }
        return prepStmt;
    }

    public int getJdbcTypeFromVenderDb(String type) {
        return Types.VARCHAR;
    }

    public EXSql sqlToExSql(SQLException e, Message Message) {
        return (EXSql) new EXSql().set(ErrorSql.ERROR___OTHER, e, Message);
    }

    public PreparedStatement getReadOnlySatement(Connection conn, String sqlString, Column[] columns,Connection connection) throws SQLException {
        int size = columns.length;
        PreparedStatement prepStmt = null;
        if (size > 0) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sql String: " + sqlString);
                }
                prepStmt = conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                if (logger.isDebugEnabled()) {
                    logger.debug("Column size: " + size);
                }

                for (int i = 0; i < size; i++) {
                    Column c = columns[i];
                    int jdbcType = c.getJdbcType();
                    if (c.isNull()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Column is null.");
                        }
                        prepStmt.setNull(i + 1, jdbcType);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Start to get statment.");
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("i: " + i);
                        }
                        String valueString = c.getValue().getValueString();
                        try {
                            switch (jdbcType) {
                                case Types.BIT:
                                    prepStmt.setBoolean(i + 1, Boolean.valueOf(valueString.trim()).booleanValue());
                                    break;
                                case Types.TINYINT:
                                    prepStmt.setByte(i + 1, Byte.valueOf(valueString.trim()).byteValue());
                                    break;
                                case Types.SMALLINT:
                                    prepStmt.setShort(i + 1, Short.valueOf(valueString.trim()).shortValue());
                                    break;
                                case Types.INTEGER:
                                    prepStmt.setInt(i + 1, Integer.valueOf(valueString.trim()).intValue());
                                    break;
                                case Types.BIGINT:
                                    prepStmt.setLong(i + 1, Long.valueOf(valueString.trim()).longValue());
                                    break;
                                case Types.FLOAT:
                                    prepStmt.setFloat(i + 1, Float.valueOf(valueString.trim()).floatValue());
                                    break;
                                case Types.REAL:
                                case Types.DOUBLE:
                                    prepStmt.setDouble(i + 1, Double.valueOf(valueString.trim()).doubleValue());
                                    break;
                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                    prepStmt.setBigDecimal(i + 1, new BigDecimal(valueString.trim()));
                                    break;
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(i + 1, valueString);
                                    break;
                                // todo: m_isInputAdapter and encoding
                                case Types.DATE:
                                    if(valueString.startsWith("-")||isNumeric(valueString.substring(1))){
                                        prepStmt.setDate(i + 1, new Date(Long.parseLong(valueString.trim())));
                                        break;
                                    }
                                    if (isNumeric(valueString.trim())) {
                                        prepStmt.setDate(i + 1, new Date(Long.parseLong(valueString)));
                                    } else {
                                        prepStmt.setDate(i + 1, Date.valueOf(valueString.trim()));
                                    }

                                    break;
                                case Types.TIME:
                                    if(valueString.startsWith("-")||isNumeric(valueString.substring(1))){
                                        prepStmt.setTime(i + 1, new Time(Long.parseLong(valueString.trim())));
                                        break;
                                    }
                                    if (isNumeric(valueString.trim())) {
                                        prepStmt.setTime(i + 1, new Time(Long.parseLong(valueString)));
                                    } else {
                                        prepStmt.setTime(i + 1, Time.valueOf(valueString.trim()));
                                    }
                                    break;
                                case Types.TIMESTAMP:
                                    if(valueString.startsWith("-")||isNumeric(valueString.substring(1))){
                                        prepStmt.setTimestamp(i + 1, new Timestamp(Long.parseLong(valueString.trim())));
                                        break;
                                    }
                                    if (isNumeric(valueString.trim()))
                                        prepStmt.setTimestamp(i + 1, new Timestamp(Long.parseLong(valueString.trim())));
                                    else
                                        prepStmt.setTimestamp(i + 1, Timestamp.valueOf(valueString.trim()));
                                    break;

                                case Types.BINARY:
                                case Types.LONGVARBINARY:
                                case Types.VARBINARY:
                                case Types.BLOB:
                                case Types.CLOB:
                                case Types.LONGVARCHAR:
                                case Types.NULL:
                                case Types.DISTINCT:
                                case Types.JAVA_OBJECT:
                                case Types.OTHER:
                                case Types.ARRAY:
                                case Types.REF:
                                case Types.STRUCT:
                                default:
                                    logger.error("type " + c.getJdbcTypeString() + " not supported: " + c.getValue());
                                    break;

                            }
                        } catch (NumberFormatException nfe) {
                            throw new SQLException("the format error: " + valueString);
                        }
                    }
                }
            } catch (SQLException sqlEEx) {
                throw sqlEEx;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Succeed to get statment.");
        }
        return prepStmt;
    }

    public boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

}
