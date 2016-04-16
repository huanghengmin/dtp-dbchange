package com.inetec.ichange.plugin.dbchange.datautils.dboperator;

import java.sql.*;
import java.util.ArrayList;
import java.io.*;
import java.math.BigDecimal;

import org.apache.log4j.Logger;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.db.Value;
import com.inetec.common.i18n.Message;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class SybaseIDbOp extends DefaultDbOp {
    public static Logger m_logger = Logger.getLogger(SybaseIDbOp.class);
    public final static String[][] sybase2jdbc = {
            {"bigint", "BIGINT"},
            {"bigint identity", "BIGINT"},
            {"binary", "BINARY"},
            {"bit", "BIT"},
            {"char", "CHAR"},
            {"datetime", "TIMESTAMP"},
            {"date", "DATE"},
            {"decimal", "DECIMAL"},
            {"decimal identity", "DECIMAL"},
            {"float", "DOUBLE"},
             {"double precis", "DOUBLE"},
            {"image", "BLOB"},
            {"int", "INTEGER"},
            {"int identity", "INTEGER"},
            {"money", "DECIMAL"},
            {"numeric", "NUMERIC"},
            {"numeric identity", "NUMERIC"},
            {"real", "REAL"},
            {"smalldatetime", "TIMESTAMP"},
            {"smallint", "SMALLINT"},
            {"smallint identity", "SMALLINT"},
            {"smallmoney", "DECIMAL"},
            {"text", "CLOB"},
            {"tinyint", "TINYINT"},
            {"tinyint identity", "TINYINT"},
            {"varbinary", "VARBINARY"},
            {"varchar", "VARCHAR"},

            // not supported in opta2000
            {"nchar", "CHAR"},
            {"ntext", "LONGVARCHAR"},
            {"nvarchar", "VARCHAR"},
            {"sql_variant", "VARCHAR"},
            {"sysname", "VARCHAR"},
            {"uniqueidentifier", "CHAR"},

    };

    public SybaseIDbOp(Connection conn) throws SQLException {
        super(conn);
    }

    public SybaseIDbOp(Connection conn, String sqlBundleName) throws SQLException {
        super(conn, sqlBundleName);
    }

    protected String[] getFieldList(Connection conn, String schemaName, String tableName)
            throws SQLException {

        ArrayList columnList = new ArrayList();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet fieldSet = metaData.getColumns(null, null, tableName.toUpperCase(), null);
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


    public String formatTableName(String schemaName, String tableName) {
        return tableName;
    }


    public int getJdbcTypeFromVenderDb(String type) {
        int size = sybase2jdbc.length;
        for (int i = 0; i < size; i++) {
            if (type.equalsIgnoreCase(sybase2jdbc[i][0])) {
                return DbopUtil.getJdbcType(sybase2jdbc[i][1]);
            }
        }
        return Types.VARCHAR;
    }


    public boolean isDbConnectionEExp(int errorCode, String sqlState) {
        if (sqlState.equalsIgnoreCase("JZ006")) {
            return true;
        } else {
            return false;
        }
    }

    public EXSql sqlToExSql(SQLException e, Message Message) {
        String sqlState = e.getSQLState();
        EXSql Exsql = null;

        if (sqlState.equalsIgnoreCase("JZ006")) {
            Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___DB_CONNECTION, e, Message);
        }
        if (Exsql == null) {
            Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___OTHER, e, Message);
        }

        return Exsql;
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
                basicValue = rs.getDate(indeEx)+"";
                 break;
            case Types.TIME:
                 basicValue = rs.getTime(indeEx)+"";
                break;
            case Types.TIMESTAMP:
                basicValue = rs.getTimestamp(indeEx) + "";
                break;
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.BLOB:
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
            case Types.LONGVARCHAR:
            case Types.CLOB: {
                InputStream clob = rs.getAsciiStream(indeEx);
                if (clob == null) {
                    column.setValue(new Value((Reader) null, (long) 0));
                } else {
                    try {
                        long len = clob.available();
                        column.setValue(new Value(clob, len));
                    } catch (IOException e) {
                        logger.error("Incorrect to get inputstream length.", e);
                    }

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

     /*public PreparedStatement getSatement(Connection conn, String sqlString, Column[] columns) throws SQLException {
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

                                    break;
                                case Types.TINYINT:
                                    prepStmt.setByte(i + 1, Byte.valueOf(valueString).byteValue());
                                    break;
                                case Types.SMALLINT:
                                    prepStmt.setShort(i + 1, Short.valueOf(valueString).shortValue());
                                    break;
                                case Types.INTEGER:
                                    prepStmt.setInt(i + 1, Integer.valueOf(valueString).intValue());
                                    break;
                                case Types.BIGINT:
                                    prepStmt.setLong(i + 1, Long.valueOf(valueString).longValue());
                                    break;
                                case Types.FLOAT:
                                    prepStmt.setFloat(i + 1, Float.valueOf(valueString).floatValue());
                                    break;
                                case Types.REAL:
                                case Types.DOUBLE:
                                    prepStmt.setDouble(i + 1, Double.valueOf(valueString).doubleValue());
                                    break;
                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                    prepStmt.setBigDecimal(i + 1, new BigDecimal(valueString));
                                    break;
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(i + 1, valueString);
                                    break;


                                case Types.DATE:
                                    prepStmt.setDate(i + 1, Date.valueOf(valueString));
                                    break;
                                case Types.TIME:
                                case Types.TIMESTAMP:
                                    prepStmt.setTimestamp(i + 1, Timestamp.valueOf(valueString));
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
     public PreparedStatement getReadOnlySatement(Connection conn, String sqlString, Column[] columns) throws SQLException {
         int size = columns.length;
        PreparedStatement prepStmt = null;
        if (size > 0) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sql String: " + sqlString);
                }
                prepStmt = conn.prepareStatement(sqlString,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

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
                                    break;
                                case Types.TINYINT:
                                    prepStmt.setByte(i + 1, Byte.valueOf(valueString).byteValue());
                                    break;
                                case Types.SMALLINT:
                                    prepStmt.setShort(i + 1, Short.valueOf(valueString).shortValue());
                                    break;
                                case Types.INTEGER:
                                    prepStmt.setInt(i + 1, Integer.valueOf(valueString).intValue());
                                    break;
                                case Types.BIGINT:
                                    prepStmt.setLong(i + 1, Long.valueOf(valueString).longValue());
                                    break;
                                case Types.FLOAT:
                                    prepStmt.setFloat(i + 1, Float.valueOf(valueString).floatValue());
                                    break;
                                case Types.REAL:
                                case Types.DOUBLE:
                                    prepStmt.setDouble(i + 1, Double.valueOf(valueString).doubleValue());
                                    break;
                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                    prepStmt.setBigDecimal(i + 1, new BigDecimal(valueString));
                                    break;
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(i + 1, valueString);
                                    break;
                                    // todo: m_isInputAdapter and encoding

                                case Types.DATE:
                                    prepStmt.setDate(i + 1, Date.valueOf(valueString));
                                    break;
                                case Types.TIME:
                                case Types.TIMESTAMP:
                                    prepStmt.setTimestamp(i + 1, Timestamp.valueOf(valueString));
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
*/

}
