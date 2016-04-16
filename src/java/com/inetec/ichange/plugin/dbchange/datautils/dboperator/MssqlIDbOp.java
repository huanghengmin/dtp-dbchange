/*=============================================================
 * 文件名称: MssqlDbOp.java
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


import com.inetec.common.util.StringUtil;
import com.inetec.ichange.plugin.dbchange.datautils.db.Value;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.common.i18n.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */

/**
 * Implementation of <CODE>IDbOp</CODE> for Microsoft SQL Server.
 */

public class MssqlIDbOp extends DefaultDbOp {

    public static Logger m_logger = Logger.getLogger(MssqlIDbOp.class);

    public final static String[][] mssql2jdbc = {
            {"bigint", "BIGINT"},
            {"bigint identity", "BIGINT"},
            {"binary", "BINARY"},
            {"bit", "BIT"},
            {"char", "CHAR"},
            {"datetime", "TIMESTAMP"},
            {"decimal", "DECIMAL"},
            {"decimal identity", "DECIMAL"},
            {"float", "DOUBLE"},
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
            {"timestamp", "BINARY"},
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

    public MssqlIDbOp(Connection conn) throws SQLException {
        super(conn);
    }

    public MssqlIDbOp(Connection conn, String sqlBundleName) throws SQLException {
        super(conn, sqlBundleName);
    }

    public int getJdbcTypeFromVenderDb(String type) {
        int size = mssql2jdbc.length;
        for (int i = 0; i < size; i++) {
            if (type.equalsIgnoreCase(mssql2jdbc[i][0])) {
                return DbopUtil.getJdbcType(mssql2jdbc[i][1]);
            }
        }

        return Types.VARCHAR;
    }

    public String formatTableName(String schemaName, String tableName) {
        return tableName;
    }


    public PreparedStatement getSatement(Connection conn, String sqlString, Column[] columns) throws SQLException {
        int size = columns.length;
        PreparedStatement prepStmt = null;
        HashMap maptest = new HashMap();
        if (size > 0) {
            try {
                if (logger.isDebugEnabled()) {
                    m_logger.info("Sql String: " + sqlString);
                }

                if (isSql2005Up(conn.getMetaData().getDatabaseProductVersion()))
                    conn.setTransactionIsolation(4096);  //快照事务
                prepStmt = conn.prepareStatement(sqlString);

                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Column size: " + size);
                }
                int j = -1;

                for (int i = 0; i < size; i++) {
                    Column c = columns[i];
                    if (maptest.containsKey(c.getName())) {
                        continue;
                    }
                    j++;
                    maptest.put(c.getName(), c.getName());
                    int jdbcType = c.getJdbcType();
                    if (c.isNull() && !c.isLobType()) {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("Column is null.");
                        }

                        prepStmt.setNull(j + 1, jdbcType);
                    } else {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("Start to get statment.");
                        }
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("i: " + i);
                        }

                        String valueString = "";
                        if (!c.isNull()) {
                            valueString = c.getValue().getValueString();
                        }
                        try {
                            switch (jdbcType) {
                                case Types.BIT:
                                    prepStmt.setBoolean(j + 1, Boolean.valueOf(valueString.trim()).booleanValue());
                                    break;
                                case Types.TINYINT:
                                    prepStmt.setByte(j + 1, Byte.valueOf(valueString.trim()).byteValue());
                                    break;
                                case Types.SMALLINT:
                                    prepStmt.setShort(j + 1, Short.valueOf(valueString.trim()).shortValue());
                                    break;
                                case Types.INTEGER:
                                    prepStmt.setInt(j + 1, Integer.valueOf(valueString.trim()).intValue());
                                    break;
                                case Types.BIGINT:
                                    prepStmt.setLong(j + 1, Long.valueOf(valueString.trim()).longValue());
                                    break;
                                case Types.FLOAT:
                                    prepStmt.setFloat(j + 1, Float.valueOf(valueString.trim()).floatValue());
                                    break;
                                case Types.REAL:
                                case Types.DOUBLE:
                                    prepStmt.setDouble(j + 1, Double.valueOf(valueString.trim()).doubleValue());
                                    break;
                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                    prepStmt.setBigDecimal(j + 1, new BigDecimal(valueString.trim()));
                                    break;
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(j + 1, valueString);
                                    break;

                                case Types.DATE:
                                    if (valueString.startsWith("-") || isNumeric(valueString.substring(1))) {
                                        prepStmt.setDate(j + 1, new Date(Long.parseLong(valueString.trim())));
                                        break;
                                    }
                                    if (isNumeric(valueString.trim())) {
                                        prepStmt.setDate(j + 1, new Date(Long.parseLong(valueString)));
                                    } else {
                                        prepStmt.setDate(j + 1, Date.valueOf(valueString.trim()));
                                    }

                                    break;
                                case Types.TIME:
                                    if (valueString.startsWith("-") || isNumeric(valueString.substring(1))) {
                                        prepStmt.setTime(j + 1, new Time(Long.parseLong(valueString.trim())));
                                        break;
                                    }
                                    if (isNumeric(valueString.trim())) {
                                        prepStmt.setTime(j + 1, new Time(Long.parseLong(valueString)));
                                    } else {
                                        prepStmt.setTime(j + 1, Time.valueOf(valueString.trim()));
                                    }
                                    break;
                                case Types.TIMESTAMP:
                                    if (valueString.startsWith("-") || isNumeric(valueString.substring(1))) {
                                        prepStmt.setTimestamp(j + 1, new Timestamp(Long.parseLong(valueString.trim())));
                                        break;
                                    }
                                    if (isNumeric(valueString.trim()))
                                        prepStmt.setTimestamp(j + 1, new Timestamp(Long.parseLong(valueString.trim())));
                                    else
                                        prepStmt.setTimestamp(j + 1, Timestamp.valueOf(valueString.trim()));
                                    break;

                                case Types.BINARY:
                                    prepStmt.setBinaryStream(j + 1, c.getValue().getBlobStream(), 0);
                                    break;
                                case Types.LONGVARBINARY:
                                    prepStmt.setBinaryStream(j + 1, c.getValue().getBlobStream(), 0);
                                    break;
                                case Types.VARBINARY:
                                    prepStmt.setBinaryStream(j + 1, new Value(null).getBlobStream(), 0);
                                    break;
                                case Types.BLOB:
                                    logger.info("blob value index:" + (i + 1));
                                    prepStmt.setBinaryStream(j + 1, new Value(null).getBlobStream(), 0);
                                    break;
                                case Types.CLOB:
                                    prepStmt.setCharacterStream(j + 1, new Value(null).getClobReader(), 0);
                                    break;
                                case Types.LONGVARCHAR:
                                    prepStmt.setCharacterStream(j + 1, new Value(null).getClobReader(), 0);
                                    break;
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

    public void updateBlobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, byte[] value)
            throws SQLException {

        PreparedStatement stmt = null;
        m_logger.info("updata blob cloumn sql:" + "update " + table + " set " + column + " = ? "
                + where+"  top 1");

        try {
            stmt = conn.prepareStatement("update " + table + " set " + column + " = ? "
                    + where+" top 1");
            int n = 0;
            stmt.setMaxRows(1);
            stmt.setObject(n++, value, value.length);
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

    public void updateBlobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, InputStream value, int length)
            throws SQLException, IOException {

        PreparedStatement stmt = null;
        if(m_logger.isDebugEnabled())
         m_logger.info("updata blob cloumn sql:" + "update " + table + " set " + column + " = ? "
                + where);

        try {
            stmt = conn.prepareStatement("update " + table + " set " + column + " = ? "
                    + where);
            int n = 1;
            //stmt.setMaxRows(1);
            stmt.setBinaryStream(n++, value,length);
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

    public EXSql sqlToExSql(SQLException e, Message Message) {
        String sqlState = e.getSQLState();
        EXSql Exsql = null;

        if (sqlState.equalsIgnoreCase("08S01")) {
            Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___DB_CONNECTION, e, Message);
        }

        if (sqlState.equalsIgnoreCase("HYT00")) {
            Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___STATEMENT_TIME_OUT, e, Message);
        }

        if (Exsql == null) {
            Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___OTHER, e, Message);
        }

        return Exsql;
    }

    public boolean isSql2005Up(String version) {
        boolean result = false;
        String temp = version.substring(0, 2);
        int ver = Integer.parseInt(temp);
        switch (ver) {
            case 6:
            case 7:
            case 8:
                result = false;
                break;
            case 9:
                result = true;
                break;
            case 10:
                result = true;
                break;
            case 11:
                result = true;
        }
        return result;
    }

}

