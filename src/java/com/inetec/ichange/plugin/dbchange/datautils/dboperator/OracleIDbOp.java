package com.inetec.ichange.plugin.dbchange.datautils.dboperator;

import com.inetec.common.util.DateUtils;
import com.inetec.ichange.plugin.dbchange.utils.MyJGeometry;
import net.sf.json.JSONObject;
import oracle.jdbc.OracleConnection;
import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;
import org.apache.log4j.Category;

import java.sql.*;
import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.math.BigDecimal;

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.db.Value;
import com.inetec.ichange.plugin.dbchange.datautils.DataInformation;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.utils.ReaderUtil;
import com.inetec.common.i18n.Message;


/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class OracleIDbOp extends DefaultDbOp {
    /**
     * The Category used to log messages for our class.
     */
    protected static final Category logger = Category.getInstance(OracleIDbOp.class);


    public final static String[][] oracle2jdbc = {
            {"CHAR", "CHAR"},
            {"VARCHAR2", "VARCHAR"},
            {"LONG", "LONGVARCHAR"},
            {"NUMBER", "NUMERIC"},
            {"RAW", "VARBINARY"},
            {"LONGRAW", "LONGVARBINARY"},
            {"DATE", "DATE"},
            {"BLOB", "BLOB"},
            {"CLOB", "CLOB"}
    };

    public OracleIDbOp(Connection conn) throws SQLException {
        super(conn);
    }

    public OracleIDbOp(Connection conn, String sqlBundleName) throws SQLException {
        super(conn, sqlBundleName);
    }


    protected String[] getTableNameList(Connection conn, String schemaName)
            throws SQLException {
        String types[] = {
                "TABLE"
        };
        ArrayList tableList = new ArrayList();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tabelSet = metaData.getTables(null, schemaName.toUpperCase(), null, types);
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
                basicValue = rs.getDate(indeEx) + "";
                break;
            case Types.TIME:
                basicValue = rs.getTime(indeEx) + "";
                break;
            case Types.TIMESTAMP:

                basicValue = rs.getTimestamp(indeEx) + "";
                break;

            case Types.LONGVARBINARY: {
                InputStream is = rs.getBinaryStream(indeEx);
                if (is == null) {
                    column.setValue(new Value((InputStream) null, (long) 0));
                } else {
                    try {
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
            case Types.VARBINARY: {
                InputStream is = rs.getBinaryStream(indeEx);
                if (is == null) {
                    column.setValue(new Value((InputStream) null, (long) 0));
                } else {
                    long len = 0;
                    try {
                        len = is.available();
                    } catch (IOException e) {
                        logger.error("Incorrect to get source stream length.");
                    }
                    column.setValue(new Value(is, len));
                }
                break;
            }
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
            case Types.LONGVARCHAR: {
                Reader reader = null;
                long len = 0;
                /*
                // for non-chinese character
                InputStream is = rs.getAsciiStream(indeEx);
                try {
                    len = is.available();
                } catch (IOException e) {
                    logger.error("Incorrect to get reader length.", e);
                }
                reader = new InputStreamReader(is);
                if (reader==null) {
                    fieldsetValue(new Value((Reader)null, (long)0));
                } else {
                    fieldsetValue(new Value(reader, len));
                }
                break;
                */

                Object object = rs.getObject(indeEx);
                // Reader reader = rs.getCharacterStream(indeEx);
                if (object == null) {
                    column.setValue(new Value((Reader) null, (long) 0));
                } else {
                    if (object instanceof String) {
                        String value = (String) object;
                        len = value.length();
                        reader = new StringReader(value);
                    } else if (object instanceof byte[]) {
                        byte[] buf = (byte[]) object;
                        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
                        len = buf.length;
                        reader = new InputStreamReader(inputStream);
                    } else if (object instanceof Clob) {
                        Clob clob = (Clob) object;
                        len = clob.length();
                        try {
                            reader = getClobReader(clob.getCharacterStream());
                        } catch (IOException e) {
                            logger.error("Incorrect to get clob Reader.");
                        }
                    } else {
                        throw new SQLException("Unable to handle LONGVARCHAR type...");
                    }

                    column.setValue(new Value(reader, len));
                }
                break;

            }
            case Types.CLOB: {
                Reader reader = null;
                Clob clob = rs.getClob(indeEx);
                if (clob == null) {
                    column.setValue(new Value((Reader) null, (long) 0));
                } else {
                    long len = clob.length();
                    try {
                        reader = getClobReader(clob.getCharacterStream());
                    } catch (IOException e) {
                        logger.error("Incorrect to get clob Reader.");
                    }
                    column.setValue(new Value(reader, len));
                }
                break;
            }
            // not supported: belows
            case Types.STRUCT:{
                Struct struct = (Struct) rs.getObject(indeEx);
                if(struct.getSQLTypeName().indexOf("MDSYS")>-1
                        ||struct.getSQLTypeName().indexOf("SDO_GEOMETRY")>-1) {

                    int gtype = ((BigDecimal) struct.getAttributes()[0]).intValue();
                    STRUCT st = STRUCT.toSTRUCT(struct, (OracleConnection) null);
                    JGeometry jGeometry = JGeometry.load(st);
                    int srid = jGeometry.getSRID();
                    double[] xyz = jGeometry.getLabelPointXYZ();
                    double x = xyz[0];
                    if(Double.isNaN(x)){
//                    System.out.println("x is NaN");
                    }
                    int[] elemInfos = jGeometry.getElemInfo();
                    double[] ordinates = jGeometry.getOrdinatesArray();
                    String elem = null;
                    String ordi = null;
                    if(elemInfos != null && elemInfos.length>0) {
                        elem = String.valueOf(elemInfos[0]);
                        for (int i = 1; i < elemInfos.length; i ++) {
                            elem += "|" + elemInfos[i];
                        }
                    }
                    if(ordinates != null && ordinates.length>0) {
                        ordi = String.valueOf(ordinates[0]);
                        for (int i = 1; i < ordinates.length; i ++) {
                            ordi += "|" + ordinates[i];
                        }
                    }
                    basicValue = "{g:"+gtype+",s:"+srid+
                            ",x:'"+xyz[0]+"',y:'"+xyz[1]+"',z:'"+xyz[2]+
                            "',e:'"+elem+"',o:'"+ordi+"'}";
                }
                break;
            }
            case Types.NULL:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.ARRAY:
            case Types.REF:

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
                                    prepStmt.setBoolean(i + 1, Boolean.valueOf(valueString).booleanValue());
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
                                    if (valueString.equalsIgnoreCase("true")) {
                                        prepStmt.setBigDecimal(i + 1, new BigDecimal(1));
                                        break;
                                    } else if (valueString.equalsIgnoreCase("false")) {
                                        prepStmt.setBigDecimal(i + 1, new BigDecimal(0));
                                        break;
                                    } else {
                                        prepStmt.setBigDecimal(i + 1, new BigDecimal(valueString));
                                        break;
                                    }
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(i + 1, valueString);
                                    break;
                                case Types.DATE:
                                    if (valueString.length() > 10) {
                                        valueString = valueString.substring(0, 10);
                                    }
                                    prepStmt.setDate(i + 1, Date.valueOf(valueString));
                                    break;
                                case Types.TIME:
                                    if (valueString.length() > 10) {
                                        valueString = valueString.substring(9);
                                    }
                                    prepStmt.setTime(i + 1, Time.valueOf(valueString));
                                    break;
                                case Types.TIMESTAMP:
                                    prepStmt.setTimestamp(i + 1, DateUtils.getTimestamp(valueString));
                                    break;
                                case Types.STRUCT:
                                    JSONObject obj = JSONObject.fromObject(valueString);
                                    int gtype = obj.getInt("g");
                                    int srid = obj.getInt("s");
                                    double x = obj.getDouble("x");
                                    double y = obj.getDouble("y");
                                    double z = obj.getDouble("z");
                                    int[] elemInfo = getIntegerArray(obj.getString("e"));
                                    double[] ordinate = getDoubleArray(obj.getString("o"));
        //                            JGeometry jGeometry = new JGeometry(gtype,srid,x,y,z,elemInfo,ordinate);

        //                            STRUCT struct = JGeometry.store(jGeometry, connection);
                                    STRUCT struct = MyJGeometry.restore(gtype, srid, x, y, z, elemInfo, ordinate, connection);
                                    prepStmt.setObject(i+1,struct);
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

    private int[] getIntegerArray(String elemInfo) {
        if("null".equals(elemInfo)) {
            return null;
        }
        String[] elems = elemInfo.split("\\|");
        int[] ele = new int[elems.length];
        for (int i = 0; i < elems.length; i ++) {
            String ee = elems[i];
            ele[i] = Integer.parseInt(elems[i]);
        }
        return ele;
    }

    private double[] getDoubleArray(String ordinate) {

        if("null".equals(ordinate)) {
            return null;
        }
        String[] ordi = ordinate.split("\\|");
        double[] o = new double[ordi.length];
        for (int i = 0; i < ordi.length; i ++) {
            o[i] = Double.parseDouble(ordi[i]);
        }
        return o;
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
                                    prepStmt.setBoolean(i + 1, Boolean.valueOf(valueString).booleanValue());
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
                                    if (valueString.equalsIgnoreCase("true")) {
                                        prepStmt.setBigDecimal(i + 1, new BigDecimal(1));
                                        break;
                                    } else if (valueString.equalsIgnoreCase("false")) {
                                        prepStmt.setBigDecimal(i + 1, new BigDecimal(0));
                                        break;
                                    } else {
                                        prepStmt.setBigDecimal(i + 1, new BigDecimal(valueString));
                                        break;
                                    }
                                case Types.CHAR:
                                case Types.VARCHAR:
                                    prepStmt.setString(i + 1, valueString);
                                    break;
                                case Types.DATE:
                                case Types.TIME:
                                case Types.TIMESTAMP:
                                    prepStmt.setTimestamp(i + 1, DateUtils.getTimestamp(valueString));
                                    break;

                                case Types.STRUCT:
                                    JSONObject obj = JSONObject.fromObject(valueString);
                                    int gtype = obj.getInt("g");
                                    int srid = obj.getInt("s");
                                    double x = obj.getDouble("x");
                                    double y = obj.getDouble("y");
                                    double z = obj.getDouble("z");
                                    int[] elemInfo = getIntegerArray(obj.getString("e"));
                                    double[] ordinate = getDoubleArray(obj.getString("o"));
        //                            JGeometry jGeometry = new JGeometry(gtype,srid,x,y,z,elemInfo,ordinate);
        //                            STRUCT struct = JGeometry.store(jGeometry, connection);
                                    STRUCT struct = MyJGeometry.restore(gtype, srid, x, y, z, elemInfo, ordinate, connection);
                                    prepStmt.setObject(i+1,struct);
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
     * @throws java.sql.SQLException if the driver encounters an error.
     * @throws java.io.IOException   if an I/O error occurs.
     */

    public void updateClobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, Reader value, int length)
            throws SQLException, IOException {
        boolean oldAutoCommit = conn.getAutoCommit();

        if (oldAutoCommit == true)
            conn.setAutoCommit(false);

        PreparedStatement stmt = null;
        CallableStatement call = null;

        try {
            stmt = conn.prepareStatement("select " + column + " from " + table + " " + where + " for update");

            if (params != null) {
                for (int i = 0; i < params.length; ++i)
                    stmt.setObject(i + 1, params[i]);
            }

            ResultSet rs = stmt.executeQuery();

            //Make sure there's something there.
            if (!rs.next())
                throw new SQLException("No such row. table=" + table + " " + where);

            //We must use getObject and not getClob here to avoid a WL bug.
            Object o = rs.getObject(1);
            Object streamObj = o;

            //Need to deal with NULL
            if (rs.wasNull()) {
                stmt.close();
                stmt = null;

                //Put an empty clob in the column.
                stmt = conn.prepareStatement("update " + table + " set " + column +
                        " = empty_clob() " + where);
                if (params != null) {
                    for (int i = 0; i < params.length; ++i) {
                        stmt.setObject(i + 1, params[i]);
                    }
                }
                stmt.executeUpdate();
                stmt.close();
                stmt = null;

                //Now select it back out. (Did I mention how much I love Oracle?)
                stmt = conn.prepareStatement("select " + column + " from " + table +
                        " " + where);
                if (params != null) {
                    for (int i = 0; i < params.length; ++i) {
                        stmt.setObject(i + 1, params[i]);
                    }
                }
                rs = stmt.executeQuery();
                rs.next();
                o = rs.getObject(1);
            }

            //What we're after is the stream to update the CLOB
            Writer out = null;

            //We're going to do some reflecting, so we need to catch
            //those exceptions
            try {

                Class oClass = o.getClass();
                String oName = oClass.getName();

                if (oName.equals("oracle.sql.CLOB")) {
                    streamObj = o;
                }

                // Look for an WebLogic wrapper hiding the real oracle.sql.CLOB
                else if (oName.equals("weblogic.jdbc.rmi.internal.OracleTClobImpl")) {

                    java.lang.reflect.Field field = oClass.getDeclaredField("t2_clob");
                    field.setAccessible(true);
                    streamObj = field.get(o);

                } else {
                    throw new SQLException("UneExpected CLOB type: " + oName);
                }

                //Here we call oracle.sql.CLOB.getCharacterOutputStream();
                //We do it this way to a void a compile-time reference to the Oracle driver.

                Class[] types = {};
                Method method =
                        streamObj.getClass().getDeclaredMethod("getCharacterOutputStream", types);
                Object[] args = {};
                out = (Writer) method.invoke(streamObj, args);

            } catch (InvocationTargetException eEx) {
                Throwable t = eEx.getTargetException();
                if (t instanceof SQLException)
                    throw (SQLException) t;
                else
                    throw new SQLException("UneExpected throwable during CLOB update: " +
                            t.getMessage());
            } catch (Exception eEx) {
                throw new SQLException("Error reflecting the CLOB: " + eEx.getMessage());
            }
            //todo:���Reader ����
            ReaderUtil readerUtil = new ReaderUtil(value);
            char[] buf = new char[8192];

            for (; ; ) {
                int nRead = readerUtil.getReader().read(buf, 0, buf.length);
                if (nRead == -1) break;
                out.write(buf, 0, nRead);
            }
            out.flush();
            out.close();

            stmt.close();
            stmt = null;

            //Now we need to set the length
            call = conn.prepareCall("begin dbms_lob.trim(?,?); end;");
            call.setObject(1, streamObj);
            call.setInt(2, (int) readerUtil.getLength());
            call.executeUpdate();

            call.close();
            call = null;

        } finally {
            try {
                if (call != null) call.close();
            } catch (Exception eEx) {
            }
            DbopUtil.closeStatement(stmt);
        }


        if (!conn.getAutoCommit()) {
            conn.commit();
        }

        if (conn.getAutoCommit() != oldAutoCommit)
            conn.setAutoCommit(oldAutoCommit);


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

        boolean oldAutoCommit = conn.getAutoCommit();

        if (oldAutoCommit == true)
            conn.setAutoCommit(false);

        PreparedStatement stmt = null;
        CallableStatement call = null;

        try {
            stmt = conn.prepareStatement("select " + column + " from " + table + " " + where + " for update");


            if (params != null) {
                for (int i = 0; i < params.length; ++i)
                    stmt.setObject(i + 1, params[i]);
            }

            ResultSet rs = stmt.executeQuery();

            //Make sure there's something there.
            if (!rs.next())
                throw new SQLException("No such row. table=" + table + " " + where);

            //We must use getObject and not getBlob here to avoid a WL bug.
            Object o = rs.getObject(1);
            Object streamObj = o;

            //Need to deal with NULL
            if (rs.wasNull()) {
                stmt.close();
                stmt = null;

                //Put an empty blob in the column.
                stmt = conn.prepareStatement("update " + table + " set " + column +
                        " = empty_blob() " + where);
                if (params != null) {
                    for (int i = 0; i < params.length; ++i) {
                        stmt.setObject(i + 1, params[i]);
                    }
                }
                stmt.executeUpdate();
                stmt.close();
                stmt = null;

                //Now select it back out. (Did I mention how much I love Oracle?)
                stmt = conn.prepareStatement("select " + column + " from " + table +
                        " " + where);
                if (params != null) {
                    for (int i = 0; i < params.length; ++i) {
                        stmt.setObject(i + 1, params[i]);
                    }
                }
                rs = stmt.executeQuery();
                rs.next();
                o = rs.getObject(1);
            }

            //What we're after is the stream to update the BLOB
            OutputStream out = null;

            //We're going to do some reflecting, so we need to catch
            //those exceptions
            try {

                Class oClass = o.getClass();
                String oName = oClass.getName();

                if (oName.equals("oracle.sql.BLOB") || oName.equals("weblogic.jdbc.rmi.internal.OracleTBlobStub")) {
                    streamObj = o;
                }

                // Look for an WebLogic wrapper hiding the real oracle.sql.BLOB
                else if (oName.equals("weblogic.jdbc.rmi.internal.OracleTBlobImpl")) {

                    java.lang.reflect.Field field = oClass.getDeclaredField("t2_blob");
                    field.setAccessible(true);
                    streamObj = field.get(o);
                } else {
                    throw new SQLException("UneExpected BLOB type: " + oName);
                }

                //Here we call oracle.sql.BLOB.getBinaryOutputStream.
                //We do it this way to avoid a compile-time reference to the Oracle driver.

                Class[] types = {};
                Method method =
                        streamObj.getClass().getDeclaredMethod("getBinaryOutputStream", types);
                Object[] args = {};
                out = (OutputStream) method.invoke(streamObj, args);

            } catch (InvocationTargetException eEx) {
                Throwable t = eEx.getTargetException();
                if (t instanceof SQLException)
                    throw (SQLException) t;
                else
                    throw new SQLException("UneExpected throwable during BLOB update: " +
                            t.getMessage());
            } catch (Exception eEx) {
                throw new SQLException("Error reflecting the BLOB: " + eEx.getClass().getName() + " " + eEx.getMessage());
            }

            byte[] buf = new byte[8192];
            for (; ; ) {
                int nRead = value.read(buf, 0, buf.length);
                if (nRead == -1) break;
                out.write(buf, 0, nRead);
            }
            out.flush();
            out.close();

            stmt.close();
            stmt = null;

            //Now we need to set the length


            call = conn.prepareCall("begin dbms_lob.trim(?,?); end;");

            call.setObject(1, streamObj);
            call.setInt(2, length);
            call.executeUpdate();


            call.close();
            call = null;


        } finally {
            try {
                if (call != null) call.close();
            } catch (Exception eEx) {
            }
            DbopUtil.closeStatement(stmt);
        }


        if (!conn.getAutoCommit()) {
            conn.commit();
        }

        if (conn.getAutoCommit() != oldAutoCommit)
            conn.setAutoCommit(oldAutoCommit);

    }


    /**
     * for longraw field
     *
     * @param conn
     * @param table
     * @param column
     * @param where
     * @param params
     * @param value
     * @param length
     * @throws SQLException
     * @throws IOException
     */

    public void updateLongRawColumn(Connection conn, String table, String column,
                                    String where, Object[] params, InputStream value, int length)
            throws SQLException, IOException {

        PreparedStatement stmt = null;
        if (logger.isDebugEnabled()) {
            logger.debug("longraw column length: " + value.available());
        }

        try {
            String sql = "update " + table + " set " + column + "=? "
                    + where;
            if (logger.isDebugEnabled()) {
                logger.debug("Update longraw column sql: " + sql);
            }
            stmt = conn.prepareStatement(sql);
            int n = 1;
            stmt.setBinaryStream(n++, value, length);
            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    stmt.setObject(n++, params[i]);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Update longraw column params: " + params[i] + "");
                    }

                }
            }
            try {
                stmt.executeUpdate();
            } catch (Exception e) {
                logger.error("Failed to eExecute update longraw data.", e);
            }
            stmt.close();
            stmt = null;
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException eEx) {
            }
        }
    }


    /**
     * for long type
     *
     * @param conn
     * @param table
     * @param column
     * @param where
     * @param params
     * @param value
     * @param length
     * @throws SQLException
     * @throws IOException
     */

    public void updateLongColumn(Connection conn, String table, String column,
                                 String where, Object[] params, Reader value, int length)
            throws SQLException, IOException {


        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("update " + table + " set " + column + " = ? "
                    + where);
            int n = 1;

            stmt.setCharacterStream(n++, value, length);

            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    stmt.setObject(n++, params[i]);
                }
            }

            try {
                stmt.executeUpdate();
            } catch (Exception eEx) {
                logger.error("Failed to eExecute the preparedstatement for clob!", eEx);
            }

            stmt.close();
            stmt = null;
        } finally {
            DbopUtil.closeStatement(stmt);
        }
    }


    public int getJdbcTypeFromVenderDb(String type) {
        int size = oracle2jdbc.length;
        for (int i = 0; i < size; i++) {
            if (type.equalsIgnoreCase(oracle2jdbc[i][0])) {
                return DbopUtil.getJdbcType(oracle2jdbc[i][1]);
            }
        }
        return Types.VARCHAR;
    }

    public EXSql sqlToExSql(SQLException e, Message Message) {
        int errorCode = e.getErrorCode();
        EXSql Exsql = null;
        switch (errorCode) {
            case 17002:
                Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___DB_CONNECTION, e, Message);
                break;
            case 17016:
                Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___STATEMENT_TIME_OUT, e, Message);
                break;
            default:
                Exsql = (EXSql) new EXSql().set(ErrorSql.ERROR___OTHER, e, Message);
                break;
        }

        return Exsql;
    }

    private Reader getClobReader(Reader reader) throws IOException {
        Reader reader1 = null;
        CharArrayWriter writer = new CharArrayWriter();
        char buf[] = new char[8192];
        int length = 0;
        while ((length = reader.read(buf)) > 0) {
            writer.write(buf, 0, length);
        }
        reader1 = new CharArrayReader(writer.toCharArray());

        return reader1;
    }
}

