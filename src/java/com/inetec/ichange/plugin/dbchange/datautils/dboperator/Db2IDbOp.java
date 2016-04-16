/*=============================================================
 * �ļ�����: Db2IDbOp.java
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

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.db.Value;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.common.i18n.Message;

import java.sql.*;
import java.util.ArrayList;
import java.io.*;

import org.apache.log4j.Category;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class Db2IDbOp extends DefaultDbOp {
    protected static final Category logger = Category.getInstance(Db2IDbOp.class);
      public final static String[][] db22jdbc = {
            {"CHARACTER", "CHAR"},
            {"VARCHAR", "VARCHAR"},
            {"LONG VARCHAR", "LONGVARCHAR"},
            {"GRAPHICS", "VARCHAR"},
            {"VARGRAPHICS", "LONGVARCHAR"},
            {"LONG VARGRAPHICS", "LONGVARCHAR"},
            {"TIMESTAMP", "TIMESTAMP"},
            {"TIME", "TIME"},
            {"INTEGER", "INTEGER"},
            {"smallint", "SMALLINT"},
            {"BIGINT", "BIGINT"},
            {"FLOAT", "DOUBLE"},
            {"DOUBLE", "DOUBLE"},
            {"DECIMAL", "DECIMAL"},
            {"NUMBER", "NUMERIC"},
            {"DATE", "DATE"},
            {"BLOB", "BLOB"},
            {"CLOB", "CLOB"},
            {"DBCLOB", "CLOB"}
    };

    public Db2IDbOp(Connection conn) throws SQLException {
        super(conn);
    }

    public Db2IDbOp(Connection conn, String sqlBundleName) throws SQLException {
        super(conn, sqlBundleName);
    }
    public int getJdbcTypeFromVenderDb(String type) {
            int size = db22jdbc.length;
            for (int i = 0; i < size; i++) {
                if (type.equalsIgnoreCase(db22jdbc[i][0])) {
                    return DbopUtil.getJdbcType(db22jdbc[i][1]);
                }
            }
            return Types.VARCHAR;
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
                basicValue = rs.getDate(indeEx)+ "";
            case Types.TIME:
                 basicValue = rs.getString(indeEx);
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
                Reader reader = null;

                Clob clob = rs.getClob(indeEx);
                if (clob == null) {
                    column.setValue(new Value((Reader) null, (long) 0));
                } else {
                    long len = clob.length();
                    try {
                        Reader reader1=clob.getCharacterStream();
                        if(reader1!=null&&len>0)
                            reader = getClobReader(reader1);
                        else {
                            reader =null;
                        }
                    } catch (IOException e) {
                        logger.error("Incorrect to get clob Reader.");
                    }
                    column.setValue(new Value(reader, len));
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
    private Reader getClobReader(Reader reader) throws IOException {
        Reader reader1 = null;
        CharArrayWriter writer = new CharArrayWriter();
        char buf[] = new char[1024];

        while (reader.read(buf) > 0) {
            writer.write(buf);
        }
        reader1 = new CharArrayReader(writer.toCharArray());

        return reader1;
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
}
