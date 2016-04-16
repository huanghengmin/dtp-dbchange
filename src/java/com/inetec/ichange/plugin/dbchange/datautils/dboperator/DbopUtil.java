/*=============================================================
* �ļ�����: DbopUtil.java
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

import org.apache.log4j.Logger;
import org.apache.commons.dbcp.PoolableConnection;

import java.sql.*;
import java.math.BigDecimal;

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.common.config.nodes.Type;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class DbopUtil {

    private static Logger m_logger = Logger.getLogger(DbopUtil.class);


    public final static int[] jdbcTypeArray = {
            Types.ARRAY, Types.BIGINT, Types.BINARY, Types.BIT, Types.BLOB,
            Types.CHAR, Types.CLOB, Types.DATE, Types.DECIMAL, Types.DISTINCT,
            Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.JAVA_OBJECT, Types.LONGVARBINARY,
            Types.LONGVARCHAR, Types.NULL, Types.NUMERIC, Types.OTHER, Types.REAL,
            Types.REF, Types.SMALLINT, Types.STRUCT, Types.TIME, Types.TIMESTAMP,
            Types.TINYINT, Types.VARBINARY, Types.VARCHAR
    };


    public final static String[] jdbcTypeStringArray = {
            "ARRAY", "BIGINT", "BINARY", "BIT", "BLOB",
            "CHAR", "CLOB", "DATE", "DECIMAL", "DISTINCT",
            "DOUBLE", "FLOAT", "INTEGER", "JAVA_OBJECT", "LONGVARBINARY",
            "LONGVARCHAR", "NULL", "NUMERIC", "OTHER", "REAL",
            "REF", "SMALLINT", "STRUCT", "TIME", "TIMESTAMP",
            "TINYINT", "VARBINARY", "VARCHAR"
    };


    public static void closeStatement(Statement st) {
        try {
            if (st != null) {
                st.close();
                st = null;
            }
        } catch (SQLException eEx) {
        }
    }

    /**
     * public static void closeConnection(Connection conn) {
     * try {
     * if (conn != null) {
     * conn.close();
     * conn = null;
     * }
     * } catch (SQLException eEx) {
     * }
     * }
     */


    public static void setAutoCommit(Connection conn) throws SQLException {
        boolean nowCommit = conn.getAutoCommit();
        if (!nowCommit) {
            conn.setAutoCommit(true);
        }
    }

    public static void setNotAutoCommit(Connection conn) throws SQLException {
        boolean nowCommit = conn.getAutoCommit();
        if (nowCommit) {
            conn.setAutoCommit(false);
        }
    }

    public static void commit(Connection conn) throws SQLException {
        boolean nowCommit = conn.getAutoCommit();
        if (!nowCommit) {
            conn.commit();
        }
    }


    public static int getJdbcType(String type) {
        int size = jdbcTypeStringArray.length;
        for (int i = 0; i < size; i++) {
            if (type.equalsIgnoreCase(jdbcTypeStringArray[i])) {
                return jdbcTypeArray[i];
            }
        }

        m_logger.error("unsupported jdbc type string:" + type);
        return Types.VARCHAR;
    }

    public static String getJdbcTypeString(int type) {
        int size = jdbcTypeArray.length;
        for (int i = 0; i < size; i++) {
            if (type == jdbcTypeArray[i]) {
                return jdbcTypeStringArray[i];
            }
        }

        m_logger.error("unsupported jdbc type indeEx:" + type);
        return "VARCHAR";
    }


    public static boolean isLobType(int jdbcType) {
        return isBlobType(jdbcType) || isClobType(jdbcType);
    }

    public static boolean isBlobType(int jdbcType) {
        switch (jdbcType) {
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return true;
        }

        return false;
    }

    public static boolean isClobType(int jdbcType) {
        switch (jdbcType) {
            case Types.CLOB:
            case Types.LONGVARCHAR:
                return true;
        }

        return false;
    }


    public static Object getObjectFromString(int jdbcType, String value) {

        if (value == null) {
            return null;
        }

        value = value.trim();

        switch (jdbcType) {
            case Types.BIT:
                return Boolean.valueOf(value);
            case Types.TINYINT:
                return Byte.valueOf(value);
            case Types.SMALLINT:
                return Short.valueOf(value);
            case Types.INTEGER:
                return Integer.valueOf(value);
            case Types.BIGINT:
                return Long.valueOf(value);
            case Types.FLOAT:
                return Float.valueOf(value);
            case Types.REAL:        //not specified
            case Types.DOUBLE:
                return Double.valueOf(value);
            case Types.DECIMAL:     //not specified
            case Types.NUMERIC:
                return new BigDecimal(value);
            case Types.CHAR:        //not specified
            case Types.VARCHAR:
                return value;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return Timestamp.valueOf(value);
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
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
                m_logger.error("can not get a object from string value:" + value);
                m_logger.error("jdbcType:" + getJdbcTypeString(jdbcType));
                break;
        }

        return value;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                m_logger.error("Failed to release the connection.");
                PoolableConnection poolConn = (PoolableConnection) conn;
                try {
                    poolConn.reallyClose();
                } catch (Exception eEx) {
                    m_logger.warn("Failed to really close the connection.");
                }
            }
        }
    }

    public static boolean verifierColumn(Column srcColumn, Column destColumn) {
        int srcType = srcColumn.getJdbcType();
        int destType = destColumn.getJdbcType();
        boolean result = false;
        if (srcType == destType) {
            result = true;
            return result ;
        }
        switch (srcType) {
            case Types.BIT :
                 if (destType == Types.VARCHAR||destType==Types.BOOLEAN)
                    result = true;
                else
                    result = false;
                break;
            case Types.BOOLEAN :
                 if (destType == Types.VARCHAR||destType==Types.BIT)
                    result = true;
                else
                    result = false;
                break;
            case Types.INTEGER:
                if (destType == Types.VARCHAR||destType==Types.FLOAT||destType==Types.REAL||destType==Types.DOUBLE||destType==Types.NUMERIC)
                    result = true;
                else
                    result = false;
                break;
            case Types.BIGINT:
                if (destType == Types.VARCHAR||destType==Types.FLOAT||destType==Types.REAL||destType==Types.DOUBLE||destType==Types.NUMERIC||destType==Types.INTEGER)
                    result = true;
                else
                    result = false;

                break;
            case Types.FLOAT:
                if (destType == Types.VARCHAR||destType==Types.REAL||destType==Types.DOUBLE||destType==Types.DECIMAL||destType==Types.NUMERIC)
                    result = true;
                else
                    result = false;
                break;
            case Types.REAL:
                if (destType == Types.VARCHAR||destType==Types.NUMERIC)
                    result = true;
                else
                    result = false;
                break;
            case Types.DOUBLE:
                if (destType == Types.VARCHAR||destType==Types.NUMERIC)
                    result = true;
                else
                    result = false;
                break;
            case Types.DECIMAL:
                if (destType == Types.VARCHAR)
                    result = true;
                else
                    result = false;
                break;
            case Types.NUMERIC:
                if (destType == Types.VARCHAR)
                    result = true;
                break;
            case Types.CHAR:
                if (destType == Types.VARCHAR)
                    result = true;
                else
                    result = false;
                break;
            case Types.VARCHAR:
                if (destType == Types.VARCHAR)
                    result = true;
                break;
            case Types.DATE:
                if (destType == Types.VARCHAR||destType== Types.TIMESTAMP)
                    result = true;
                else
                    result = false;
                break;
            case Types.TIME:
                if (destType == Types.VARCHAR||destType== Types.TIMESTAMP)
                    result = true;
                else
                    result = false;
                break;
            case Types.TIMESTAMP:
                if (destType == Types.VARCHAR||destType== Types.DATE||destType == Types.TIME)
                    result = true;
                else
                    result = false;
                break;
            case Types.BLOB:
                switch (destType) {
                    case Types.BINARY:
                    case Types.LONGVARBINARY:
                    case Types.VARBINARY:
                        result = true;
                        break;
                    default :
                        result = false;
                        break;
                }
                break;
            case Types.BINARY:
                switch (destType) {
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                    case Types.VARBINARY:
                        result = true;
                        break;
                    default :
                        result = false;
                        break;
                }
                break;
            case Types.LONGVARBINARY:
                switch (destType) {
                    case Types.BLOB:
                    case Types.BINARY:
                    case Types.VARBINARY:
                        result = true;
                        break;
                    default :
                        result = false;
                        break;
                }
                break;
            case Types.VARBINARY:
                switch (destType) {
                    case Types.BLOB:
                    case Types.BINARY:
                    case Types.LONGVARBINARY:
                        result = true;
                        break;
                    default :
                        result = false;
                        break;
                }
                break;
            case Types.CLOB:
                switch (destType) {
                    case Types.LONGVARCHAR:
                        result = true;
                        break;
                    default :
                        result = false;
                        break;
                }
                break;
            case Types.LONGVARCHAR:
                switch (destType) {
                    case Types.CLOB:
                        result = true;
                        break;
                    default :
                        result = false;
                        break;
                }
                break;
            case Types.NULL:
                result = true;
                break;
            case Types.DISTINCT:
                result = false;
                break;
            case Types.JAVA_OBJECT:
                result = false;
                break;
            case Types.OTHER:
                result = false;
                break;
            case Types.ARRAY:
                result = false;
                break;
            case Types.REF:
                result = false;
                break;
            case Types.STRUCT:
                result = false;
                break;
        }
        return result;
    }
}
