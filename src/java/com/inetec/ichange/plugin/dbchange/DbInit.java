/*=============================================================
 * ??????: DbInit.java
 * ??    ??: 1.0
 * ??    ??: bluewind
 * ???????: 2005-10-17
 * ============================================================
 * <p>???????  (c) 2005 ????????????????????</p>
 * <p>
 * ?????????????????????????????????????????????
 * ???????????????????????????????????????????á?
 * </p>
 * <p>
 * ??????????????????????????????????????????涨???????
 * ??????
 * </p>
 * <p>
 * ?????????????????????????????????????????????????
 * ??????ò????????????????????и???????????????????
 * ??δ????????????????????????????л??????????????????
 * ???????????????????????????????????????????????
 * </p>
 * ==========================================================*/
package com.inetec.ichange.plugin.dbchange;

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;

import com.inetec.common.i18n.Message;
import com.inetec.common.logs.LogHelper;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.IDbOp;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopFactory;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DBType;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DefaultDBTypeMapper;

import com.inetec.ichange.plugin.dbchange.exception.EDbChange;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.ichange.api.IChangeMain;
import com.inetec.ichange.api.EStatus;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import com.inetec.common.db.datasource.DatabaseSource;

public abstract class DbInit {
    // SQLException sleep time
    public final static int Int_Thread_SleepTime = 10 * 1000;
    public final static int Int_DatabaseRetry_SleepTime = 60 * 1000;

    public final static int I_StatementTimeOut = 5 * 60;
    public final static int I_MaxRows = 20000;
    public static Logger m_logger = Logger.getLogger(DbInit.class.getName());


    protected IChangeMain m_changeMain;
    public LogHelper m_log = null;
    protected String m_schemaName = "";
    protected String m_dbName;
    protected String m_dbDesc;
    protected String m_nativeCharSet = "";
    protected String m_driverClass = "";
    protected DatabaseSource m_dbSource = null;
    protected IDbOp m_I_dbOp = null;
    protected DBType m_dbType = null;

    //
    public static String Str_Format_Date = "yyyy-MM-dd HH:mm:ss";
    public final static int Int_DataBaseRetryMeanTime = 5 * 60 * 1000;
    private long m_fristTime = 0;
    private long m_lastTime = 0;
    protected long m_tempRowLastMaxRow = 0;


    public void initDbSource() throws Ex {
        if (m_dbSource == null) {
            m_dbSource = m_changeMain.findDataSource(m_dbName);
        }
        initDbOp();
    }

    private void initDbOp() {
        try {
            if (m_dbSource != null) {
                m_I_dbOp = DbopFactory.getDbms(m_dbSource.getConnection(), m_driverClass);
                m_dbType = DefaultDBTypeMapper.getDBType(m_dbSource.getConnection());
                initDbType();
            }
        } catch (RuntimeException e) {
            m_I_dbOp = null;
            m_logger.warn("DataSource create Connection error.");

        } catch (SQLException e) {
            m_I_dbOp = null;
            m_logger.warn("DataSource create Connection error.", e);
        }
    }

    protected void dataBaseRetryProcess(Connection con) {
        try {
            Thread.sleep(Int_DatabaseRetry_SleepTime);
        } catch (InterruptedException e) {
            //okay
        }

    }

    public String dataFormat(long time) {
        Date date = new Date(time);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Str_Format_Date);
        return simpleDateFormat.format(date);
    }

    public void setDatabaseFailingTime() {
        if (m_fristTime != 0) {
            m_lastTime = System.currentTimeMillis();
        } else {
            m_fristTime = System.currentTimeMillis();
            m_lastTime = m_fristTime;
        }

    }

    protected void testBadConnection(Connection conn, EXSql e) {
        if (conn != null && e.getErrcode().equals(ErrorSql.ERROR___DB_CONNECTION)) {
            Statement stmt = null;
            try {
                stmt = conn.createStatement();
            } catch (SQLException e2) {
                setDatabaseFailingTime();
                returnConnection(conn);
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ee) {
                    // ignore.
                }
            } finally {
                DbopUtil.closeStatement(stmt);
            }
        }

    }

    protected Connection getConnection() throws Ex {
        Connection conn = null;
        if (m_dbSource == null) {
            initDbSource();
        }
        if (m_dbSource == null) {
            return null;
        }
        try {
            conn = m_dbSource.getConnection();
            if (conn == null)
                throw new Ex().set(E.E_NullPointer, new Message("{0} Database create connection is error.", m_schemaName));
        } catch (Ex Ex) {
            setDatabaseFailingTime();
            throw Ex;
        }

        return conn;
    }


    protected void returnConnection(Connection con) {
        if (m_dbSource != null)
            m_dbSource.returnConnection(con);
        else {
            DbopUtil.closeConnection(con);
        }

    }

    public boolean isOkay() {
        boolean result = false;
        if (m_dbSource != null)
            result = m_dbSource.isOkay();
        return result;
    }

    public void testDb() {
        try {
            if (m_dbSource != null) {
                m_dbSource.testDatabase();
            } else
                initDbSource();
            if (m_I_dbOp == null) {
                initDbOp();
            }
        } catch (Ex ex) {
            m_logger.warn(m_dbName + "Test Database connection error.");
        }
    }

    public void sleepTime() {
        try {
            Thread.sleep(Int_Thread_SleepTime);
        } catch (InterruptedException ee) {
            // ignore.
        }
    }

    public void testStatus() throws Ex {
        if (!isOkay()) {
            Message Message = new Message("测试数据源:{0}.", m_dbName);
            m_logger.warn(Message.toString());
            if (m_dbSource != null) {
                m_log.setSource_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.warn(Message.toString());
            sleepTime();
            testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        if (!m_changeMain.isNetWorkOkay()) {
            Message Message = new Message("测试平台通道");
            m_logger.warn(Message.toString());
            if (m_dbSource != null) {
                m_log.setSource_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setStatusCode(EStatus.E_NetWorkError.getCode() + "");
            m_log.warn(Message.toString());
            sleepTime();
            throw new Ex().set(EDbChange.E_NetWorkError, Message);
        }
    }

    public void initDbType() {
        if (m_dbType != null) {
            if (m_dbType.equals(DBType.C_Oracle)) {
                m_schemaName = m_schemaName.toUpperCase();
            }
        }
    }

    public boolean isMssqlDB() {
        if (m_dbType != null) {
            return m_dbType.equals(DBType.C_MSSQL);
        }
        return false;
    }

    public boolean isOracleDB() {
        if (m_dbType != null) {
            return m_dbType.equals(DBType.C_Oracle);
        }
        return false;
    }
}
