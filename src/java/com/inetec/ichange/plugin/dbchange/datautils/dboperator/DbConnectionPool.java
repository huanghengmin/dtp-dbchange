/*=============================================================
 * �ļ�����: DbConnectionPool.java
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

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-9
 * Time: 15:29:22
 * To change this template use File | Settings | File Templates.
 */
public class DbConnectionPool {


    private static Logger m_logger = Logger.getLogger(DbConnectionPool.class);
    private static final String Str_PoolName = "dbpool";
    private static final String Str_PoolingDriver = "org.apache.commons.dbcp.PoolingDriver";
    private static final String Str_PrefixMyPoolDriver = "jdbc:apache:commons:dbcp:";
    private static final int Int_MaxActivePoolSize = 4;
    private static final int Int_MinActivePoolSize = 2;
    GenericObjectPool m_objectPool;
    PoolingDriver m_poolDriver;
    private String m_context;
    private int m_maxPoolSize;
    private int m_minPoolSize;


    public DbConnectionPool(String poolContext) {
        m_objectPool = null;
        m_poolDriver = null;
        if (poolContext == null)
            m_context = Str_PoolName;
        else
            m_context = poolContext;
        m_objectPool = new GenericObjectPool(null);
        m_maxPoolSize = Int_MaxActivePoolSize;
        m_minPoolSize = Int_MinActivePoolSize;
    }

    public void release()
            throws Ex {
        if (m_objectPool != null) {
            try {
                DriverManager.deregisterDriver(m_poolDriver);
                m_poolDriver.closePool(m_context);
            } catch (SQLException e) {
                if (m_logger.isDebugEnabled())
                    m_logger.debug("Failed to deregister driver");
                throw (new Ex()).set(E.E_DatabaseError, e);
            }
            try {
                m_objectPool.close();
            } catch (Exception e) {
                if (m_logger.isDebugEnabled())
                    m_logger.debug("Failed to close database connection pool.");
                throw (new Ex()).set(E.E_IOException, e);
            }
        }
    }

    public void setupDriver(String driverClass, String url, String dbUser, String password)
            throws Exception {
        try {
            Class.forName(driverClass);
            Class.forName(Str_PoolingDriver);
        } catch (Exception e) {
            m_logger.error("Failed to load driver class, caused by " + e);
        }
        m_objectPool.setMaxActive(m_maxPoolSize);
        m_objectPool.setMaxIdle(m_maxPoolSize - m_minPoolSize);

        DriverManagerConnectionFactory driverFactory = new DriverManagerConnectionFactory(url, dbUser, password);
        PoolableConnectionFactory poolFactory = new PoolableConnectionFactory(driverFactory, m_objectPool, null, null, false, true);
        m_poolDriver = (PoolingDriver) DriverManager.getDriver(Str_PrefixMyPoolDriver);
        m_poolDriver.registerPool(m_context, m_objectPool);
    }

    public int getMaxPoolSize() {
        return m_maxPoolSize;
    }

    public void setMaxPoolSize(int maxConn) {
        m_maxPoolSize = maxConn;
    }

    public int getMinPoolSize() {
        return m_minPoolSize;
    }

    public void setMinPoolSize(int minConn) {
        m_minPoolSize = minConn;
    }

    public int getAvailableSize() {
        return m_objectPool.getNumIdle();
    }

    public String getPoolContext() {
        return m_context;
    }

    public synchronized Connection getConnection()
            throws Ex {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Str_PrefixMyPoolDriver + m_context);
        } catch (SQLException e) {
            m_logger.error("Failed to get a connection from the pool", e);
        }
        return conn;
    }

    public synchronized void closeConnection(Connection conn)
            throws Ex {
        try {
            conn.close();
        } catch (Exception e) {
            if (m_logger.isDebugEnabled())
                m_logger.debug("Failed to return connection.");
            throw (new Ex()).set(E.E_DatabaseError, e);
        }
    }

    public void trueCloseConnection(Connection conn)
            throws Ex {
        PoolableConnection poolConn = (PoolableConnection) conn;
        try {
            poolConn.reallyClose();
        } catch (Exception e) {
            if (m_logger.isDebugEnabled())
                m_logger.debug("Failed to really close connection.");
            throw (new Ex()).set(E.E_DatabaseError, e);
        }
    }

}
