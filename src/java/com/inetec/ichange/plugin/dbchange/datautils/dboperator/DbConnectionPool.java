/*=============================================================
 * 文件名称: DbConnectionPool.java
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
