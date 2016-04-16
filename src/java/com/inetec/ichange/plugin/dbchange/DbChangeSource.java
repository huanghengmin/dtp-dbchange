package com.inetec.ichange.plugin.dbchange;

import com.inetec.ichange.api.*;
import com.inetec.ichange.plugin.dbchange.source.SourceDbOperation;
import com.inetec.ichange.plugin.dbchange.source.info.DbChangeSourceInfo;
import com.inetec.ichange.plugin.dbchange.source.info.DatabaseInfo;
import com.inetec.ichange.plugin.dbchange.source.info.TableInfo;
import com.inetec.ichange.plugin.dbchange.source.twoway.SourceObjectCache;
import com.inetec.ichange.plugin.dbchange.target.info.TableMap;
import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import com.inetec.common.config.nodes.IChange;
import com.inetec.common.config.nodes.Type;
import com.inetec.common.config.nodes.Plugin;
import com.inetec.common.logs.LogHelper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;


public class DbChangeSource implements ISourcePlugin {

    public final static Logger m_logger = Logger.getLogger(DbChangeSource.class);
    public static String Str_DbChangeType = "IChange DbChange";
    public LogHelper m_log = null;
    private SourceDbOperation m_dbOperations[] = new SourceDbOperation[0];
    private SourceObjectCache m_objectCache = new SourceObjectCache();


    private boolean m_bConfigured = false;
    private IChangeType m_changeType = null;
    private IChangeMain m_dc = null;
    private ITargetPlugin m_target = null;
    private DbChangeTarget m_dbtarget = null;

    public static final String Str_Change = "Change";
    public static final String Str_ChangeTime = "ChangeTime";
    public static final String Str_processTime = "processTime";
    public static final String Str_syncDatabase = "ChangeDatabase";


    public DataAttributes externalData(InputStream in, DataAttributes dp) throws Ex {
        DataAttributes dp1 = dp;

        dp1 = m_dc.dispose(m_changeType, in, dp);
        try {
            in.close();
        } catch (IOException e) {
            m_logger.error("close InputStream is Faild", e);
            m_log.error("close InputStream is Faild.", e);
        }

        return dp1;
    }


    /**
     * this method is used by the data ichange or another plugin to
     * send a control package to this source adapter.
     *
     * @param command   command: connect, disconnect, controlwrite, or getstatus; or
     *                  other customer control command
     * @param dataProps Nullable: no attributes when null. Attributes as parameters of the command.
     * @throws Ex
     */
    public DataAttributes control(String command, DataAttributes dataProps) throws Ex {
        DataAttributes dp = dataProps;
        if (command == null) {
            command = "";
        }

        if (IChangeMain.Str_ControlGetStatus.equalsIgnoreCase(command)) {
            /* dp.setProperty(DataAttributes.Str_Running, "" + isRunning());
         dp.setProperty(DataAttributes.Str_Busying, "" + isBusy());*/
        } else if (Str_Change.equalsIgnoreCase(command)) {
            boolean result = false;
            String syncDb = dataProps.getValue(DbChangeSource.Str_syncDatabase);
            String temp = dataProps.getValue(DbChangeSource.Str_ChangeTime);

            if (syncDb != null && temp != null) {

                long syncTime = 0;
                try {
                    if (temp.equals("")) {
                        result = false;
                    } else {
                        syncTime = Long.parseLong(temp);
                        result = syncForOneTableTwoway(syncDb, syncTime);
                    }
                } catch (NumberFormatException e) {
                    dp.setStatus(Status.S_Faild_TargetProcess);
                    dp.setErrorMessage("Change Time format failed.");
                } catch (Ex Ex) {
                    dp.setStatus(Status.S_Faild_TargetProcess);
                    dp.setErrorMessage(Ex.getMessage());
                    //m_logger.warn("Change Time format failed.");
                    //m_log.warn("Change Time format failed.");
                }

                if (result) {
                    dp.setStatus(Status.S_Success_TargetProcess);
                } else {
                    dp.setStatus(Status.S_Faild_TargetProcess);
                }
            }
        } else {
            throw new Ex().set(E.E_OperationFailed, new Message("the command {0} is not recognized by the source of {1}.", command, m_changeType.getType()));
        }

        return dp;
    }


    public SourceObjectCache getObjectCache() {
        return m_objectCache;
    }


    public TableInfo getTableInfo(String dbName, String tableName) {
        for (int i = 0; i < m_dbOperations.length; i++) {
            SourceDbOperation dbOperation = m_dbOperations[i];
            if (dbOperation.getDbName().equalsIgnoreCase(dbName)) {
                return dbOperation.findTableInfo(tableName);
            }
        }

        return null;
    }

    public boolean isTriggerEnable(String dbName) {
        for (int i = 0; i < m_dbOperations.length; i++) {
            SourceDbOperation dbOperation = m_dbOperations[i];
            if (dbOperation.getDbName().equalsIgnoreCase(dbName)) {
                return dbOperation.isTriggerEnable();
            }
        }

        return false;
    }

    public boolean syncForOneTableTwoway(String dbname, long time) throws Ex {
        boolean result = false;
        DbChangeTarget output = (DbChangeTarget) m_target;

        SourceDbOperation sourceOperation = null;
        sourceOperation = findOperationByTarget(dbname);
        if (sourceOperation != null) {

            if (!sourceOperation.isChange()) {
                result = true;
                sourceOperation.setChangeTime();
                //sourceOperation.changeForTable(sourceOperation.getChangeTime());
            }
        }
        if (sourceOperation == null) {
            m_logger.warn("Change Two way not find sourceOperation.");
            m_log.warn("Change Two way not find sourceOperation.");
        }

        return result;
    }

    public SourceDbOperation findOperationBySource(String dbName) throws Ex {
        SourceDbOperation dbOperation = null;
        TableInfo tableInfo = null;
        for (int i = 0; i < m_dbOperations.length; i++) {
            if (m_dbOperations[i].m_dbName.equalsIgnoreCase(dbName)) {
                dbOperation = m_dbOperations[i];
            }
        }

        return dbOperation;
    }
    // TODO: END

    public SourceDbOperation findOperationByTarget(String dbName) throws Ex {
        SourceDbOperation sourceOperation = null;
        TableMap[] tableMaps = m_dbtarget.findTableMap(dbName);
        for (int i = 0; i < tableMaps.length; i++) {
            for (int j = 0; j < m_dbOperations.length; j++) {
                if (tableMaps[i].getTargetDb().equals(m_dbOperations[j].m_dbName)) {
                    sourceOperation = m_dbOperations[j];
                    break;
                }
            }
            if (sourceOperation != null) {
                break;
            }
        }
        return sourceOperation;

    }
    //
    //
    // for utest
    //
    //
    /*public void setUtestCallBack(Object ob, Method m) {
        backObject = ob;
        backMethod = m;
    }
*/

    public void init(IChangeMain iChangeMain, IChangeType iChangeType, ITargetPlugin iTargetPlugin) throws Ex {
        m_dc = iChangeMain;
        m_changeType = iChangeType;
        m_target = iTargetPlugin;
        m_log = iChangeMain.createLogHelper();
        m_dbtarget = (DbChangeTarget) m_target;
    }

    public DataAttributes start(DataAttributes dataAttributes) throws Ex {
        if (!m_bConfigured) {
            m_dc.setStatus(m_changeType, EStatus.E_CF_NotConfigured, "", true);
            throw new Ex().set(E.E_OperationFailed, new Message("Not config."));
        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("DatatBase Source size:" + m_dbOperations.length);
        }
        for (int i = 0; i < m_dbOperations.length; i++) {
            m_dbOperations[i].run();
        }
        dataAttributes.setStatus(Status.S_Success);
        return dataAttributes;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public DataAttributes stop() throws Ex {
        throw new Ex().set(E.E_NotImplemented, new Message("this method is implemented."));
    }


    public void config(IChange iChange) throws Ex {

        if (m_bConfigured) {
            m_dc.setStatus(m_changeType, EStatus.E_AlreadyConfigured, "Already Configured.", true);
            throw new Ex().set(E.E_OperationFailed, new Message("Already Configured."));
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("start config source dbchange, the config path is " + iChange);
        }
        Type type = iChange.getType(m_changeType.getType());
        DbChangeSource.Str_DbChangeType = type.getAppType();
        Plugin plugin = type.getPlugin();
        if (plugin == null) {
            m_dc.setStatus(m_changeType, EStatus.E_CF_Faild, "PlugIn is null.", true);
            throw new Ex().set(E.E_OperationFailed, new Message("PlugIn is null."));
        }
        DbChangeSourceInfo dbSyncInfo = new DbChangeSourceInfo(plugin, iChange);
        DatabaseInfo[] databaseInfos = dbSyncInfo.getDatabases();
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("DatabaseInfo size:" + databaseInfos.length);
        }
        m_dbOperations = new SourceDbOperation[databaseInfos.length];
        for (int i = 0; i < m_dbOperations.length; i++) {
            SourceDbOperation dbOperation = new SourceDbOperation(m_dc, m_changeType, this);
            dbOperation.setDatabaseInfo(databaseInfos[i]);
            m_dbOperations[i] = dbOperation;
        }

        m_bConfigured = true;
        m_dc.setStatus(m_changeType, EStatus.E_CONFIGOK, null, true);

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("finish config source dbchange.");
        }
        m_log.setAppName(m_changeType.getType());
        m_log.setAppType(Str_DbChangeType);
        m_log.setSouORDes(LogHelper.Str_souORDes_Source);
    }


    public boolean configred() {
        return m_bConfigured;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
