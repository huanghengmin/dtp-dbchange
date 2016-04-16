package com.inetec.ichange.plugin.dbchange;

import com.inetec.ichange.api.*;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.target.TargetDbOperation;
import com.inetec.ichange.plugin.dbchange.target.info.ColumnMap;
import com.inetec.ichange.plugin.dbchange.target.info.TableMap;
import com.inetec.ichange.plugin.dbchange.target.info.TableMapSet;
import com.inetec.ichange.plugin.dbchange.target.info.FilterResult;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;
import com.inetec.ichange.plugin.dbchange.datautils.*;
import com.inetec.ichange.plugin.dbchange.source.info.JdbcInfo;


import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import com.inetec.common.config.nodes.*;
import com.inetec.common.logs.LogHelper;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;

import java.util.HashMap;

import org.apache.log4j.Logger;


public class DbChangeTarget implements ITargetPlugin {

    protected final static Logger m_logger = Logger.getLogger(ITargetPlugin.class);
    protected LogHelper m_log = null;
    public static String Str_DbChangeType = "IChange DbChange";

    private int[] m_cap = null;

    private TargetDbOperation[] m_dbOperations = new TargetDbOperation[0];
    private TableMapSet m_tableMaps = new TableMapSet();

    private boolean m_bConfigured = false;
    private ISourcePlugin m_source = null;
    private IChangeMain m_changeMain;
    private IChangeType m_changeType;


    public DataAttributes process(String collectionType, DataAttributes dataProps, InputStream is) throws Ex {
        String appType = collectionType;
        if (appType == null) {
            throw new Ex().set(E.E_NullPointer, new Message("can not get the apptype value from DataAttributes object"));
        }
        if (m_dbOperations.length == 0) {
            throw new Ex().set(E.E_OperationError, new Message("Target Database is null."));
        }
        for (int i = 0; i < m_dbOperations.length; i++) {
            if (!m_dbOperations[i].isOkay()) {
                Message Message = new Message("数据源:{0}.", m_dbOperations[i].m_dbName);
                m_logger.warn(Message.toString());
                m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                m_log.warn(Message.toString());
                m_dbOperations[i].sleepTime();
                m_dbOperations[i].testDb();
                throw new Ex().set(E.E_DatabaseConnectionError);
            }
        }
//        OutputStream out = null;
//        try {
//            out = new FileOutputStream("F:/ichange/data/test_s.tmp");
//            int len = 0;
//                byte[] buff = new byte[1024];
//                while ((len = is.read(buff))!=-1) {
//                    out.write(buff,0,len);
//                    out.flush();
//                }
//
//
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }   finally {
//            try {
//                if(is !=null){
//                    is.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            try {
//                if(out !=null){
//                    out.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        InputStream in = null;
//        try {
//            in = new FileInputStream("F:/ichange/data/test_s.tmp");
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        ArrayList exceptions = new ArrayList();
        MDataParseImp dataConsumer = new MDataParseImp(is);
        try {
            if (!dataConsumer.isRecover()) {

                DataInformation basicDataInformation = dataConsumer.getBasicDataInfo();
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Get basic data...");
                }
                DefaultData defaultData = new DefaultData(basicDataInformation);
                String schemaName = defaultData.getSchemaName();
                String tableName = defaultData.getTableName();
                defaultData.close();
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Find table maps by DbChangeSource(" + schemaName + ", " + tableName + ")...");
                }
                TableMap[] tableMaps = m_tableMaps.findBySource(schemaName, tableName);
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("TableMapSet length is: " + tableMaps.length);
                }
                if (tableMaps.length == 0) {
                    throw new Ex().set(E.E_OperationError, new Message("Find table maps is empty by DbChangeSource {0}, tableName {1}", schemaName, tableName));
                }
                for (int i = 0; i < tableMaps.length; i++) {
                    TableMap tableMap = tableMaps[i];
                    String targetSchema = tableMap.getTargetDb();
                    try {
                        processOneTargetSchema(targetSchema, dataConsumer);
                        //dataConsumer.succeedToConsumer(targetSchema);
                    } catch (Ex Ex) {
                        exceptions.add(Ex);
                    } catch (IllegalArgumentException e) {
                        exceptions.add(new Ex().set(E.E_Unknown, e));
                    }
                }
                if (exceptions.size() > 0) {      // Exceptions occured
                    Message Message = new Message(exceptions.size() + " Errors occured in data processing; see below for details.");
                    m_logger.error(Message.toString());
                    int number = 0;
                    int n = exceptions.size();
                    for (int i = 0; i < n; i++) {
                        Ex Ex = (Ex) exceptions.get(i);
                        Message = new Message("Detailed information of error " + "" + (i + 1) + ": ");
                        m_logger.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        m_log.setStatusCode(EStatus.E_DbChangeTargetProcessFaild.getCode() + "");
                        m_log.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        if (Ex.getErrcode().equals(E.E_DatabaseConnectionError)) {
                            number++;
                        }
                    }
                    if (number == exceptions.size()) {
                        dataProps.setStatus(Status.S_Faild_TargetProcess);
                        return dataProps;
                    } else {
                        throw new Ex().set(E.E_Unknown, new Message(n + " Errors occured during data processing; error messages as detailed."));
                    }
                }
            }
        } finally {
            dataConsumer.close();
        }
        dataProps.setStatus(Status.S_Success_TargetProcess);
        return dataProps;
    }


    public DataAttributes process(String collectionType, DataAttributes dataProps, String filename) throws Ex {

        String appType = collectionType;
        if (appType == null) {
            throw new Ex().set(E.E_NullPointer, new Message("can not get the apptype value from DataAttributes object"));
        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to process " + filename);
        }
        if (m_dbOperations.length == 0) {
            throw new Ex().set(E.E_OperationError, new Message("Target Database is null."));
        }
        for (int i = 0; i < m_dbOperations.length; i++) {
            if (!m_dbOperations[i].isOkay()) {
                Message Message = new Message("数据源:{0}.", m_dbOperations[i].m_dbName);
                m_logger.warn(Message.toString());
                m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                m_log.warn(Message.toString());
                m_dbOperations[i].sleepTime();
                m_dbOperations[i].testDb();
                throw new Ex().set(E.E_DatabaseConnectionError);
            }
        }
        ArrayList exceptions = new ArrayList();
        MDataParseImp dataConsumer = new MDataParseImp(filename);

        try {
            if (dataConsumer.isRecover()) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Start to recover.");
                }
                DataInformation[] successDataInfos = dataConsumer.getSuccessDataInfo();
                SuccessData[] successDatas = new SuccessData[successDataInfos.length];
                ArrayList recoverTargetList = new ArrayList();
                for (int i = 0; i < successDatas.length; i++) {
                    successDatas[i] = new SuccessData(successDataInfos[i]);
                    successDatas[i].setFilename(filename);
                }
                boolean isRecover = false;
                for (int i = 0; i < m_dbOperations.length; i++) {
                    isRecover = false;
                    for (int j = 0; j < successDatas.length; j++) {

                        if (m_dbOperations[i].getDbName().equals(successDatas[j].getTargetSchema())) {
                            isRecover = true;
                        }
                    }
                    if (isRecover) {
                        recoverTargetList.add(m_dbOperations[i].getDbName());
                    }
                }
                String targetName = "";
                for (int i = 0; i < recoverTargetList.size(); i++) {
                    targetName = (String) recoverTargetList.get(i);
                    try {
                        processOneTargetSchema(targetName, dataConsumer, filename);
                        dataConsumer.succeedToConsumer(targetName);
                    } catch (Ex Ex) {
                        //dataConsumer.failedToConsumer(successData.getTargetSchema(), 1, Ex.getMessage(), Ex);
                        exceptions.add(Ex);
                    } catch (IllegalArgumentException e) {
                        //dataConsumer.failedToConsumer(successData.getTargetSchema(), 1, e.getMessage(), e);
                        exceptions.add(new Ex().set(E.E_Unknown, e));
                    }
                }
                int number = 0;
                if (exceptions.size() > 0) {      // Exceptions occured
                    Message Message = new Message(exceptions.size() + " Errors occured in data recovery; see below for details.");
                    m_logger.error(Message.toString());

                    int n = exceptions.size();
                    for (int i = 0; i < n; i++) {
                        Ex Ex = (Ex) exceptions.get(i);
                        Message = new Message("Detailed information of error " + "" + (i + 1) + ": ");
                        m_logger.error(Message.toString() + exceptionProcess(Ex), Ex);
                        m_log.setStatusCode(EStatus.E_DbChangeTargetProcessFaild.getKey().toString());
                        m_log.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        if (Ex.getErrcode().equals(E.E_DatabaseConnectionError)) {
                            number++;
                        }
                    }
                    if (number == exceptions.size()) {
                        dataProps.setStatus(Status.S_Faild_TargetProcess);
                        return dataProps;
                    } else {
                        throw new Ex().set(E.E_Unknown, new Message(n + " Errors occured during recovery; error messages as detailed."));
                    }
                }
            } else {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Start to process normal data" + filename + " ...");
                }

                DataInformation basicDataInformation = dataConsumer.getBasicDataInfo();
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Get basic data...");
                }
                DefaultData defaultData = new DefaultData(basicDataInformation);
                String schemaName = defaultData.getSchemaName();
                String tableName = defaultData.getTableName();
                defaultData.close();
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Find table maps by DbChangeSource(" + schemaName + ", " + tableName + ")...");
                }
                TableMap[] tableMaps = m_tableMaps.findBySource(schemaName, tableName);
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("TableMapSet length is: " + tableMaps.length);
                }
                if (tableMaps.length == 0) {
                    throw new Ex().set(E.E_OperationError, new Message("Find table maps is empty by DbChangeSource {0}, tableName {1}", schemaName, tableName));
                }
                for (int i = 0; i < tableMaps.length; i++) {
                    TableMap tableMap = tableMaps[i];
                    String targetSchema = tableMap.getTargetDb();
                    try {
                        processOneTargetSchema(targetSchema, dataConsumer, filename);
                        if (m_dbOperations.length > 1)
                            dataConsumer.succeedToConsumer(targetSchema);
                    } catch (Ex Ex) {
// m_logger.error("Failed to process one target schema.", eEx);
                        //dataConsumer.failedToConsumer(targetSchema, 1, "Failed to process!", Ex);
                        exceptions.add(Ex);
                    } catch (IllegalArgumentException e) {
                        //dataConsumer.failedToConsumer(targetSchema, 1, "Failed to process!", e);
                        exceptions.add(new Ex().set(E.E_Unknown, e));
                    } catch (Exception e) {
                        m_logger.warn("", e);
                        exceptions.add(new Ex().set(E.E_DatabaseError,e));
                    }finally {

                    }
                }
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Finish to process " + filename + " data.");
                }
                if (exceptions.size() > 0) {      // Exceptions occured
                    Message Message = new Message(exceptions.size() + " Errors occured in data processing; see below for details.");
                    m_logger.error(Message.toString());

                    int number = 0;
                    int n = exceptions.size();
                    for (int i = 0; i < n; i++) {
                        Ex Ex = (Ex) exceptions.get(i);
                        Message = new Message("Detailed information of error " + "" + (i + 1) + ": ");
                        m_logger.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        m_log.setStatusCode(EStatus.E_DbChangeTargetProcessFaild.getKey().toString());
                        m_log.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        if (Ex.getErrcode().equals(E.E_DatabaseConnectionError)) {
                            number++;
                        }
                    }
                    if (number == exceptions.size()) {
                        dataProps.setStatus(Status.S_Faild_TargetProcess);
                        return dataProps;
                    } else {
                        throw new Ex().set(E.E_Unknown, new Message(n + " Errors occured during data processing; error messages as detailed."));
                    }
                }
            }
        } finally {
            dataConsumer.close();
        }

        dataProps.setStatus(Status.S_Success_TargetProcess);
        return dataProps;
    }

    private void processOneTargetSchema(String schema, MDataParseImp dataConsumer) throws Ex {
        TargetDbOperation operation = getTargetDbOperations(schema);
        if (operation == null) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("operation is null");
            }
            throw new Ex().set(E.E_ObjectNotFound, new Message("OuputDbOperation is null for database {0} ", schema));
        }
        if (!operation.isOkay()) {
            Message Message = new Message("数据源:{0}.", operation.m_dbName);
            m_logger.warn(Message.toString());
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.warn(Message.toString());
            operation.sleepTime();
            operation.testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        DataInformation basicDataInformation = dataConsumer.getBasicDataInfo();
        DefaultData defaultData = new DefaultData(basicDataInformation);
        FilterResult f = null;
        try {
            if(basicDataInformation.isSequenceData()){

                f =  operation.processSequenceData(defaultData);
                return;
            } else {
                f = operation.processBasicData(defaultData);
            }
        } catch (Exception e) {
            m_logger.warn("", e);
            throw new Ex().set(E.E_DatabaseError,e);
        }finally {
            defaultData.close();
        }

        // blob
        DataInformation[] blobDataInfos = dataConsumer.getBlobDataInfo();
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("BLOB field count: " + blobDataInfos.length);
        }

        for (int j = 0; j < blobDataInfos.length; j++) {
            ByteLargeObjectData byteLargeObjectData = new ByteLargeObjectData(blobDataInfos[j]);
            try {
                operation.insertOrUpdateBlobData(byteLargeObjectData, f);
            } catch (Exception e) {
                m_logger.warn("", e);
                throw new Ex().set(E.E_DatabaseError,e);
            } finally {
                byteLargeObjectData.close();
            }
        }

        // clob
        DataInformation[] clobDataInfos = dataConsumer.getClobDataInfo();

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("CLOB field count: " + blobDataInfos.length);
        }
        for (int j = 0; j < clobDataInfos.length; j++) {
            CharLargeObjectData charLargeObjectData = new CharLargeObjectData(clobDataInfos[j]);
            try {
                operation.insertOrUpdateClobData(charLargeObjectData, f);
            } catch (Exception e) {
                m_logger.warn("", e);
                throw new Ex().set(E.E_DatabaseError,e);
            }finally {
                charLargeObjectData.close();
            }
        }
    }

    private void processOneTargetSchema(String schema, MDataParseImp dataConsumer, String filename) throws Ex {
        TargetDbOperation operation = getTargetDbOperations(schema);
        if (operation == null) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("operation is null");
            }
            throw new Ex().set(E.E_ObjectNotFound, new Message("OuputDbOperation is null for database {0} ", schema));
        }
        if (!operation.isOkay()) {
            Message Message = new Message("数据源:{0}.", operation.m_dbName);
            m_logger.warn(Message.toString());
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.warn(Message.toString());
            operation.sleepTime();
            operation.testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        DataInformation basicDataInformation = dataConsumer.getBasicDataInfo();
        DefaultData defaultData = new DefaultData(basicDataInformation);
        defaultData.setFilename(filename);
        FilterResult f = null;
        try {
            if(basicDataInformation.isSequenceData()){
                f =  operation.processSequenceData(defaultData);
                return;
            } else {
                f = operation.processBasicData(defaultData);
            }
        } catch (Exception e) {
            m_logger.warn("", e);
            throw new Ex().set(E.E_DatabaseError,e);
        }finally {
            defaultData.close();
        }

        // blob
        DataInformation[] blobDataInfos = dataConsumer.getBlobDataInfo();
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("BLOB field count: " + blobDataInfos.length);
        }

        for (int j = 0; j < blobDataInfos.length; j++) {
            ByteLargeObjectData byteLargeObjectData = new ByteLargeObjectData(blobDataInfos[j]);
            byteLargeObjectData.setFilename(filename);
            try {
                operation.insertOrUpdateBlobData(byteLargeObjectData, f);
            } catch (Exception e) {
                m_logger.warn("", e);
                throw new Ex().set(E.E_DatabaseError,e);
            }finally {
                byteLargeObjectData.close();
            }
        }

        // clob
        DataInformation[] clobDataInfos = dataConsumer.getClobDataInfo();

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("CLOB field count: " + blobDataInfos.length);
        }
        for (int j = 0; j < clobDataInfos.length; j++) {
            CharLargeObjectData charLargeObjectData = new CharLargeObjectData(clobDataInfos[j]);
            charLargeObjectData.setFilename(filename);
            try {
                operation.insertOrUpdateClobData(charLargeObjectData, f);
            } catch (Exception e) {
                m_logger.warn("", e);
                throw new Ex().set(E.E_DatabaseError,e);
            }finally {
                charLargeObjectData.close();
            }
        }
    }

    protected TargetDbOperation getTargetDbOperations(String schemaName) throws Ex {
        TargetDbOperation result = null;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("schemaName " + schemaName);
        }
        for (int i = 0; i < m_dbOperations.length; i++) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("m_dbOperations[i] is " + m_dbOperations[i].getDbName());
            }
            if (schemaName.equalsIgnoreCase(m_dbOperations[i].getDbName())) {
                result = m_dbOperations[i];
                i = m_dbOperations.length;
            }
        }
        return result;
    }

    /**
     * get the capabilities of the implementation in the decendent order (element[0] being preferred).
     *
     * @return the capabilities as defined by the constents at the top.
     */
    public int[] getCapabilities() {
        return new int[]{I_FileCapability, I_StreamCapability};

        //return m_cap;
    }

    public void init(IChangeMain iChangeMain, IChangeType iChangeType, ISourcePlugin iSourePlugin) throws Ex {
        //To change body of implemented methods use File | Settings | File Templates.
        m_changeMain = iChangeMain;
        m_changeType = iChangeType;
        m_source = iSourePlugin;
        m_cap = new int[]{I_FileCapability, I_StreamCapability};
        m_log = iChangeMain.createLogHelper();

    }


    public DataAttributes control(String s, DataAttributes dataAttributes) throws Ex {

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void config(IChange iChange) throws Ex {
        if (configred()) {
            m_changeMain.setStatus(m_changeType, EStatus.E_AlreadyConfigured, null, false);
            throw new Ex().set(E.E_Unknown, new Message("Already Configured. "));
        }
        String network = System.getProperty("privatenetwork");
        Map jdbcs = new HashMap();

        //DbSyncOutputConfigNode dbSyncConfigNode = new DbSyncOutputConfigNode(path);
        Type type = iChange.getType(m_changeType.getType());
        DbChangeTarget.Str_DbChangeType = type.getAppType();
        Plugin plugin = type.getPlugin();
        if (plugin == null) {
            m_bConfigured = false;
            return;
        }
        SourceDb srcdb = plugin.getSourceDb();
        if (srcdb == null) {
            m_bConfigured = false;
            return;
        }
        SourceTable[] srctables = (SourceTable[]) srcdb.getAllSourceTables();
        for (int i = 0; i < srctables.length; i++) {
            TargetDb[] targetdb = srctables[i].getAllTargetDbstoArray();
            for (int j = 0; j < targetdb.length; j++) {
                jdbcs.put(targetdb[j].getDbName(), iChange.getJdbc(targetdb[j].getDbName()));
                Table targetTables = targetdb[j].getTable();
                //for (int z=0;z<targetTables.length;z++) {
                TableMap tableMap = new TableMap(srcdb.getDbName(),
                        srctables[i].getTableName(),
                        targetdb[j].getDbName(),
                        targetTables.getTableName());
                tableMap.setDeleteEnable(targetTables.isDeleteEnable());
                tableMap.setOnlyInsert(targetTables.isOnlyinsert());
                tableMap.setCondition(targetTables.getCondition());
                com.inetec.common.config.nodes.Field[] fields = targetTables.getAllFields();
                HashMap maptest = new HashMap();
                for (int n = 0; n < fields.length; n++) {
                    Column targetColumn = new Column(fields[n].getDestField(),
                            DbopUtil.getJdbcType(fields[n].getJdbcType()),
                            fields[n].getDbType(),
                            fields[n].isPk());
                    String srcColumnName = fields[n].getFieldName();
                    ColumnMap columnMap = new ColumnMap(srcColumnName, targetColumn);
                    if (!maptest.containsKey(columnMap.getTargetColumn().getName()))
                        tableMap.AddFieldMap(columnMap);
                    else
                        m_logger.warn("column name is exists:" + columnMap.getTargetColumn().getName());
                    maptest.put(columnMap.getTargetColumn().getName(), columnMap.getTargetColumn().getName());
                }
                if (m_source != null) {
                    tableMap.setInputTableInfo(((DbChangeSource) m_source).getTableInfo(targetdb[j].getDbName(),
                            targetTables.getTableName()));
                    tableMap.setInputTriggerEnable(((DbChangeSource) m_source).isTriggerEnable(targetdb[j].getDbName()));
                }
                m_tableMaps.AddTableMap(tableMap);
                //}

            }
        }

        //}


        m_dbOperations = new TargetDbOperation[jdbcs.size()];
        Jdbc[] jdbcarray = null;
        if (jdbcs.size() > 0) {
            jdbcarray = (Jdbc[]) jdbcs.values().toArray(new Jdbc[0]);
        }
        for (int i = 0; i < m_dbOperations.length; i++) {
            TargetDbOperation dbOperation = new TargetDbOperation(m_changeMain, m_changeType, this);
            // todo: cache and encoding
            // dbOperation.getCurrentDbms().setEncoding(defDbConfigNodes[i].getEncoding(), false);
            // DbopUtil.setEncoding(defDbConfigNodes[i].getEncoding(), false);

            // set table mapping
            TableMap[] tableMaps = m_tableMaps.findByTarget(jdbcarray[i].getJdbcName());
            dbOperation.setTableMaps(tableMaps);

            // set jdbc info
            JdbcInfo jdbcInfo = new JdbcInfo(jdbcarray[i]);
            dbOperation.setJdbcInfo(jdbcInfo);
            dbOperation.setNativeCharSet(jdbcarray[i].getEncoding());
            m_dbOperations[i] = dbOperation;

        }
        if (m_dbOperations.length == 1) {
            m_cap = new int[]{I_StreamCapability, I_FileCapability};

        } else {
            m_cap = new int[]{I_FileCapability, I_StreamCapability};
        }
        m_bConfigured = true;
        m_changeMain.setStatus(m_changeType, EStatus.E_CONFIGOK, null, false);
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("finish config target dbchange");
        }
        m_log.setAppName(m_changeType.getType());
        m_log.setAppType(Str_DbChangeType);
        m_log.setSouORDes(LogHelper.Str_souORDes_Dest);

    }


    public boolean configred() {
        return m_bConfigured;  //To change body of implemented methods use File | Settings | File Templates.
    }


    public ISourcePlugin getISourcePlugin() {
        return m_source;
    }

    public String exceptionProcess(Ex Ex) {
        String temp = "";
        try {
            temp = Ex.getMessage().toString();
            temp = temp + " " + Ex.getCause().getMessage();
        } catch (NullPointerException e) {
            //okay;
        }
        return temp;
    }

    public TableMap[] findTableMap(String schemaName, String tableName) {

        return m_tableMaps.findBySource(schemaName, tableName);
    }

    public TableMap[] findTableMap(String schemaName) {

        return m_tableMaps.findBySourceDb(schemaName);
    }

    private TableMap findTableMap(String targetSchema, MDataParseImp dataConsumer) throws Ex {
        TableMap tableMap = null;
        DefaultData defaultData = new DefaultData(dataConsumer.getBasicDataInfo());
        String srcSchema = defaultData.getSchemaName();
        String srcTable = defaultData.getTableName();
        TableMap[] tableMaps = m_tableMaps.findBySource(srcSchema, srcTable);
        for (int j = 0; j < tableMaps.length; j++) {
            tableMap = tableMaps[j];
            if (targetSchema.equalsIgnoreCase(tableMap.getTargetDb())) {
                return tableMap;
            }
        }
        if (tableMap == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("Can not find target database {0} and  in the config mapper", targetSchema));
        }
        return tableMap;
    }


}
