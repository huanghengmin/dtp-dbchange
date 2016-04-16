/*=============================================================
 * 文件名称: SourceDbOperation.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-10-17
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
package com.inetec.ichange.plugin.dbchange.source;


import com.inetec.common.db.datasource.DatabaseSource;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import com.inetec.common.logs.LogHelper;
import com.inetec.ichange.api.*;
import com.inetec.ichange.plugin.dbchange.DbChangeSource;
import com.inetec.ichange.plugin.dbchange.DbInit;
import com.inetec.ichange.plugin.dbchange.datautils.ArrayListInputStream;
import com.inetec.ichange.plugin.dbchange.datautils.MDataConstructor;
import com.inetec.ichange.plugin.dbchange.datautils.db.*;
import com.inetec.ichange.plugin.dbchange.datautils.db.pk.PkSet;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DBType;
import com.inetec.ichange.plugin.dbchange.exception.EDbChange;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.exception.ErrorSql;
import com.inetec.ichange.plugin.dbchange.source.info.*;
import com.inetec.ichange.plugin.dbchange.source.twoway.SourceObject;
import com.inetec.ichange.plugin.dbchange.source.twoway.SourceObjectCache;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class SourceDbOperation extends DbInit implements Runnable {

    public static Logger m_logger = Logger.getLogger(SourceDbOperation.class);
    Logger auditLogger = Logger.getLogger("ichange.audit.database");

    private Map m_rowList = new HashMap();
    private Map mergeTableInfo = new HashMap();

    // SQL bundle

    public final static String Str_Sql_GetTempCount = "Sql_GetTempCount";
    public final static String Str_Sql_SelectAllFromTemp = "Sql_SelectAllFromTemp";
    public final static String Str_Sql_SelectAllFromTempForOp_time = "Sql_SelectAllFromTempForOp_time";
    public final static String Str_Sql_SelectCountFromTempForOp_time = "Sql_SelectCountFromTempForOp_time";
    public final static String Str_Sql_SelectCountFromTempForSys_time = "Sql_SelectCountFromTempForSys_time";
    public final static String Str_Sql_TimeSyncSelect = "Sql_TimeSyncSelect";
    public final static String Str_Sql_TimeSyncInitDate = "sql_TimeSyncInitDate";
    public final static String Str_Sql_TimeSyncEndDate = "sql_TimeSyncEndDate";
    public final static String Str_Sql_InitChangeTwoWay = "Sql_DeleteDuplicateItems";
    public final static String Str_Sql_DeleteFromTemp = "Sql_DeleteFromTemp";
    public final static String Str_Sql_DeleteFromTempForIDSet = "Sql_DeleteFromTempForIDSet";
    public final static String Str_Sql_DeleteFormTempForTableName = "Sql_DeleteFromTempForTableName";
    public final static String Str_Sql_DeleteFromTable = "Sql_DeleteFromTable";
    public final static String Str_Sql_UpdateSpecifyFlag = "Sql_UpdateSpecifyFlag";


    public SourceDbOperation(IChangeMain dc, IChangeType type, ISourcePlugin in) {
        m_changeMain = dc;
        m_changeType = type;
        m_sourcePlugin = in;
        m_log = dc.createLogHelper();
        m_log.setAppType(DbChangeSource.Str_DbChangeType);
        m_log.setAppName(type.getType());
        m_log.setSouORDes(LogHelper.Str_souORDes_Source);
        appType = m_changeType.getType();
        m_objectCache = ((DbChangeSource) m_sourcePlugin).getObjectCache();
        String trusted = System.getProperty("privatenetwork");
        if (trusted != null) {
            trusted.toLowerCase();
            if (trusted.equalsIgnoreCase("true"))
                m_bTrusted = true;
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("privatenetwork = " + String.valueOf(m_bTrusted));
        }
//        if("dbOneLine".equals(DbChangeSource.Str_DbChangeType)) {
//            this.sourceDbOperationAddTrigger = new SourceDbOperationAddTrigger(dc,type,in);
//        }

    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) throws Ex {
        m_databaseInfo = databaseInfo;
        m_tableInfos = m_databaseInfo.getTableInfo();
        m_nativeCharSet = databaseInfo.getEncoding();
        m_schemaName = databaseInfo.getJdbcInfo().getDbOwner();
        m_dbDesc = databaseInfo.getJdbcInfo().getDesc();
        m_driverClass = databaseInfo.getJdbcInfo().getDriverClass();
        m_log.setDbName(m_databaseInfo.getJdbcInfo().getDbName());
        m_log.setIp(m_databaseInfo.getJdbcInfo().getDbHost());
        m_dbName = m_databaseInfo.getJdbcInfo().getDbName();
        initMergeTableInfo();
        initDbSource();

    }

    /**
     * 对当前数据源中的辅助表进行规则检查
     */
    private void initMergeTableInfo() {
        for (int i = 0; i < m_tableInfos.length; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            if (tableInfo.isMergeTable()) {
                for (int j = 0; j < tableInfo.getAllMergeTable().length; j++) {
                    String mergeTable = tableInfo.getAllMergeTable()[j].getMergeTableName();
                    TableInfo table2 = findTableInfo(mergeTable);
                    if (table2 == null) {
                        Message Message = new Message("导出源表 {0} 配置错误.在同步数据表中没有醪置辅助表", mergeTable);
                        m_logger.warn(Message.toString(), new Exception());
                        continue;
                    }
                    mergeTableInfo.put(mergeTable, mergeTable);
                }
            }
        }

    }

    public String getSchemaName() {
        return m_schemaName;
    }

    public String getDbName() {
        return m_dbName;
    }

    public boolean isTriggerEnable() {
        return m_databaseInfo.isTriggerEnable();
    }

    public TableInfo findTableInfo(String tableName) {
        for (int i = 0; i < m_tableInfos.length; i++) {
            if (m_tableInfos[i].getName().equalsIgnoreCase(tableName)) {
                return m_tableInfos[i];
            }
        }

        return null;
    }

    public void run() {
        if (!m_databaseInfo.isEnable()) {
            m_logger.warn("要交换源数据库 " + m_dbName + "设置为不可用.");
            return;
        }
        boolean initdb = false;
        String initdbString = System.getProperty("initdb");
        if (initdbString != null) {
            initdb = initdbString.equalsIgnoreCase("true");
        } else {
            initdb = m_databaseInfo.isOldStep();
        }
        m_running = true;
        int runningCount = 0;
        while (m_running) {
            try {
                runningCount++;
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("循环处理次数: " + runningCount);

                }
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("数据操作方式:" + m_databaseInfo.isTimeSync());
                }
                if(m_databaseInfo.isSequenceEnable()){    //序列同步
                    try{
                        exportSequenceTable();
                    } catch (Exception e){
                        m_logger.error("序列完全同步",e);
                    }
                    try {
                        Thread.sleep(m_databaseInfo.getInterval() * 1000);
                    } catch (InterruptedException ie) {
                        // okay.
                    }

                } else if (m_databaseInfo.isTriggerEnable()) { // 触发同步

                    if (initdb && !m_databaseInfo.isTwoway()) { //单向触发同步
                        exportOldData();
                        initdb = false;

                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("完成全表交换的数据库是:" + m_dbName);
                        }
                    } else {
                        if (m_databaseInfo.isTwoway()) {   //双向触发同步
                            if (m_bTrusted) {
                                changeForOneTableTwowayForAll();
                            } else {
                                if (m_syncTime > 0)
                                    changeForTable(m_syncTime);
                            }
                            try {
                                Thread.sleep(m_databaseInfo.getInterval() * 1000);
                            } catch (InterruptedException ie) {
                                // okay.
                            }
                        } else {
                            int size = exportNewData();
                            if (size == 0) {

                                try {
                                    Thread.sleep(m_databaseInfo.getInterval() * 1000);
                                } catch (InterruptedException ie) {
                                    // okay.
                                }
                            }
                        }// else {}
                    }
                } else { // trigger process is disabled
                    //timesync is operator.
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("数据操作方式:" + m_databaseInfo.isTimeSync());
                    }
                    if (m_databaseInfo.isTimeSync()) {   //时间标记同步
                        timeSync();
                        try {
                            Thread.sleep(m_databaseInfo.getInterval() * 1000);
                        } catch (InterruptedException ie) {
                            // okay.
                        }
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("完成全表交换的数据库是Op(TimeSync):" + m_dbName);
                        }

                    } else {
                        long sleepTime = m_databaseInfo.getNeExtSleepTime();
                        if (sleepTime < 0) {
                            exportOldData();  //全表同步
                        } else {
                            try {
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException ie) {
                                // okay.
                            }
                        }
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("完成全表交换的数据库是:" + m_dbName);
                        }
                    }
                }

            } catch (RuntimeException e) {
                m_logger.error("导出交换源数据出错 Runtime exception.", e);
            }
        }
    }

    public void initializeAll() throws Ex {

        sequenceTable();

        m_initialized = true;
    }

    /**
     * Descriptions: sequence tables for the synchronize old data
     *
     * @throws Ex
     */
    protected void sequenceTable() throws Ex {
        Arrays.sort(m_tableInfos, new Comparator() {
            public int compare(Object o1, Object o2) {
                int i1 = ((TableInfo) o1).getSequence();
                int i2 = ((TableInfo) o2).getSequence();
                return i1 - i2;
            }
        });
    }


    private void exportSequenceTable() {
        int tableSize = m_tableInfos.length;
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return;
        }
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            String tableName = m_tableInfos[i].getName();
            try {
                exportWholeSequenceTable(tableName);
            } catch (Ex Ex) {
                Message Message = new Message("导出源表 {0} 数据出错.", tableName);
                m_logger.warn(Message.toString(), Ex);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), Ex);
            }
        }

    }

    private void exportWholeSequenceTable(String tableName) throws Ex {
        TableInfo tableInfo = findTableInfo(tableName);
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }
//        String strBasicQuery = "select "+tableName+".nextval,"+tableName+".currval as id from dual";
//        String strBasicQuery = "select * from all_sequences where SEQUENCE_OWNER = '"+m_schemaName
//                +"' and SEQUENCE_NAME = '"+tableName.toUpperCase()+"'";
        String strBasicQuery = "select * from all_sequences where SEQUENCE_OWNER = '"+m_schemaName.toUpperCase()
                +"' and SEQUENCE_NAME = '"+tableName.toUpperCase()+"'";
        Connection conn = getConnection();
        if (conn == null) {
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            dataBaseRetryProcess(conn);
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        InputStream in = null;
        try {
            stmt = (PreparedStatement)conn.prepareStatement(strBasicQuery);
            rs = stmt.executeQuery();
            rs.next();
            int id = rs.getInt("last_number");
            Row[] rows = new Row[1];
            rows[0] = new Row(m_databaseInfo.getName(), tableName);
            if(m_logger.isDebugEnabled()){
                m_logger.debug("last_number is " + id );
            }
            MDataConstructor mDataConstructor = dataConstructorSequence(id, tableInfo);
            InputStream is = mDataConstructor.getDataInputStream();

            ByteArrayOutputStream gzip = null;
            try {
                gzip = new ByteArrayOutputStream();
                IOUtils.copy(is, gzip);
                gzip.flush();
                in = new ByteArrayInputStream(gzip.toByteArray());
            } catch (Exception e) {
                m_logger.error("", e);
            } finally {
                try {
                    if(gzip!=null){
                        gzip.close();
                    }
                } catch (IOException e) {
                }
                try {
                    if(is!=null){
                        is.close();
                    }
                } catch (IOException e) {
                }
            }
            disposeDataSequence(in, rows, false);
        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            try {
                if(rs != null){
                    rs.close();
                }
            } catch (SQLException e) {
            }
            try {
                    if(in!=null){
                        in.close();
                    }
                } catch (IOException e) {
                }
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    private void exportOldData() {
        int tableSize = m_tableInfos.length;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("开始导出源表数据, 源表个数为: " + tableSize);
        }
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return;
        }
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            long nextTime = tableInfo.getNeExtTime();
            if (System.currentTimeMillis() >= nextTime) {
                String tableName = m_tableInfos[i].getName();
                if (isMergeTable(tableName)) {
                    continue;
                }
                try {
                    exportWholeTable(tableName);  //导出
                    tableInfo.nextTime();
                } catch (Ex Ex) {
                    Message Message = new Message("导出源表 {0} 数据出错.", tableName);
                    m_logger.warn(Message.toString(), Ex);
                    m_log.setTableName(tableName);
                    m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                    m_log.warn(Message.toString(), Ex);
                }
            }
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("完成源表数据的导出.");
        }
        return;
    }


    private void exportWholeTable(String tableName) throws Ex {
        TableInfo tableInfo = findTableInfo(tableName);
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }

        Column[] clobColumns = tableInfo.getClobColumns();
        Column[] blobColumns = tableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasLob = hasBlob || hasClob;
        boolean hasBothBlobAndClob = hasBlob && hasClob;

        Column[] basicColumns = tableInfo.getBasicColumns(); //获取基本列

        String strBasicQuery = null;
        String strWhere = null;

        boolean isOldStep = tableInfo.isTriggerEnable() || tableInfo.isDelete();
        isOldStep = isOldStep || tableInfo.isSpecifyFlag();
        isOldStep = m_databaseInfo.isOldStep() || !isOldStep;

        if (hasLob) {
            Column[] columns = null;
            Column[] lobColumn = null;

            if (hasBothBlobAndClob) {
                lobColumn = addTwoTypeColumns(blobColumns, clobColumns);
            } else {
                if (hasBlob) {
                    lobColumn = blobColumns;
                } else if (hasClob) {
                    lobColumn = clobColumns;
                }
            }
            columns = addTwoTypeColumns(basicColumns, lobColumn);
            strBasicQuery = getTableQueryString(tableName, columns);
            strWhere = getWhereStringFromSpeicfyFlag(tableInfo.isSpecifyFlag());//标记同步
            strBasicQuery += strWhere;
        } else {
            strBasicQuery = getTableQueryString(tableName, basicColumns);
            strWhere = getWhereStringFromSpeicfyFlag(tableInfo.isSpecifyFlag());  //标记同步
            strBasicQuery += strWhere;

        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("query table sql string: " + strBasicQuery);
        }
        if (isOldStep) {
            processMaxRecordsForOldStep(strBasicQuery, tableInfo, m_databaseInfo.getMaxRecord());
        } else {
            int n = 0;
            do {
                n = processMaxRecords(strBasicQuery, tableInfo, m_databaseInfo.getMaxRecord());
            } while (n > 0);
        }

    }

    private void exportWholeTableTimeSync(String tableName, Timestamp begin, Timestamp end, String pkWhere, int rownum) throws Ex {
        TableInfo tableInfo = findTableInfo(tableName);
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }

        Column[] clobColumns = tableInfo.getClobColumns();
        Column[] blobColumns = tableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasLob = hasBlob || hasClob;
        boolean hasBothBlobAndClob = hasBlob && hasClob;

        Column[] basicColumns = tableInfo.getBasicColumns();

        String strBasicQuery = null;
        String strWhere = null;

        if (hasLob) {
            Column[] columns = null;
            Column[] lobColumn = null;

            if (hasBothBlobAndClob) {
                lobColumn = addTwoTypeColumns(blobColumns, clobColumns);
            } else {
                if (hasBlob) {
                    lobColumn = blobColumns;
                } else if (hasClob) {
                    lobColumn = clobColumns;
                }
            }
            columns = addTwoTypeColumns(basicColumns, lobColumn);
            strBasicQuery = getTableQueryString(tableName, columns);
            strWhere = getWhereStringFromTimeSync(tableInfo, pkWhere, begin);
            strBasicQuery += strWhere;
        } else {
            strBasicQuery = getTableQueryString(tableName, basicColumns);
            strWhere = getWhereStringFromTimeSync(tableInfo, pkWhere, begin);
            strBasicQuery += strWhere;

        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("query table sql string: " + strBasicQuery);
        }
        processMaxRecordsForTimeSync(strBasicQuery, tableInfo, m_databaseInfo.getMaxRecord(), begin, end, rownum);

    }

    private int processMaxRecords(String sql, TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        testStatus();
        Connection conn = getConnection();
        if (conn == null) {
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            dataBaseRetryProcess(conn);
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;

        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(max);
            stmt.setMaxRows(max);
            ResultSet rs = stmt.executeQuery(sql);
            multiData = dataConstructor(rs, tableInfo, max);
            if (rs != null) {
                rs.close();
            }
            if (multiData.getData().getRowArray().length > 0) {
                if (tableInfo.isMergeTable()) {
                    disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), false, tableInfo.getAllMergeTable());
                } else {
                    disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), false);
                }
                return multiData.getData().getRowArray().length;
            } else {
                return multiData.getData().getRowArray().length;
            }

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    private void postProcessor(Row row) throws Ex {
        Connection conn = getConnection();
        PreparedStatement stmt = null;
        String tableName = row.getTableName();
        TableInfo tableInfo = findTableInfo(tableName);
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }

        String sqlString = null;
        boolean isSpecifyFlag = m_databaseInfo.isSpecifyFlag();
        String flagName = m_databaseInfo.getFlagName();
        boolean isDelete = m_databaseInfo.isDelete();
        TimeoutRow timeoutRow = (TimeoutRow) m_rowList.get(row);
        if (timeoutRow != null) {
            if (timeoutRow.verifier()) {
                m_rowList.remove(row);
            } else {
                return;
            }
        }
        try {
            if (isDelete) {
                // String sqlString = "delete from {0}";
                sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTable,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, tableName)});
            } else if (isSpecifyFlag) {

                sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_UpdateSpecifyFlag,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, tableName), flagName});


                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("update SpecifyFlag sql: " + sqlString);
                }
            } // else {}

            if (isDelete || isSpecifyFlag) {
                Column[] pkArray = row.getPkColumnArray();
                String strWhere = getWhereStringFromPks(pkArray);
                sqlString = sqlString + strWhere;
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("isDelete or isSpecifyFlag sql: " + sqlString);
                    m_logger.debug("is pk value: " + pkArray[0].getValue().getValueString());
                }
                stmt = m_I_dbOp.getSatement(conn, sqlString, pkArray,null);
                stmt.setQueryTimeout(I_StatementTimeOut);
                int numn = stmt.executeUpdate();
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("update datebase  record number: " + numn);
                }

            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while processing the table {0} of the old data.", tableName));
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            testBadConnection(conn, Exsql);
            if (Exsql.getErrcode().equals(ErrorSql.ERROR___STATEMENT_TIME_OUT)) {
                m_rowList.put(row, new TimeoutRow(row));
                m_logger.warn("Post Processor Time Out: " + Exsql.getMessage());
                m_log.setDbName(m_dbName);
                m_log.setTableName(tableInfo.getName());
                m_log.setStatusCode(EStatus.E_RecordProcessFaild.getCode() + "");
                m_log.warn("Post Processor Time Out: " + Exsql.getMessage());
            } else {
                throw Exsql;
            }
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }


    private String getWhereStringFromSpeicfyFlag(boolean specify) {
        if (specify) {
            String strWhere = "";
            strWhere += " where ";
            strWhere += m_databaseInfo.getFlagName().toUpperCase();
            strWhere += "=\'0\'";
            return strWhere;
        } else {
            return "";
        }
    }

    private String getWhereStringFromTimeSync(TableInfo tableinfo, String pkwhere, Timestamp begin) {
        if (tableinfo.isTimeSync()) {
            String strWhere = "";
            String order = " order by ";
            String ordertemp = "";
            strWhere += " where ";

            strWhere += " ";
//            PkSet pkset = null;
            Column[] pkwhers = new Column[0];
            if (pkwhere != null && !pkwhere.equalsIgnoreCase("")) {
                //.try {

                /* pkset = new PkSet(pkwhere);
              pkwhers = pkset.getPkArray();*/
//                } catch (Ex ex) {
//
//                }
            }
            if (pkwhers.length == 0) {
                Column[] pkcolumn = tableinfo.getPkColumns();
                for (int i = 0; i < pkcolumn.length; i++) {
                    if (pkcolumn[i].getJdbcType() == Types.DATE || pkcolumn[i].getJdbcType() == Types.TIMESTAMP || pkcolumn[i].getJdbcType() == Types.TIME) {
                        strWhere += pkcolumn[i].getName() + " >=? and " + pkcolumn[i].getName() + "< ?";
                        order += pkcolumn[i].getName() + " asc ";
                    } else {
                        /*strWhere += pkcolumn[i].getName() + " >  ";
                        if (pkcolumn[i].getJdbcType() == Types.VARCHAR || pkcolumn[i].getJdbcType() == Types.CHAR) {
                            strWhere += "'" + pk + "'";
                        }

                        if (ordertemp.endsWith("asc")) {
                            ordertemp += ",";
                        }
                        ordertemp += pkcolumn[i].getName() + " asc";*/
                    }

                }
            } else {
                for (int i = 0; i < pkwhers.length; i++) {
                    if (pkwhers[i].getDbType().equalsIgnoreCase("DATE") || pkwhers[i].getDbType().equalsIgnoreCase("TIMESTAMP") || pkwhers[i].getDbType().equalsIgnoreCase("TIME")) {
                        strWhere += pkwhers[i].getName() + " >=? and " + pkwhers[i].getName() + "<? ";
                        order += pkwhers[i].getName() + " asc ";
                        //begin = new Timestamp(Long.parseLong(pkwhers[i].getValue().getValueString()));
                    } else {
                        strWhere += pkwhers[i].getName() + " >";
                        if (pkwhers[i].getDbType().equalsIgnoreCase("VARCHAR") || pkwhers[i].getDbType().equalsIgnoreCase("CHAR")) {
                            strWhere += "'" + pkwhers[i].getValue().getValueString() + "'";
                        } else {

                            strWhere += pkwhers[i].getValue().getValueString();
                        }
                        if (ordertemp.endsWith("asc")) {
                            ordertemp += ",";
                        }
                        ordertemp += pkwhers[i].getName() + " asc";
                    }
                    if (i < pkwhers.length - 1) {
                        strWhere += " and ";
                    }
                }
            }

            if (!order.equalsIgnoreCase(" order by ")) {
                strWhere += order;
                if (!ordertemp.equalsIgnoreCase("")) {
                    strWhere += "," + ordertemp;
                }
            }
            if (m_logger.isDebugEnabled())
                m_logger.debug("time sync where is :" + strWhere);
            return strWhere;
        } else {
            return "";
        }
    }

    private int exportNewData() {
        int size = 0;
        try {
            testStatus();
        } catch (Ex ex) {
            return 0;
        }

        int tableSize = m_tableInfos.length;
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            String tableName = m_tableInfos[i].getName();

            try {
                if (isMergeTable(tableName)) {
                    //deleteTempRowByTableName(tableName);
                    continue;
                }
                do {
                    size = exportForTempTable(tableInfo, m_tempRowLastMaxRow);
                } while (size > 0);
            } catch (Ex Ex) {
                Message Message = new Message("导出源表 {0} 数据出错.", tableName);
                m_logger.warn(Message.toString(), Ex);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), Ex);
            }
        }
        return size;
    }


    private int exportForTempTable(TableInfo tableInfo, long maxrecod) throws Ex {
        try {
            testStatus();
        } catch (Ex ex) {
            return 0;
        }
        Connection conn = getConnection();
        Statement stmt = null;
        MDataConstructor multiData = new MDataConstructor();
        int length = 0;
        ArrayList listRow = new ArrayList();
        try {
            int max = m_databaseInfo.getMaxRecord();

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectAllFromTemp,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()),
                            "'" + m_schemaName + "'",
                            "'" + tableInfo.getName() + "'",
                            String.valueOf(max)});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Str_Sql_SelectAllFromTemp: " + sqlQuery);
            }
//            int max = m_databaseInfo.getMaxRecord();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setFetchSize(max);
            if (m_dbType.equals(DBType.C_MSSQL))
                stmt.setMaxRows(max);
            else
                stmt.setMaxRows(I_MaxRows);
            ResultSet rs = stmt.executeQuery(sqlQuery);
            boolean done = false;
            int size = 0;
            TempRowBean tmp = dataConstructorForTemp(rs, max);
            listRow.add(tmp);
//            while (!done) {
//                TempRowBean tmp = dataConstructorForTemp(rs, max);
//                if (tmp.getLength() == 0) {
//                    done = true;
//                } else {
//                    if (tmp.getMaxId() > maxrecod) {
//                        maxrecod = tmp.getMaxId();
//                    }
//                    size = size + tmp.getLength();
//                    listRow.add(tmp);
//                }
//            }
            if (rs != null)
                rs.close();

            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            //删除临时记录

            if (listRow.size() > 0) {
                if (m_dbType.equals(DBType.C_MSSQL)) {
                    deleteTempRow(conn, listRow);
                }
                listRow.clear();
                m_tempRowLastMaxRow = maxrecod;
                length = size;
            } else {
                m_tempRowLastMaxRow = 0;

            }
        } catch (SQLException
                sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while retrieving a temp row."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return length;
    }


    private int exportForTempTable(long time, TableInfo tableInfo, long maxrecod) throws Ex {
        testStatus();
        int size = 0;
        Connection conn = getConnection();
        MDataConstructor multiData = new MDataConstructor();
        PreparedStatement stmt = null;
        ArrayList listRow = new ArrayList();
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectAllFromTempForOp_time,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), "'" + m_schemaName + "'", "'" + tableInfo.getName() + "'", String.valueOf(maxrecod)});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Sql_SelectAllFromTempForOp_time: " + sqlQuery);
            }

            int max = m_databaseInfo.getMaxRecord();
            max = (tableInfo.getClobColumns().length + tableInfo.getBlobColumns().length + 1) * max;
            if (max > DatabaseInfo.I_MaxRecord)
                max = DatabaseInfo.I_MaxRecord;
            stmt = conn.prepareStatement(sqlQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setFetchSize(max);
            if (m_dbType.equals(DBType.C_MSSQL))
                stmt.setMaxRows(max);
            else
                stmt.setMaxRows(I_MaxRows);
            stmt.setTimestamp(1, new Timestamp(time));
            ResultSet rs = stmt.executeQuery();
            boolean done = false;
            while (!done) {
                TempRowBean tmp = dataConstructorForTemp(rs, max);
                if (tmp.getLength() == 0) {
                    done = true;
                } else {
                    if (tmp.getMaxId() > maxrecod) {
                        maxrecod = tmp.getMaxId();
                    }
                    size = size + tmp.getLength();
                    listRow.add(tmp);
                }
            }
            if (rs != null)
                rs.close();
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            //删除临时记录
            if (listRow.size() > 0) {
                if (m_dbType.equals(DBType.C_MSSQL))
                    deleteTempRow(conn, listRow);
                m_tempRowLastMaxRow = maxrecod;
                listRow.clear();
            } else {
                size = 0;
                m_tempRowLastMaxRow = 0;
            }

        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while retrieving a temp row."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return size;
    }


    private int deleteTempRow(long seqNo, Connection conn) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTemp,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), seqNo + ""});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete temprow sql: " + sqlQuery);
            }
            stmt = conn.createStatement();
            {
                nUpdate = stmt.executeUpdate(sqlQuery);
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + seqNo));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
        }

        return nUpdate;
    }

    private int deleteTempRow(long seqNo) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        Connection conn = null;
        if (conn == null) {
            conn = getConnection();
        }
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTemp,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), seqNo + ""});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete temprow sql: " + sqlQuery);
            }
            stmt = conn.createStatement();
            {
                nUpdate = stmt.executeUpdate(sqlQuery);
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + seqNo));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }

        return nUpdate;
    }

    /**
     * 批量delete  .成批删除TempRow
     *
     * @param ids
     * @return
     * @throws Ex
     */
    private int deleteTempRow(Connection conn, String ids) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        if (conn == null) {
            conn = getConnection();
        }
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTempForIDSet,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), ids + ""});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete temprow sql: " + sqlQuery);
            }
            stmt = conn.createStatement();
            {
                nUpdate = stmt.executeUpdate(sqlQuery);
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + ids));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            //returnConnection(conn);
        }

        return nUpdate;
    }

    /**
     * 批量delete  .成批删除TempRow
     *
     * @param list
     * @return
     * @throws Ex
     */
    private int deleteTempRow(Connection conn, ArrayList list) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        if (conn == null) {
            conn = getConnection();
        }
        String sqlQuery = "";
        try {

            stmt = conn.createStatement();
            for (int i = 0; i < list.size(); i++) {
                TempRowBean tmp = (TempRowBean) list.get(i);
                sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTempForIDSet,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), tmp.getIds() + ""});
                //if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete temprow sql: " + sqlQuery);
                //}
                stmt.addBatch(sqlQuery);
            }
            int sizes[] = stmt.executeBatch();
            for (int i = 0; i < sizes.length; i++) {
                nUpdate = nUpdate + sizes[i];
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + sqlQuery));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            //returnConnection(conn);
        }

        return nUpdate;
    }

    private int deleteTempRow(ArrayList list) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        Connection conn = null;
        if (conn == null) {
            conn = getConnection();
        }
        String sqlQuery = "";
        try {
            for (int i = 0; i < list.size(); i++) {
                TempRowBean tmp = (TempRowBean) list.get(i);
                sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTempForIDSet,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), tmp.getIds() + ""});
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Delete temprow sql: " + sqlQuery);
                }
                stmt = conn.createStatement();
                {
                    nUpdate = +nUpdate + stmt.executeUpdate(sqlQuery);
                }
                list.remove(i);
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + sqlQuery));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            //returnConnection(conn);
        }

        return nUpdate;
    }

    private int deleteTempRow(String ids) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        Connection conn = null;
        if (conn == null) {
            conn = getConnection();
        }
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTempForIDSet,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), ids + ""});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete temprow sql: " + sqlQuery);
            }
            stmt = conn.createStatement();
            {
                nUpdate = stmt.executeUpdate(sqlQuery);
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + ids));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }

        return nUpdate;
    }

    /**
     * 批量delete  .成批删除TempRow
     *
     * @param Name 数据表名
     * @return
     * @throws Ex
     */
    private int deleteTempRowByTableName(String Name) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        Connection conn = null;
        if (conn == null) {
            conn = getConnection();
        }
        String sqlQuery = "";
        try {

            sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFormTempForTableName,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, m_databaseInfo.getTodoTable()), "'" + Name + "'"});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete temprow sql: " + sqlQuery);
            }
            stmt = conn.createStatement();
            {
                nUpdate = stmt.executeUpdate(sqlQuery);
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row by table name({0})sql:" + sqlQuery + ".", "" + Name));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }

        return nUpdate;
    }

    protected MDataConstructor processTempRows(Connection conn, List tempRows) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        MDataConstructor m_dataConstructor = new MDataConstructor();
        MDataConstructor m_result = new MDataConstructor();
        //Connection conn = null;
        Rows tempBasicRows = new Rows();
        Properties dataProps = new Properties();
        TempRow tempRow = null;
        testStatus();
        try {
            //conn = getConnection();
            if (m_databaseInfo.isTwoway() && m_syncTime > 0) {
                dataProps.setProperty(DbChangeSource.Str_ChangeTime, String.valueOf(m_syncTime));
            }
            for (int x = 0; x < tempRows.size(); x++) {
                tempRow = (TempRow) tempRows.get(x);
                String tableName = tempRow.getTableName();
                TableInfo tableInfo = findTableInfo(tableName);

                PkSet pks = tempRow.getPks();
                Column pkColumns[] = pks.getPkArray();


                if (tempRow.getAction() == Operator.OPERATOR__DELETE) {
                    Row datarow = new Row(m_databaseInfo.getName(), tableName);
                    datarow.setAction(Operator.OPERATOR__DELETE);

                    for (int j = 0; j < pkColumns.length; j++) {
                        Column c = pkColumns[j];
                        Column metaRow = m_databaseInfo.find(tableName, c.getName()).copyColumnWithoutValue();
                        if (metaRow != null) {
                            metaRow.setValue(c.getValue());
                            datarow.addColumn(metaRow);
                        }
                    }
                    if (m_databaseInfo.isTwoway()) {
                        datarow.setOp_time(tempRow.getActTime().getTime());
                    }
                    tempBasicRows.addRow(datarow);
                } else {
                    PreparedStatement prepStmt = null;
                    try {
                        Column[] clobColumns = tableInfo.getClobColumns();
                        Column[] blobColumns = tableInfo.getBlobColumns();
                        boolean hasClob = clobColumns.length > 0;
                        boolean hasBlob = blobColumns.length > 0;
                        boolean hasBothBlobAndClob = hasBlob && hasClob;

                        String strQuery = getTableQueryString(tableName, tableInfo.getBasicColumns());
                        String strWhere = getWhereStringFromPks(pkColumns);
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("Get where sql string(lob): " + strWhere);
                        }
                        prepStmt = m_I_dbOp.getSatement(conn, strQuery + strWhere, pkColumns,null);
                        prepStmt.setFetchSize(1);
                        ResultSet rs = prepStmt.executeQuery();

                        if (rs.next()) {
                            Row row = getRowData(tableInfo, rs);
                            if (m_databaseInfo.isTwoway()) {
                                row.setOp_time(tempRow.getActTime().getTime());
                            }
                            row.setAction(tempRow.getAction());
                            tempBasicRows.addRow(row);
                            multiData.setBasicData(m_databaseInfo.getName(), tableName, tempBasicRows);
                            if (hasBothBlobAndClob) {
                                for (int i = 0; i < blobColumns.length; i++) {
                                    Column c = blobColumns[i].copyColumnWithoutValue();
                                    c = getLobData(tableName, c, pkColumns, strWhere);
                                    if (!c.isNull()) {
                                        multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                                    } //else {}
                                }

                                for (int i = 0; i < clobColumns.length; i++) {
                                    Column c = clobColumns[i].copyColumnWithoutValue();
                                    c = getLobData(tableName, c, pkColumns, strWhere);
                                    if (!c.isNull()) {
                                        multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                                    }
                                }

                            } else {
                                if (hasBlob) {
                                    for (int i = 0; i < blobColumns.length; i++) {
                                        Column c = blobColumns[i].copyColumnWithoutValue();
                                        c = getLobData(tableName, c, pkColumns, strWhere);
                                        if (!c.isNull()) {
                                            multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                                        } //else {}
                                    }
                                }
                                if (hasClob) {
                                    for (int i = 0; i < clobColumns.length; i++) {
                                        Column c = clobColumns[i].copyColumnWithoutValue();
                                        c = getLobData(tableName, c, pkColumns, strWhere);
                                        if (!c.isNull()) {
                                            multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                                        }
                                    }
                                }
                            }
                        } else {
                            //deleteTempRow(tempRow.getId(), conn);
                            deleteTempRow(tempRow.getId());
                            multiData = null;
                            m_logger.warn("Can not find DbChangeSource temp-data from temp table.");
                            m_log.setTableName(tableName);
                            m_log.setStatusCode(EStatus.E_PackDataFaild.getCode() + "");
                            m_log.warn("Can not find DbChangeSource temp-data from temp table.");
                        }
                        rs.close();
                    } catch (SQLException sqlEEx) {
                        EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while processing the temp row('{0}') of table '{1}'.", "" + tempRow.getId(), tempRow.getTableName()));
                        EStatus status = EStatus.E_DATABASEERROR;
                        status.setDbInfo(m_dbName, m_dbDesc);
                        m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + tempRow.getTableName(), true);
                        testBadConnection(conn, Exsql);
                        throw Exsql;
                    } finally {
                        DbopUtil.closeStatement(prepStmt);

                    }
                }


            }
            ArrayListInputStream data = new ArrayListInputStream();
            if (tempRow != null)
                m_dataConstructor.setBasicData(m_databaseInfo.getName(), tempRow.getTableName(), tempBasicRows, dataProps);
            else
                m_dataConstructor.setBasicData(m_databaseInfo.getName(), "", tempBasicRows, dataProps);
            m_dataConstructor.updateHeader(multiData.getHeader());
            data.addInputStream(m_dataConstructor.getDataInputStream());
            data.addInputStream(multiData.getDataWhitoutBaseInputStream());
            m_result = new MDataConstructor(m_dataConstructor.getHeader(), data, m_dataConstructor.getData());
        } finally {
            //returnConnection(conn);
        }
        return m_result;
    }

    protected MDataConstructor processTempRows(List tempRows) throws Ex {

        MDataConstructor multiData = new MDataConstructor();
        MDataConstructor m_dataConstructor = new MDataConstructor();
        MDataConstructor m_result = new MDataConstructor();
        Connection conn = null;
        Rows tempBasicRows = new Rows();
        Properties dataProps = new Properties();
        TempRow tempRow = null;
        testStatus();
        try {
            conn = getConnection();
            if (m_databaseInfo.isTwoway() && m_syncTime > 0) {
                dataProps.setProperty(DbChangeSource.Str_ChangeTime, String.valueOf(m_syncTime));
            }
            for (int x = 0; x < tempRows.size(); x++) {
                tempRow = (TempRow) tempRows.get(x);
                String tableName = tempRow.getTableName();
                TableInfo tableInfo = findTableInfo(tableName);

                PkSet pks = tempRow.getPks();
                Column pkColumns[] = pks.getPkArray();


                if (tempRow.getAction() == Operator.OPERATOR__DELETE) {
                    Row datarow = new Row(m_databaseInfo.getName(), tableName);
                    datarow.setAction(Operator.OPERATOR__DELETE);

                    for (int j = 0; j < pkColumns.length; j++) {
                        Column c = pkColumns[j];
                        Column metaRow = m_databaseInfo.find(tableName, c.getName()).copyColumnWithoutValue();
                        if (metaRow != null) {
                            metaRow.setValue(c.getValue());
                            datarow.addColumn(metaRow);
                        }
                    }
                    if (m_databaseInfo.isTwoway()) {
                        datarow.setOp_time(tempRow.getActTime().getTime());
                    }
                    tempBasicRows.addRow(datarow);
                } else {
                    PreparedStatement prepStmt = null;
                    try {
                        Column[] clobColumns = tableInfo.getClobColumns();
                        Column[] blobColumns = tableInfo.getBlobColumns();
                        boolean hasClob = clobColumns.length > 0;
                        boolean hasBlob = blobColumns.length > 0;
                        boolean hasBothBlobAndClob = hasBlob && hasClob;

                        String strQuery = getTableQueryString(tableName, tableInfo.getBasicColumns());
                        String strWhere = getWhereStringFromPks(pkColumns);
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("Get where sql string(lob): " + strWhere);
                        }
                        prepStmt = m_I_dbOp.getSatement(conn, strQuery + strWhere, pkColumns,null);
                        prepStmt.setFetchSize(1);
                        ResultSet rs = prepStmt.executeQuery();

                        if (rs.next()) {
                            Row row = getRowData(tableInfo, rs);
                            if (m_databaseInfo.isTwoway()) {
                                row.setOp_time(tempRow.getActTime().getTime());
                            }
                            tempBasicRows.addRow(row);
                            multiData.setBasicData(m_databaseInfo.getName(), tableName, tempBasicRows);
                            if (hasBothBlobAndClob) {
                                for (int i = 0; i < blobColumns.length; i++) {
                                    Column c = blobColumns[i].copyColumnWithoutValue();
                                    c = getLobData(tableName, c, pkColumns, strWhere);
                                    if (!c.isNull()) {
                                        multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                                    } //else {}
                                }

                                for (int i = 0; i < clobColumns.length; i++) {
                                    Column c = clobColumns[i].copyColumnWithoutValue();
                                    c = getLobData(tableName, c, pkColumns, strWhere);
                                    if (!c.isNull()) {
                                        multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                                    }
                                }

                            } else {
                                if (hasBlob) {
                                    for (int i = 0; i < blobColumns.length; i++) {
                                        Column c = blobColumns[i].copyColumnWithoutValue();
                                        c = getLobData(tableName, c, pkColumns, strWhere);
                                        if (!c.isNull()) {
                                            multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                                        } //else {}
                                    }
                                }
                                if (hasClob) {
                                    for (int i = 0; i < clobColumns.length; i++) {
                                        Column c = clobColumns[i].copyColumnWithoutValue();
                                        c = getLobData(tableName, c, pkColumns, strWhere);
                                        if (!c.isNull()) {
                                            multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                                        }
                                    }
                                }
                            }
                        } else {
                            //deleteTempRow(tempRow.getId(), conn);
                            deleteTempRow(tempRow.getId());
                            // multiData = null;
                            m_logger.warn("Can not find DbChangeSource temp-data from temp table.");
                            m_log.setTableName(tableName);
                            m_log.setStatusCode(EStatus.E_PackDataFaild.getCode() + "");
                            m_log.warn("Can not find DbChangeSource temp-data from temp table.");
                        }
                        rs.close();
                    } catch (SQLException sqlEEx) {
                        EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while processing the temp row('{0}') of table '{1}'.", "" + tempRow.getId(), tempRow.getTableName()));
                        EStatus status = EStatus.E_DATABASEERROR;
                        status.setDbInfo(m_dbName, m_dbDesc);
                        m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + tempRow.getTableName(), true);
                        testBadConnection(conn, Exsql);
                        throw Exsql;
                    } finally {
                        DbopUtil.closeStatement(prepStmt);

                    }
                }


            }
            ArrayListInputStream data = new ArrayListInputStream();
            if (tempRow != null)
                m_dataConstructor.setBasicData(m_databaseInfo.getName(), tempRow.getTableName(), tempBasicRows, dataProps);
            else
                m_dataConstructor.setBasicData(m_databaseInfo.getName(), "", tempBasicRows, dataProps);
            m_dataConstructor.updateHeader(multiData.getHeader());
            data.addInputStream(m_dataConstructor.getDataInputStream());
            data.addInputStream(multiData.getDataWhitoutBaseInputStream());
            m_result = new MDataConstructor(m_dataConstructor.getHeader(), data, m_dataConstructor.getData());
        } finally {
            returnConnection(conn);
        }
        return m_result;
    }

    private Column getLobData(String tableName, Column lobColumn, Column[] pkColumns, String strWhere) throws Ex {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Get table query where sql string(lob): " + strWhere);
        }
        String strLobQuery = getTableQueryString(tableName, new Column[]{lobColumn});
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Get table query sql string(lob): " + strLobQuery);
        }
        strLobQuery += strWhere;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Query lob table sql string(lob): " + strLobQuery);
        }
        testStatus();
        if (m_conn == null)
            m_conn = getConnection();
        if (m_conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement prepStmt = null;
        try {
            prepStmt = m_I_dbOp.getReadOnlySatement(m_conn, strLobQuery, pkColumns,null);
            prepStmt.setMaxRows(1);
            //prepStmt.setQueryTimeout(I_StatementTimeOut);
            ResultSet rs = prepStmt.executeQuery();

            if (rs.next()) {
                return m_I_dbOp.getColumnData(lobColumn, rs, 1);
            }
            rs.close();
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to retrieve LOB column '{0}' from the table '{1}'.", lobColumn.getName(), tableName));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + tableName, true);
            testBadConnection(m_conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
            //returnConnection(conn);
        }

        return lobColumn;
    }


    private Row getRowData(TableInfo tableInfo, ResultSet rs) throws SQLException {
        String tableName = tableInfo.getName();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        int columnCount = rsMetaData.getColumnCount();
        Row row = new Row(m_databaseInfo.getName(), tableName);
        for (int i = 0; i < columnCount; i++) {
            String columnName = rsMetaData.getColumnName(i + 1);
            Column column = tableInfo.find(columnName).copyColumnWithoutValue();
            if (!column.isLobType()) {
                column = m_I_dbOp.getColumnData(column, rs, i + 1);
            }
            row.addColumn(column);
        }
        return row;
    }

    private String getTableQueryString(String tableName, Column[] cs) throws Ex {
        TableInfo tableInfo = findTableInfo(tableName);
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }

        try {
            StringBuffer result = new StringBuffer();
            int columnSize = cs.length;
            if (columnSize != 0) {
                result.append("select ");
                for (int i = 0; i < columnSize; i++) {
                    String columnName = cs[i].getName();
                    result.append(m_I_dbOp.formatColumnName(columnName));
                    if (i != columnSize - 1) {
                        result.append(",");
                    }
                }
            }
            result.append(" from ");
            result.append(m_I_dbOp.formatTableName(m_schemaName, tableName));
            return result.toString();
        } catch (SQLException eEx) {
            throw new Ex().set(eEx);
        }
    }


    private String getWhereStringFromPks(Column[] pkColumns) throws SQLException {

        String strWhere = " where ";
        for (int i = 0; i < pkColumns.length; i++) {
            Column pkColumn = pkColumns[i];
            strWhere += m_I_dbOp.formatColumnName(pkColumn.getName());
            if (i != pkColumns.length - 1) {
                strWhere += "=? and ";
            } else {
                strWhere += "=?";
            }
        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Get where string: " + strWhere);
        }
        return strWhere;
    }


    private Column[] addTwoTypeColumns(Column[] columns1, Column[] columns2) {
        ArrayList columnList = new ArrayList();
        for (int i = 0; i < columns1.length; i++) {
            columnList.add(columns1[i]);
        }
        for (int i = 0; i < columns2.length; i++) {
            columnList.add(columns2[i]);
        }

        return (Column[]) columnList.toArray(new Column[0]);
    }


    private void auditProcess(String tableName, int processedRows) throws Ex {

        StringBuffer buff = new StringBuffer();
        buff.append("Data Source Audit Info (ChangeType/SourceDb/Table)/");
        buff.append(appType);
        buff.append("/");
        buff.append(getDbName());
        buff.append("/");
        buff.append(tableName);
        buff.append("/");
        buff.append(processedRows + " record.");
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("start auditProcess:" + tableName);
        }
        auditLogger.info(buff.toString());
        m_changeMain.setStatus(m_changeType, EStatus.E_OK, "正常", true);
        //m_logger.info("数据源端审计信息(业务表):" + tableName + " ," + processedRows + "条记录.");
        m_log.setTableName(tableName);
        if (m_dbSource != null) {
            m_log.setSource_ip(m_dbSource.getDbHost());
            m_log.setUserName(m_dbSource.getDbUser());
        }
        m_log.setOperate("readdb");
        m_log.setStatusCode(EStatus.E_OK.getCode() + "");
        m_log.setRecordCount(processedRows + "");
        m_log.info(buff.toString());
        m_log.setRecordCount("" + processedRows);

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("edit auditProcess:" + tableName);
        }

    }

    private void processMaxRecordsForOldStep(String query, TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = null;
        testStatus();
        Connection conn = getConnection();
        if (conn == null) {
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            dataBaseRetryProcess(conn);
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;
        try {

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(max);
            //stmt.setQueryTimeout(I_StatementTimeOut);
            ResultSet rs = stmt.executeQuery(query);
            boolean done = false;
            multiData = new MDataConstructor();
            do {
                try {
                    multiData = dataConstructor(rs, tableInfo, max);
                    if (multiData.getData().getRowArray().length > 0) {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("basic row length:" + multiData.getData().getRowArray().length);
                        }
                        if (isMergeTable(tableInfo.getName())) {
                            multiData.getHeader().getProperties().setProperty("mergetable", tableInfo.getName());
                        }
                        if (tableInfo.isMergeTable()) {
                            disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), false, tableInfo.getAllMergeTable());
                        } else {
                            disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), false);
                        }

                    } else {
                        done = true;
                    }
                } catch (Ex
                        ex) {
                    if (ex.getErrcode().toInt() == EDbChange.I_TargetProcessError) {
                        sleepTime();
                    } else {
                        throw ex;
                    }

                }

            }
            while (!done);
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException
                e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbName);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + "table:" + tableInfo.getName(), true);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    private void processMaxRecordsForTimeSync(String query, TableInfo tableInfo, int max, Timestamp begin, Timestamp end, int rownum) throws Ex {
        MDataConstructor multiData = null;
        testStatus();
        Connection conn = getConnection();
        if (conn == null) {
            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            dataBaseRetryProcess(conn);
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement stmt = null;
        int row = 0;
        try {

            stmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("timeSync query sql:" + query);
            }
            stmt.setFetchSize(max);
            //stmt.setMaxRows();
            //stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setTimestamp(1, begin);
            stmt.setTimestamp(2, end);
            ResultSet rs = stmt.executeQuery();
            boolean done = false;
            if (rownum > 0) {
                for (int i = 0; i < rownum; i++) {
                    rs.next();
                }
                //rs.absolute(rownum);
            }

            do {
                try {
                    multiData = dataConstructor(rs, tableInfo, max);
                    row = rs.getRow();
                    if (multiData.getData().size() > 0) {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("basic row length:" + multiData.getData().getRowArray().length);
                        }
                        if (isMergeTable(tableInfo.getName())) {
                            multiData.getHeader().getProperties().setProperty("mergetable", tableInfo.getName());
                        }
                        if (tableInfo.isMergeTable()) {
                            disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), true, tableInfo.getAllMergeTable());
                        } else {
                            disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
                        }
                        //okay

                        updateTimeSyncPksetAndTime(begin, end, multiData.getData().getRowArray()[multiData.getData().getRowArray().length - 1], tableInfo, false, row);
                    } else {
                        updateTimeSyncPksetAndTime(begin, end, null, tableInfo, true, 0);
                        done = true;
                    }
                } catch (Ex
                        ex) {
                    if (multiData != null && multiData.getData().size() > 0) {

                        if (row >= multiData.getData().size()) {
                            row = row - multiData.getData().size();
                        } else {
                            row = 0;
                        }
                        updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, row);
                    }
                    switch (ex.getErrcode().toInt()) {
                        case EDbChange.I_TargetProcessError:
                            sleepTime();
                            break;
                        default:
                            throw ex;

                    }
                    /*if (ex.getErrcode().toInt() == EDbChange.I_TargetProcessError) {
                        sleepTime();
                    } else {
                        throw ex;
                    }*/

                }
                try {
                    multiData.getData().clear();
                } catch (Ex ex) {
                    //okay
                }
            }
            while (!done);
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException
                e) {
            if (multiData != null && multiData.getData().size() > 0) {

                if (row >= multiData.getData().size()) {
                    row = row - multiData.getData().size();
                } else {
                    row = 0;
                }
                updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, row);
            }
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbName);
            m_logger.warn("An error occured while exporting the table,SQL:" + query);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + "table:" + tableInfo.getName(), true);
            throw Exsql;


        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    public boolean isTempRowNew
            (String
                     dbname, String
                    tableName, Column[] pkColumns, long time) throws Ex {
        boolean result = false;
        String pkName = "";
        String pkType = "";
        String pkValue = "";
        String pks = "";
        if (dbname == null || tableName == null) {
            throw new Ex().set(E.E_Unknown, new Message("Schema or TableName is null."));
        }

        for (int i = 0; i < pkColumns.length; i++) {
            pkName = pkName + pkColumns[i].getName();
            pkType = pkType + pkColumns[i].getDbType();
            pkValue = pkValue + pkColumns[i].getValue().getValueString();
            if (i < pkColumns.length - 1) {
                pkName = pkName + ",";
                pkType = pkType + ",";
                pkValue = pkValue + ",";
            }
        }
        pks = pkName + ";";
        pks = pks + pkType + ";";
        pks = pks + pkValue + ";";
        if (!isOkay()) {
            Message Message = new Message("数据库异常:{0}.", m_dbName);
            m_logger.warn(Message.toString());
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.warn(Message.toString());
            sleepTime();
            testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Connection conn = null;
        if (conn == null)
            conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement stmt = null;
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectCountFromTempForOp_time,
                    new String[]{m_databaseInfo.getTodoTable()});
            if (m_logger.isDebugEnabled()) {
                m_logger.info("Str_Sql_SelectCountFromTempForOp_time sql:" + sqlQuery);
            }
            stmt = conn.prepareStatement(sqlQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, dbname);
            stmt.setString(2, tableName);
            stmt.setString(3, pks);
            stmt.setTimestamp(4, new Timestamp(m_syncTime + time + 1 * 60 * 1000));
            stmt.setQueryTimeout(I_StatementTimeOut);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                if (rs.getInt(1) > 0)
                    result = true;
                else
                    result = false;
            }
            stmt.close();

            return result;
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to retrieve temp data for the table {0}.", m_databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            m_log.warn("应用为：" + appType + "判断是否为临时记录错误.", sqlEEx);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    public long getDbSysDate
            () throws Ex {
        long result = 0;
        testStatus();
        Connection conn = null;
        if (conn == null)
            conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement stmt = null;
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectCountFromTempForSys_time,
                    new String[]{m_databaseInfo.getTodoTable()});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Str_Sql_SelectCountFromTempForSys_time sql:" + sqlQuery);
            }
            stmt = conn.prepareStatement(sqlQuery);
            stmt.setQueryTimeout(I_StatementTimeOut);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                result = rs.getTimestamp(1).getTime();
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to retrieve temp data for the table {0}.", m_databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            m_log.warn("应用为：" + appType + "取的数据库时间错误.", e);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return result;
    }

    public Timestamp getTimeSyncInitDate
            (String time, String tableName) throws Ex {
        Timestamp result = new Timestamp(0);
        testStatus();
        Connection conn = null;
        if (conn == null)
            conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_TimeSyncInitDate,
                    new String[]{time, tableName});
            if (m_logger.isDebugEnabled()) {
                m_logger.info("Str_Sql_SelectCountFromTempForSys_time sql:" + sqlQuery);
            }
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setMaxRows(1);
            stmt.setFetchSize(1);
            ResultSet rs = stmt.executeQuery(sqlQuery);

            if (rs.next()) {
                result.setTime(rs.getTimestamp(1).getTime());
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to retrieve temp data for the table {0}.", m_databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            m_log.warn("应用为：" + appType + " 初始化时间标记读取错误.", e);
            throw Exsql;
        } catch (NullPointerException e) {
            m_log.warn("应用为：" + appType + " 初始化时间标记读取错误.", e);
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return result;
    }

    public Timestamp getTimeSyncEndDate
            (String time, String tablename) throws Ex {
        Timestamp result = new Timestamp(0);
        testStatus();
        Connection conn = null;
        if (conn == null)
            conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_TimeSyncEndDate,
                    new String[]{time, tablename});
            //if (m_logger.isDebugEnabled()) {
            m_logger.info("Str_Sql_TimeSyncEndDate sql:" + sqlQuery);
            //}
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setMaxRows(1);
            stmt.setFetchSize(1);
            ResultSet rs = stmt.executeQuery(sqlQuery);

            if (rs.next()) {
                result.setTime(rs.getTimestamp(1).getTime());
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to retrieve temp data for the table {0}.", m_databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            m_log.warn("应用为：" + appType + " 结束时间标记读取错误.", e);
            throw Exsql;
        } catch (NullPointerException e) {
            m_log.warn("应用为：" + appType + " 结束时间标记读取错误.", e);
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return result;
    }

    public void initChangeTwoWay
            (
                    long time) throws Ex {

        testStatus();
        Connection conn = null;
        if (conn == null)
            conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement stmt = null;
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_InitChangeTwoWay,
                    new String[]{m_databaseInfo.getTodoTable()});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Str_Sql_InitChangeTwoWay sql:" + sqlQuery);
            }
            stmt = conn.prepareStatement(sqlQuery);
            stmt.setTimestamp(1, new Timestamp(time));
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.executeUpdate();


        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to retrieve temp data for the table {0}.", m_databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    /**
     * process  for temp.
     *
     * @param rs
     * @param max
     * @return
     * @throws Ex
     */
    private int dataConstructorForTemp (Connection conn, ResultSet  rs, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        HashMap map = new HashMap();
        ArrayList list = new ArrayList();
        ArrayList list1 = new ArrayList();
        try {
            TempRow row = new TempRow();
            for (int i = 0; i < max && rs.next(); i++) {
                row = new TempRow();
                row.setId(rs.getLong(1));
                row.setDatabaseName(rs.getString(2));
                row.setTableName(rs.getString(3));
                row.setPks(rs.getString(4));
                row.setAct(rs.getString(5));
                row.setActTime(rs.getTimestamp(6));
                list.add(row);
                map.put(row, row);
            }
            if (rs != null) {
                rs.close();
            }
            TableInfo tableinfo = findTableInfo(row.getTableName());
            list1 = preprocessForTemp(map);
            if (list1.size() == 0) {
                if (list.size() > 0)
                    deleteTempRow(conn, getTempRowIdSet(list));
                return 0;
            }
            multiData = processTempRows(list1);
            if (multiData.getData().getRowArray().length > 0) {
                if (tableinfo.isMergeTable()) {
                    disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), true, tableinfo.getAllMergeTable());
                } else {
                    disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
                }
            }
            //删除临时记录
            if (list.size() > 0)
                deleteTempRow(conn, getTempRowIdSet(list));
        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table."));
            throw Exsql;
        }
        return list.size();
    }

    /**
     * process  for temp.
     *
     * @param rs
     * @param max
     * @return
     * @throws Ex
     */
    private TempRowBean dataConstructorForTemp (ResultSet rs, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        HashMap map = new HashMap();
        ArrayList list = new ArrayList();
        ArrayList list1 = new ArrayList();
        TempRowBean result = new TempRowBean();
        long maxrecode = 0;
        try {
            TempRow row = new TempRow();
            String action = null;
            String action_temp = null;
            String tableName = null;
            String tableName_temp = null;

            for (int i = 0; i < max && rs.next(); i++) {
                row = new TempRow();
                row.setId(rs.getLong(1));
                row.setDatabaseName(rs.getString(2));
                tableName = rs.getString(3);
                row.setTableName(tableName);
                row.setPks(rs.getString(4));
                action = rs.getString(5);
                row.setAct(action);
                row.setActTime(rs.getTimestamp(6));
                if(action.equals(action_temp) && tableName.equals(tableName_temp)) {
                    list.add(row);
                    if (row.getId() > maxrecode) {
                        maxrecode = row.getId();
                    }
                    map.put(row, row);
                } else {
                    if(action_temp!=null && tableName_temp!=null) {
                        TableInfo tableinfo = findTableInfo(row.getTableName());
                        list1 = preprocessForTemp(map);
                        multiData = processTempRows(list1);
                        if (multiData.getData().getRowArray().length > 0) {
                            if (tableinfo.isMergeTable()) {
                                disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), true, tableinfo.getAllMergeTable());
                            } else {
                                disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
                            }
                        }
                        multiData = new MDataConstructor();
                        list1 = new ArrayList();
                        map = new HashMap();
                        list = new ArrayList();
                    }
                    list.add(row);
                    if (row.getId() > maxrecode) {
                        maxrecode = row.getId();
                    }
                    map.put(row, row);
                }
                action_temp = action;
                tableName_temp = tableName;
            }

            TableInfo tableinfo = findTableInfo(row.getTableName());
            list1 = preprocessForTemp(map);
            if (list1.size() == 0) {
                if (list.size() > 0) {

                    String ids = getTempRowIdSet(list);
                    m_logger.warn("delete two way sync temp row recoder:"+ids);
                    deleteTempRow(ids);
                }
                return new TempRowBean();
            }
            multiData = processTempRows(list1);
            if (multiData.getData().getRowArray().length > 0) {
                if (tableinfo.isMergeTable()) {
                    disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), true, tableinfo.getAllMergeTable());
                } else {
                    disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
                }
            }
            //删除临时记录
            if (list.size() > 0) {
                String ids = getTempRowIdSet(list);
                //if (!m_dbType.equals(DBType.C_MSSQL)) {
                deleteTempRow(ids);
                //}
                result.setLength(list.size());
                result.setMaxId(maxrecode);
                result.setIds(ids);
            }
        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table."));
            throw Exsql;
        }
        return result;
    }

    private TempRowBean dataConstructorForTemp_old_single (ResultSet rs, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        HashMap map = new HashMap();
        ArrayList list = new ArrayList();
        ArrayList list1 = new ArrayList();
        TempRowBean result = new TempRowBean();
        long maxrecode = 0;
        try {
            TempRow row = new TempRow();
            for (int i = 0; i < max && rs.next(); i++) {
                row = new TempRow();
                row.setId(rs.getLong(1));
                row.setDatabaseName(rs.getString(2));
                row.setTableName(rs.getString(3));
                row.setPks(rs.getString(4));
                row.setAct(rs.getString(5));
                row.setActTime(rs.getTimestamp(6));
                list.add(row);
                if (row.getId() > maxrecode) {
                    maxrecode = row.getId();
                }
                map.put(row, row);
            }

            TableInfo tableinfo = findTableInfo(row.getTableName());
            list1 = preprocessForTemp(map);
            if (list1.size() == 0) {
                if (list.size() > 0) {

                    String ids = getTempRowIdSet(list);
                    m_logger.warn("delete two way sync temp row recoder:"+ids);
                    deleteTempRow(ids);
                }
                return new TempRowBean();
            }
            multiData = processTempRows(list1);
            if (multiData.getData().getRowArray().length > 0) {
                if (tableinfo.isMergeTable()) {
                    disposeDataByMerge(multiData.getDataInputStream(), multiData.getData().getRowArray(), true, tableinfo.getAllMergeTable());
                } else {
                    disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
                }
            }
            //删除临时记录
            if (list.size() > 0) {
                String ids = getTempRowIdSet(list);
                //if (!m_dbType.equals(DBType.C_MSSQL)) {
                deleteTempRow(ids);
                //}
                result.setLength(list.size());
                result.setMaxId(maxrecode);
                result.setIds(ids);
            }
        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table."));
            throw Exsql;
        }
        return result;
    }

    /**
     * 预处理TempRow 合并对相同记录.以最后更新的时间为准.
     *
     * @param list
     * @return List
     */
    private ArrayList preprocessForTemp(Map list) throws Ex {

        ArrayList result = new ArrayList();
        Iterator tempRows = list.keySet().iterator();
        //HashMap map = new HashMap();
        //HashMap tememap = new HashMap();
        boolean isEqual = false;
        for (; tempRows.hasNext(); ) {
            TempRow tempRow = (TempRow) tempRows.next();
            String tableName = tempRow.getTableName();
            PkSet pks = tempRow.getPks();
            Column pkColumns[] = null;
            try {
                pkColumns = pks.getPkArray();
            } catch (Ex ex) {
                continue;
            }
            SourceObject sourceObject = new SourceObject(getDbName(), tableName, tempRow.getAction());
            for (int j = 0; j < pkColumns.length; j++) {
                Column c = pkColumns[j];
                Column metaRow = m_databaseInfo.find(tableName, c.getName()).copyColumnWithoutValue();
                if (metaRow != null) {
                    c.setJdbcType(metaRow.getJdbcType());
                    sourceObject.addPk(c.getName(), c.getObject());
                } else {
                    Message Message = new Message("临时表中业务表的主键名不同于配置的主键名,需要用户干预.");
                    Ex Ex = new Ex().set(EDbChange.E_DataIsNullErrorr, Message);
                    m_logger.warn(Message.toString(), Ex);
                    m_log.warn(Message.toString(), Ex);
                    throw Ex;
                }
            }
            boolean inputCache = m_objectCache.remove(sourceObject);

            if (!inputCache) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("One pk object is removed from cache, cache size=" + m_objectCache.size());
                }
                //TempRow tempRow = (TempRow) tememap.get(sourceObject);
                result.add(tempRow);
            }
            //tememap.put(sourceObject, tempRow);
            //map.put(sourceObject, sourceObject);

        }
        /* Iterator itr = map.values().iterator();
        for (; itr.hasNext();) {
            SourceObject sourceObject = (SourceObject) itr.next();
            boolean inputCache = m_objectCache.remove(sourceObject);

            if (!inputCache) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("One pk object is removed from cache, cache size=" + m_objectCache.size());
                }
                TempRow tempRow = (TempRow) tememap.get(sourceObject);
                result.add(tempRow);
            }
        }*/

        return result;
    }

    private String getTempRowIdSet
            (List
                     tempRows) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < tempRows.size(); i++) {
            TempRow temp = (TempRow) tempRows.get(i);
            buff.append(String.valueOf(temp.getId()));
            if (i < tempRows.size() - 1) {
                buff.append(",");
            }
        }
        return buff.toString();
    }

    private MDataConstructor dataConstructor (ResultSet
                     rs, TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        MDataConstructor m_dataConstructor = new MDataConstructor();
        MDataConstructor m_result = new MDataConstructor();
        Rows basicRows = new Rows();
        String tableName = tableInfo.getName();
        Column[] clobColumns = tableInfo.getClobColumns();
        Column[] blobColumns = tableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasBothBlobAndClob = hasBlob && hasClob;
        PkSet pks = null;
        try {
            for (int x = 0; x < max && rs.next(); x++) {
                Row row = getRowData(tableInfo, rs);
                pks = new PkSet(PkSet.getPks(row));
                basicRows.addRow(row);

                int j = tableInfo.getBasicColumns().length + 1;
                if (hasBothBlobAndClob) {

                    for (int i = 0; i < blobColumns.length; i++) {
                        Column c = blobColumns[i].copyColumnWithoutValue();
                        c = m_I_dbOp.getColumnData(c, rs, j);
                        if (!c.isNull()) {
                            multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                        } else {
                            multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, null, 0);
                        }
                        j++;
                    }
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Lob column index: " + j);
                    }
                    for (int i = 0; i < clobColumns.length; i++) {
                        Column c = clobColumns[i].copyColumnWithoutValue();
                        c = m_I_dbOp.getColumnData(c, rs, j);
                        if (!c.isNull()) {
                            multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                        } else {
                            multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, null, 0);
                        }
                        j++;
                    }
                } else {
                    if (hasBlob) {
                        for (int i = 0; i < blobColumns.length; i++) {
                            Column c = blobColumns[i].copyColumnWithoutValue();
                            c = m_I_dbOp.getColumnData(c, rs, j);
                            if (!c.isNull()) {
                                multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                            } else {
                                multiData.addBlobData(m_databaseInfo.getName(), tableName, c.getName(), pks, null, 0);
                            }
                            j++;
                        }
                    } else if (hasClob) {
                        for (int i = 0; i < clobColumns.length; i++) {
                            Column c = clobColumns[i].copyColumnWithoutValue();
                            c = m_I_dbOp.getColumnData(c, rs, j);
                            if (!c.isNull()) {
                                multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                            } else {
                                multiData.addClobData(m_databaseInfo.getName(), tableName, c.getName(), m_databaseInfo.getEncoding(), pks, null, 0);
                            }
                            j++;
                        }
                    }
                }

            }

            ArrayListInputStream data = new ArrayListInputStream();
            m_dataConstructor.setBasicData(m_databaseInfo.getName(), tableName, basicRows);
            m_dataConstructor.updateHeader(multiData.getHeader());
            data.addInputStream(m_dataConstructor.getDataInputStream());
            data.addInputStream(multiData.getDataWhitoutBaseInputStream());
            m_result = new MDataConstructor(m_dataConstructor.getHeader(), data, basicRows);

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableName));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + "table:" + tableName, true);
            throw Exsql;
        }
        return m_result;
    }

    private MDataConstructor dataConstructorSequence (int id,
                                                      TableInfo tableInfo) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        MDataConstructor m_dataConstructor = new MDataConstructor();
        MDataConstructor m_result = new MDataConstructor();
        Rows basicRows = new Rows();
        String tableName = tableInfo.getName();

        int columnCount = 1;
        Row row = new Row(m_databaseInfo.getName(), tableName);
        String columnName = "last_number";
        Column column = new Column(columnName,2,"NUMBER",false);
        column.setValue(new Value(""+id));
        row.addColumn(column);
        basicRows.addRow(row);

        ArrayListInputStream data = new ArrayListInputStream();
        m_dataConstructor.setSequenceData(m_databaseInfo.getName(), tableName, basicRows);
//        m_dataConstructor.updateHeader(multiData.getHeader());
        data.addInputStream(m_dataConstructor.getDataInputStream());
        data.addInputStream(multiData.getDataWhitoutBaseInputStream());
        m_result = new MDataConstructor(m_dataConstructor.getHeader(), data, basicRows);

        return m_result;
    }

    /**
     * @param data
     * @param rows
     * @throws Ex
     */
    private void disposeData (InputStream
             data, Row[] rows, boolean isTemp) throws Ex {

        if (rows.length > 0) {
            // dispose
            DataAttributes result;
            try {
                if (m_changeMain.isNetWorkOkay())
                    result = m_changeMain.dispose(m_changeType, data, new DataAttributes());
                else
                    throw new Ex().set(EDbChange.E_NetWorkError);
            } catch (Ex Ex) {
                EStatus status = EStatus.E_NetWorkError;
                status.setDbInfo(m_dbName, m_dbDesc);
                m_changeMain.setStatus(m_changeType, status, null, true);
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ie) {
                    //okay
                }
                throw Ex;
            }
            if (result.getStatus().isSuccess()) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("process status Value:" + result.getStatus().isSuccess());
                }
                if (!isTemp) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("isTemp Value:" + isTemp);
                    }
                    for (int j = 0; j < rows.length; j++) {
                        Row r = rows[j];
                        postProcessor(r);
                    }
                }
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("process status Value:" + result.getStatus().isSuccess());
                }
                auditProcess(rows[0].getTableName(), rows.length);
            }

            if (!result.getStatus().isSuccess()) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("process status Value:" + result.getStatus().isSuccess());
                }
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ie) {
                    //okay
                }
                Message Message = new Message("目标端处理出错.");
                throw new Ex().set(EDbChange.E_TargetProcessError, Message);
            }

        } else {
            Message Message = new Message("数据为空,不传输.");

            throw new Ex().set(EDbChange.E_DataIsNullErrorr, Message);
        }

        //

    }

    private void disposeDataSequence (InputStream
             data, Row[] rows, boolean isTemp) throws Ex {
        DataAttributes result;
        try {
            if (m_changeMain.isNetWorkOkay())
                result = m_changeMain.dispose(m_changeType, data, new DataAttributes());
            else
                throw new Ex().set(EDbChange.E_NetWorkError);
        } catch (Ex Ex) {
            EStatus status = EStatus.E_NetWorkError;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, null, true);
            try {
                Thread.sleep(Int_Thread_SleepTime);
            } catch (InterruptedException ie) {
                //okay
            }
            throw Ex;
        }
        if(data!=null) {
            try {
                data.close();
            } catch (IOException e) {
            }
        }
        if (result.getStatus().isSuccess()) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("process status Value:" + result.getStatus().isSuccess());
            }
            auditProcess(rows[0].getTableName(), rows.length);
        }
        if (!result.getStatus().isSuccess()) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("process status Value:" + result.getStatus().isSuccess());
            }
            try {
                Thread.sleep(Int_Thread_SleepTime);
            } catch (InterruptedException ie) {
                //okay
            }
            Message Message = new Message("目标端处理出错.");
            throw new Ex().set(EDbChange.E_TargetProcessError, Message);
        }

    } 

    /**
     * mergeTable in to one table
     *
     * @param data
     * @param rows
     * @throws Ex
     */
    private void disposeDataByMerge
    (InputStream
             data, Row[] rows, boolean isTemp, MergeTableInfo[]
            mergetables) throws Ex {


        if (rows.length > 0) {
            // dispose
            DataAttributes result;
            try {
                if (m_changeMain.isNetWorkOkay())
                    result = m_changeMain.dispose(m_changeType, data, new DataAttributes());
                else
                    throw new IOException();
            } catch (Ex Ex) {
                EStatus status = EStatus.E_NetWorkError;
                status.setDbInfo(m_dbName, m_dbDesc);
                m_changeMain.setStatus(m_changeType, status, null, true);
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ie) {
                    //okay
                }
                throw Ex;
            } catch (IOException e) {
                throw new Ex().set(E.E_Unknown);
            }
            if (result.getStatus().isSuccess()) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("process status Value:" + result.getStatus().isSuccess());
                }
                if (!isTemp) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("isTemp Value:" + isTemp);
                    }
                    for (int j = 0; j < rows.length; j++) {
                        Row r = rows[j];
                        postProcessor(r);
                    }
                }
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("process status Value:" + result.getStatus().isSuccess());
                }
                auditProcess(rows[0].getTableName(), rows.length);
                //mergeTable process
                for (int i = 0; i < mergetables.length; i++) {
                    MergeTableInfo tempinfo = mergetables[i];
                    TableInfo tempTableInfo = m_databaseInfo.find(tempinfo.getMergeTableName());
                    if (tempTableInfo == null) {
                        Message Message = new Message("导出源表 {0} 配置错误.没有醪置辅助表在同步表中", tempinfo.getMergeTableName());
                        m_logger.warn(Message.toString(), new Exception());
                        continue;
                    }
                    //
                    boolean processstatus = false;
                    while (!processstatus) {
                        try {
                            testStatus();
                            exportMergeTable(findTableInfo(tempinfo.getTableName()), rows, tempTableInfo);
                            processstatus = true;
                        } catch (Ex ex) {
                            sleepTime();
                        }

                    }

                }
            }

            if (!result.getStatus().isSuccess()) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("process status Value:" + result.getStatus().isSuccess());
                }
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ie) {
                    //okay
                }
                Message Message = new Message("目标端处理出错.");
                throw new Ex().set(EDbChange.E_TargetProcessError, Message);
            }

        } else {
            Message Message = new Message("数据为空,不传输.");

            throw new Ex().set(EDbChange.E_DataIsNullErrorr, Message);
        }


        //

    }

    /**
     * 是否为合并辅助表。
     *
     * @param tableName
     * @return
     */
    public boolean isMergeTable
    (String
             tableName) {
        boolean result = false;
        if (mergeTableInfo.isEmpty()) {
            return false;
        }
        String mergeTable = (String) mergeTableInfo.get(tableName);
        if (mergeTable != null) {
            result = true;
        }
        return result;
    }

    private void exportMergeTable
            (TableInfo
                     tableinfo, Row[] rows, TableInfo
                    tempTableInfo) throws Ex {
        try {
            processMaxRecords(getMergeTableSql(tableinfo, rows, tempTableInfo), tempTableInfo, rows.length);
        } catch (Ex Ex) {
            Message Message = new Message("处理辅助表出错，等待下个周期处理.");
            m_logger.warn(Message.toString(), Ex);
            m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
            m_log.warn(Message.toString(), Ex);
            sleepTime();
            throw Ex;
        }
    }

    private String getMergeTableSql(TableInfo tableinfo, Row[] rows, TableInfo tempTableInfo) throws Ex {
        //
        //dataConstructor
        Column[] clobColumns = tempTableInfo.getClobColumns();
        Column[] blobColumns = tempTableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasLob = hasBlob || hasClob;
        boolean hasBothBlobAndClob = hasBlob && hasClob;
        Column[] basicColumns = tempTableInfo.getBasicColumns();
        String strBasicQuery = null;
        String strWhere = " where ";
        MergeTableInfo mergetable = tableinfo.getMergeTableByName(tempTableInfo.getName());
        String[] tableFields = mergetable.getAllFieldKey();
        String[] mergeFields = mergetable.getAllFieldValue();
        for (int i = 0; i < tableFields.length; i++) {
            strWhere = strWhere + mergetable.getFieldNameByName(tableFields[i]);
            strWhere = strWhere + getMergeTableWhereValue(tableinfo.find(tableFields[i]), rows);
            if (i < tableFields.length - 1) {
                strWhere = strWhere + "and ";
            }
        }

        if (hasLob) {
            Column[] columns = null;
            Column[] lobColumn = null;

            if (hasBothBlobAndClob) {
                lobColumn = addTwoTypeColumns(blobColumns, clobColumns);
            } else {
                if (hasBlob) {
                    lobColumn = blobColumns;
                } else if (hasClob) {
                    lobColumn = clobColumns;
                }
            }
            columns = addTwoTypeColumns(basicColumns, lobColumn);
            strBasicQuery = getTableQueryString(tempTableInfo.getName(), columns);
            strBasicQuery += strWhere;
        } else {
            strBasicQuery = getTableQueryString(tempTableInfo.getName(), basicColumns);
            strBasicQuery += strWhere;

        }

        return strBasicQuery;
    }

    private String getMergeTableWhereValue(Column column, Row[] rows) {
        String result = "";
        HashMap valueset = new HashMap();
        for (int i = 0; i < rows.length; i++) {
            Column[] columns = rows[i].getColumnArray();
            for (int j = 0; j < columns.length; j++) {
                if (columns[j].getName().equalsIgnoreCase(column.getName())) {
                    valueset.put(columns[j], columns[j]);
                    break;
                }
            }
        }
        if (valueset.size() == 1) {
            Column[] columns = (Column[]) valueset.values().toArray(new Column[0]);
            int jdbctype = columns[0].getJdbcType();

            if (jdbctype == Types.CHAR || jdbctype == Types.VARCHAR) {
                result = result + "='" + columns[0].getValue().getValueString() + "'";
            }
            if (jdbctype == Types.BIGINT || jdbctype == Types.INTEGER || jdbctype == Types.FLOAT || jdbctype == Types.DOUBLE || jdbctype == Types.DOUBLE || jdbctype == Types.NUMERIC) {
                result = result + "=" + columns[0].getValue().getValueString();
            }
            if (result.equals("")) {

            }
            return result;
        }
        result = " in (";
        Column[] columns = (Column[]) valueset.values().toArray(new Column[0]);
        for (int i = 0; i < columns.length; i++) {
            int jdbctype = columns[i].getJdbcType();

            if (jdbctype == Types.CHAR || jdbctype == Types.VARCHAR) {
                result = result + "'" + columns[i].getValue().getValueString() + "'";
            }
            if (jdbctype == Types.BIGINT || jdbctype == Types.INTEGER || jdbctype == Types.FLOAT || jdbctype == Types.DOUBLE || jdbctype == Types.DOUBLE || jdbctype == Types.NUMERIC) {
                result = result + columns[i].getValue().getValueString();
            }
            if (i < columns.length - 1) {
                result += ",";
            }
        }
        result += ") ";
        return result;
    }

    private void changeForOneTableTwowayForAll
            () {
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return;
        }
        try {
            if (changeForOneTableTwoway())
                return;
        } catch (Ex Ex) {
            Message Message = new Message("However, it will be exported in a future cycle after the problem is corrected.");
            m_logger.warn(Message.toString(), Ex);
            m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
            m_log.warn(Message.toString(), Ex);
            sleepTime();

        }
    }

    public boolean changeForTable
            (
                    long time) {
        boolean result = false;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Sync table status is:" + m_databaseInfo.isTwoway());
        }
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return false;
        }
        if (m_databaseInfo.isTwowayChange()) {
            result = false;
        } else {
            m_databaseInfo.setTwowayChange();
            exportNewData(time);
            resetChangeTime();
            result = true;
        }

        return result;
    }

    private int exportNewData
            (
                    long time) {

        int size = 0;
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return 0;
        }
        int tableSize = m_tableInfos.length;
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            String tableName = m_tableInfos[i].getName();
            try {
                do {
                    size = exportForTempTable(time, tableInfo, 0);
                } while (size > 0);
            } catch (Ex Ex) {
                Message Message = new Message("导出源表 {0} 数据出错.", tableName);
                m_logger.warn(Message.toString(), Ex);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), Ex);
                m_databaseInfo.resetTwowayChange();
            }
        }

        m_databaseInfo.resetTwowayChange();
        return size;
    }


    private boolean changeForOneTableTwoway
            () throws Ex {
        boolean result = false;

        if (m_databaseInfo.isTwowayChange()) {
            result = false;
            return result;
        }
        testStatus();
        DataAttributes data = new DataAttributes();
        m_syncTime = getDbSysDate();
        data.putValue(DbChangeSource.Str_ChangeTime, String.valueOf(m_syncTime));
        data.putValue(DbChangeSource.Str_syncDatabase, m_dbName);
        DataAttributes res;
        try {
            res = m_changeMain.control(m_changeType, DbChangeSource.Str_Change, data);
            if (res.getStatus().isSuccess()) {
                String syncTime = res.getValue(DbChangeSource.Str_ChangeTime);
                long temTime = 0;
                if (syncTime != null) {
                    try {
                        temTime = Long.parseLong(syncTime);
                    } catch (NumberFormatException e) {
                        throw new Ex().set(E.E_FormatError, new Message("Change Time format error time is {0}", syncTime));
                    }
                }
                m_syncTimeTolerance = m_syncTime - temTime;
                result = changeForTable(m_syncTime);
            }
        } catch (Ex Ex) {
            throw new Ex().set(Ex.getErrcode(), Ex.getCause(), new Message("Send Change time command error."));
        }

        return result;
    }

    /**
     * 时间标记同步 ，同时加上主键。
     *
     * @return
     */
    public void timeSync() {
        //加载同步时间
        Properties timesyncProp = new Properties();
        String ichangehome = System.getProperty("ichange.home");
        String propname = ichangehome + File.separator + "data" + File.separator + m_changeType.getType() + "timesync.properties";
        File propfile = new File(propname);
        boolean isInittime = false;
        try {

            if (!propfile.exists()) {
                isInittime = true;
                if (!propfile.getParentFile().exists()) {
                    propfile.getParentFile().mkdirs();
                }
                propfile.createNewFile();
            } else {
                if (propfile.length() == 0) {
                    isInittime = true;
                }
            }
            timesyncProp.load(new FileInputStream(propfile));

        } catch (IOException e) {
            m_logger.warn(m_changeType.getType() + " type dbchange timesync operator init error!", e);
        }

        Timestamp begin = null;
        Timestamp end = null;

        String pkWhere = "";

        int tableSize = m_tableInfos.length;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("开始导出源表数据,Operator is TimeSync 源表个数为: " + tableSize);
        }
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return;
        }
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];

            String tableName = m_tableInfos[i].getName();


            if (isMergeTable(tableName)) {
                continue;
            }

            try {
                Timestamp initTime = new Timestamp(0);
                if (isInittime) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.info("应用为：" + appType + "取到的时间标记为 isInittime:" + isInittime);
                    }
                    if (tableInfo.getTimeSyncTimeField() == null) {
                        m_logger.warn("应用为：" + appType + "取到的时间标记主键为：0,请检查配置.");
                    }

                    initTime = getTimeSyncInitDate(tableInfo.getTimeSyncTimeField().getName(), m_tableInfos[i].getName());
                    if (initTime == null) {
                        m_logger.warn("应用为：" + appType + "取到的时间标记为：0,请检查数据.");
                        return;
                    }
                    if (m_logger.isDebugEnabled()) {
                        m_logger.info("应用为：" + appType + "取到的时间标记为 Inittime:" + initTime);
                    }
                } else {
                    if (timesyncProp.getProperty("begintime") == null) {
                        initTime = getTimeSyncInitDate(tableInfo.getTimeSyncTimeField().getName(), m_tableInfos[i].getName());
                        if (initTime == null) {
                            m_logger.warn("应用为：" + appType + "取到的时间标记为：0,请检查数据.");
                            return;
                        }
                    } else {
                        initTime = Timestamp.valueOf(timesyncProp.getProperty("begintime"));
                    }

                }

                String begins = timesyncProp.getProperty("begintime", initTime.toString());
                if (m_logger.isDebugEnabled())
                    m_logger.info("开始时间标记：" + begins);
                end = new Timestamp(initTime.getTime() + (tableInfo.getInterval() + 1) * 60 * 1000);
                String ends = timesyncProp.getProperty("endtime", end.toString());
                begin = Timestamp.valueOf(begins);
                if (m_logger.isDebugEnabled())
                    m_logger.info("结束时间标记：" + ends);
                end = Timestamp.valueOf(ends);

                String initEndTime = timesyncProp.getProperty("InitEndTime");
                if (initEndTime == null) {
                    initEndTime = begins;
                }
                long initEnd = Timestamp.valueOf(initEndTime).getTime();

                if (begin.getTime() == end.getTime()) {
                    if (initEnd >= end.getTime()) {
                        end = new Timestamp((end.getTime() + tableInfo.getInterval() + 1) * 60 * 1000);
                    } else {
                        updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, 0);
                    }
                }


                pkWhere = timesyncProp.getProperty("pkwere", "");
                int rownum = Integer.parseInt(timesyncProp.getProperty("rownum", "0"));
                exportWholeTableTimeSync(m_tableInfos[i].getName(), begin, end, pkWhere, rownum);
                timesyncProp.clear();
            } catch (Ex Ex) {
                Message Message = new Message("导出源表 {0} Operator is TimeSync 数据出错.", tableName);
                m_logger.warn(Message.toString(), Ex);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), Ex);
            } catch (NullPointerException e) {
                Message Message = new Message("导出源表 {0} Operator is TimeSync 数据出错.", tableName);
                m_logger.warn(Message.toString(), e);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), e);

            }
            //}
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("完成源表数据的导出.");
        }
        return;

    }

    public void updateTimeSyncPksetAndTime(Timestamp begintime, Timestamp endtime, Row row, TableInfo tableInfo, boolean isUpdate, int rownum) {
        Properties timesyncProp = new Properties();
        String ichangehome = System.getProperty("ichange.home");
        String propname = ichangehome + File.separator + "data" + File.separator + m_changeType.getType() + "timesync.properties";
        File propfile = new File(propname);
        try {
            if (!propfile.exists()) {
                if (!propfile.getParentFile().exists()) {
                    propfile.getParentFile().mkdirs();
                }
                propfile.createNewFile();
            }
            timesyncProp.load(new FileInputStream(propfile));

        } catch (IOException e) {
            m_logger.warn(m_changeType.getType() + " type dbchange timesync operator init error!", e);
        }
        Timestamp tempEndtime = new Timestamp(0);

        Timestamp endTime = new Timestamp(0);
        if (isUpdate) {
            long tbegin = Timestamp.valueOf(timesyncProp.getProperty("begintime", begintime.toString())).getTime();
            if (tbegin <= endtime.getTime()) {
                timesyncProp.put("begintime", endtime.toString());
                tempEndtime = new Timestamp(endtime.getTime() + (tableInfo.getInterval() + 1) * 60 * 1000);
            } else {
                timesyncProp.put("begintime", new Timestamp(tbegin).toString());
                tempEndtime = new Timestamp(tbegin + (tableInfo.getInterval() + 1) * 60 * 1000);
            }
        } else {
            timesyncProp.put("begintime", begintime.toString());
        }

        String ends = timesyncProp.getProperty("InitEndTime", new Timestamp(0).toString());

        try {
            endTime = Timestamp.valueOf(ends);
            if (endTime.getTime() == 0) {
                endTime = getTimeSyncEndDate(tableInfo.getTimeSyncTimeField().getName(), tableInfo.getName());
            }
            if (tempEndtime.getTime() > endTime.getTime()) {
                Timestamp endTime2 = getTimeSyncEndDate(tableInfo.getTimeSyncTimeField().getName(), tableInfo.getName());
                if (endTime2.getTime() > 0) {
                    endTime.setTime(endTime2.getTime());
                }
                tempEndtime = new Timestamp(endTime.getTime() + 1000);
            }
            if (isUpdate)
                timesyncProp.put("endtime", tempEndtime.toString());
            else
                timesyncProp.put("endtime", endtime.toString());

            timesyncProp.put("InitEndTime", endTime.toString());
            if (row != null)
                timesyncProp.put("pkwere", new PkSet(PkSet.getPks(row)).getPkString());
            else {
                if (isUpdate)
                    timesyncProp.put("pkwere", "");
                else {
                    timesyncProp.put("pkwere", timesyncProp.getProperty("pkwhere", ""));
                }
            }
            timesyncProp.put("rownum", "" + rownum);
            FileOutputStream outfile = new FileOutputStream(propfile);
            timesyncProp.store(outfile, null);
            outfile.flush();
            outfile.close();
            timesyncProp.clear();
        } catch (Ex Ex) {
            Message Message = new Message("导出源表 {0} Operator is TimeSync update  数据出错.", tableInfo.getName());
            m_logger.warn(Message.toString(), Ex);
            m_log.setTableName(tableInfo.getName());
            m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
            m_log.warn(Message.toString(), Ex);
        } catch (FileNotFoundException e) {
            Message Message = new Message("导出源表 {0} Operator is TimeSync update 数据出错.", tableInfo.getName());
            m_logger.warn(Message.toString(), e);
            m_log.setTableName(tableInfo.getName());
            m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
            m_log.warn(Message.toString(), e);
        } catch (IOException e) {
            Message Message = new Message("导出源表 {0} Operator is TimeSync update 数据出错.", tableInfo.getName());
            m_logger.warn(Message.toString(), e);
            m_log.setTableName(tableInfo.getName());
            m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
            m_log.warn(Message.toString(), e);
        }
    }

    public long getChangeTime
            () {
        return m_syncTime;
    }

    public long getChangeTimeTolerance
            () {
        return m_syncTimeTolerance;
    }

    public void setChangeTime
            () throws Ex {
        m_syncTime = getDbSysDate();
    }

    public void setChangeTime
            (
                    long time) throws Ex {
        m_syncTime = time;
    }

    public void resetChangeTime
            () {
        m_syncTime = 0;
    }

    public boolean isChange
            () {
        return m_databaseInfo.isTwowayChange();
    }

    public boolean isTwoWayChange
            () {
        return m_databaseInfo.isTwoway();
    }


    // 平台相关变量
    private IChangeType m_changeType = null;
    private ISourcePlugin m_sourcePlugin = null;
    private SourceObjectCache m_objectCache = null;

    // 数据库相关变量
    private DatabaseInfo m_databaseInfo = null;
    private TableInfo[] m_tableInfos = new TableInfo[0];

    private boolean m_running = false;
    private boolean m_initialized = false;

    private boolean m_bTrusted = false;
    private long m_syncTime = 0;
    private long m_syncTimeTolerance = 0;
    private Connection m_conn = null;
    private String appType = null;

}
