package com.inetec.ichange.plugin.dbchange.target;

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import com.inetec.common.logs.LogHelper;
import com.inetec.common.security.DesEncrypterAsPassword;
import com.inetec.ichange.plugin.dbchange.utils.StringUtil;
import com.inetec.ichange.plugin.dbchange.utils.XmlSaxParser;
import com.inetec.ichange.plugin.dbchange.utils.RBufferedInputStream;
import com.inetec.ichange.plugin.dbchange.target.info.TableMap;
import com.inetec.ichange.plugin.dbchange.target.info.ColumnMap;
import com.inetec.ichange.plugin.dbchange.target.info.FilterResult;
import com.inetec.ichange.plugin.dbchange.source.twoway.SourceObjectCache;
import com.inetec.ichange.plugin.dbchange.source.twoway.SourceObject;
import com.inetec.ichange.plugin.dbchange.datautils.db.*;
import com.inetec.ichange.plugin.dbchange.DbChangeTarget;
import com.inetec.ichange.plugin.dbchange.DbChangeSource;
import com.inetec.ichange.plugin.dbchange.DbInit;
import com.inetec.ichange.plugin.dbchange.exception.EXSql;
import com.inetec.ichange.plugin.dbchange.datautils.db.pk.PkSet;
import com.inetec.ichange.plugin.dbchange.datautils.DefaultData;
import com.inetec.ichange.plugin.dbchange.datautils.ByteLargeObjectData;
import com.inetec.ichange.plugin.dbchange.datautils.CharLargeObjectData;
import com.inetec.ichange.plugin.dbchange.datautils.DataInformation;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.IDbOp;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.OracleIDbOp;
import com.inetec.ichange.plugin.dbchange.target.reg.Parser;
import com.inetec.ichange.plugin.dbchange.target.reg.Expression;
import com.inetec.ichange.plugin.dbchange.source.info.TableInfo;
import com.inetec.ichange.plugin.dbchange.source.info.JdbcInfo;
import com.inetec.ichange.plugin.dbchange.source.SourceDbOperation;
import com.inetec.ichange.api.*;

import org.apache.log4j.Logger;
import org.apache.commons.codec.binary.Base64;

import java.util.*;
import java.sql.*;
import java.io.*;


public class TargetDbOperation extends DbInit {

    public final static Logger m_logger = Logger.getLogger(TargetDbOperation.class);
    public final static String Str_Sql_DeleteFromTable = "Sql_DeleteFromTable";
    public final static String Str_Sql_InsertIntoTable = "Sql_InsertIntoTable";
    public final static String Str_Sql_SelectFromTable = "Sql_SelectFromTable";
    public final static String Str_Sql_UpdateTable = "Sql_UpdateTable";


    // 平台相关变量
    private IChangeType m_changeType = null;
    private ITargetPlugin m_outputAdapter = null;
    private ISourcePlugin m_inputAdapter = null;

    // 数据库相关变量
    private String m_schemaName = "";
    private TableMap[] m_tableMaps;

    // 其他变量
    private SourceObjectCache m_objectCache = null;
    private DataAttributes m_dataProps = new DataAttributes();
    private JdbcInfo jdbcInfo;

    public TargetDbOperation(IChangeMain dc, IChangeType type, ITargetPlugin out) {
        m_changeMain = dc;
        m_changeType = type;
        m_outputAdapter = out;
        m_log = dc.createLogHelper();
        m_log.setAppName(type.getType());
        m_log.setAppType(DbChangeTarget.Str_DbChangeType);
        m_log.setSouORDes(LogHelper.Str_souORDes_Dest);
        m_inputAdapter = ((DbChangeTarget) m_outputAdapter).getISourcePlugin();
        m_objectCache = ((DbChangeSource) m_inputAdapter).getObjectCache();

    }

    public void setTableMaps(TableMap[] tableMaps) {
        m_tableMaps = tableMaps;
    }


    public void setJdbcInfo(JdbcInfo jdbcInfo) throws Ex {
        this.jdbcInfo = jdbcInfo;
        m_schemaName = jdbcInfo.getDbOwner();
        m_dbName = jdbcInfo.getJdbcName();
        m_log.setDbName(jdbcInfo.getJdbcName());
        m_dbDesc = jdbcInfo.getDesc();
        m_driverClass = jdbcInfo.getDriverClass();
        m_log.setIp(jdbcInfo.getDbHost());
        initDbSource();

    }

    public void setNativeCharSet(String nativeCharSet) {
        m_nativeCharSet = nativeCharSet;
    }

    public IDbOp getCurrentDbms() {
        return m_I_dbOp;
    }

    public String getSchemaName() throws Ex {
        return m_schemaName;
    }

    public String getDbName() throws Ex {
        return m_dbName;
    }

    public void addDataProps(DataAttributes dataProps) {
        m_dataProps.putAll(dataProps);
    }

    public FilterResult processBasicData(DefaultData defaultData) throws Ex {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to process basic data.");
        }

        boolean passed = true;

        String sourceSchema = defaultData.getSchemaName();
        String sourceTable = defaultData.getTableName();

        String syncTime = defaultData.getHeadValue(DbChangeSource.Str_ChangeTime);


        InputStream basicContentIs = defaultData.getContentStream();
        InputStream basicContentIs2 = new RBufferedInputStream(basicContentIs, (int) defaultData.getContentLength());
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to parse basic xml document. the length is " + defaultData.getContentLength());
        }
        String pks = "";
        String operator = "";
        XmlSaxParser parser = new XmlSaxParser();
        Rows basic = parser.parse(basicContentIs2);
        Row[] rows = basic.getRowArray();
        if (syncTime != null) {
            int size = 0;
            for (int i = 0; i < rows.length; i++) {
                long tempTime = 0;
                try {
                    tempTime = new Long(syncTime).longValue() - new Long(rows[i].getOptime()).longValue();
                } catch (NumberFormatException e) {
                    throw new Ex().set(E.E_FormatError, new Message("Change Time or Process Time format error."));
                }
                DbChangeSource dbChangeSource = (DbChangeSource) m_inputAdapter;
                TableMap tablemap = findTableMap(sourceSchema, sourceTable);
                SourceDbOperation sourceOperation = dbChangeSource.findOperationBySource(tablemap.getTargetDb());

                if (sourceOperation != null && sourceOperation.isTwoWayChange()) {
                    if (rows.length > 0) {
                        boolean isTodoNew = sourceOperation.isTempRowNew(tablemap.getTargetDb(), tablemap.getTargetTable(), rows[i].getPkColumnArray(), tempTime);
                        if (isTodoNew) {
                            m_logger.info("This record is invalid for database/table :" + tablemap.getTargetDb() + "/" + tablemap.getTargetTable());
                            if (m_dbSource != null) {
                                m_log.setDest_ip(m_dbSource.getDbHost());
                                m_log.setUserName(m_dbSource.getDbUser());
                            }
                            m_log.setOperate("insert");
                            m_log.setDbName(tablemap.getTargetDb());
                            m_log.setTableName(tablemap.getTargetTable());
                            m_log.setStatusCode(EStatus.E_InvalidData.getCode() + "");
                            m_log.setFilename(defaultData.getFilename());
                            m_log.setPk_id(PkSet.getPks(rows[i]));
                            m_log.info("This record is invalid for database/table :" + tablemap.getTargetDb() + "/" + tablemap.getTargetTable());
                            size++;
                        }
                    }
                }
            }
            if (size > 0) {
                return new FilterResult(size);
            }
        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Row size:" + rows.length);
        }
        FilterResult fr = new FilterResult(rows.length);
        TableMap tableMap = findTableMap(sourceSchema, sourceTable);
        tableMap.setTotalnumber(rows.length);
        Connection conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Connection connection = null;
        try {
            if(connection == null&&isOracleDB()) {
                String password = null;
                try {
                    Class.forName(jdbcInfo.getDriverClass());
                    DesEncrypterAsPassword deap = new DesEncrypterAsPassword(StringUtil.S_PWD_ENCRYPT_CODE);
                    password = new String(deap.decrypt(jdbcInfo.getDbPassword().getBytes()));
                } catch (Exception e) {
                }
                connection = DriverManager.getConnection(
                        jdbcInfo.getDriverUrl(),
                        jdbcInfo.getDbUser(), password);
            }
            DbopUtil.setNotAutoCommit(conn);  //非自动提交
        } catch (SQLException e) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        for (int i = 0; i < rows.length; i++) {
            try {

                String condition = tableMap.getCondition();
                if (!condition.equals("")) {
                    Parser conditionParser = new Parser(condition);
                    Expression express = conditionParser.getExpression();
                    express.setRowData(rows[i]);
                    passed = express.test();
                }
            } catch (SQLException sql) {
                // testBadConnection(conn, sqlEEx);      // No SQL operation
                m_changeMain.setStatus(m_changeType, EStatus.E_Data_FormatError, "", false);
                throw new Ex().set(E.E_FormatError, new Message(sql.getMessage()));
            }

            fr.setIth(i, passed);

            if (passed) {
                boolean bDeleteEnable = tableMap.isDeleteEnable();
                boolean bOnlyInsert = tableMap.isOnlyInsert();
                // String targetSchema = tableMap.getTargetDb();
                String targetTable = tableMap.getTargetTable();
                if (targetTable == null || targetTable.equals("")) {
                    targetTable = sourceTable;
                }
                boolean islob = false;
                // set value into tableMap

                Column[] srcColumns = rows[i].getColumnArray();
                for (int j = 0; j < srcColumns.length; j++) {
                    Column srcColumn = srcColumns[j];
                    Column targetColumn = tableMap.findColumnBySourceField(srcColumn.getName());

                    if (targetColumn == null) {
                        m_logger.warn(srcColumn.getName() + " Column not found destField.");
//                        m_log.setDbName(tableMap.getTargetDb());
//                        m_log.setTableName(tableMap.getTargetTable());
//                        m_log.setStatusCode(EStatus.E_InvalidTargetField.getCode() + "");
//                        m_log.setFilename(defaultData.getFilename());
//                        m_log.setPk_id(PkSet.getPks(rows[i]));
//                        m_log.warn(srcColumn.getName() + " Column not found destField.");
                        continue;
                    }
                    if (!DbopUtil.verifierColumn(srcColumn, targetColumn)) {
                        String log = tableMap.getTargetDb() + "数据源中" + tableMap.getTargetTable() + "数据表的" + "源字段(" + srcColumn.getName() + ")类型(" + srcColumn.getJdbcType() + ")与目标字段("
                                + targetColumn.getName() + ")类型(" + targetColumn.getJdbcType() + ")不匹配";
                        m_logger.warn(log);
                        if (m_dbSource != null) {
                            m_log.setDest_ip(m_dbSource.getDbHost());
                            m_log.setUserName(m_dbSource.getDbUser());
                        }
                        m_log.setOperate("savedb");
                        m_log.setDbName(tableMap.getTargetDb());
                        m_log.setTableName(tableMap.getTargetTable());
                        m_log.setStatusCode(EStatus.E_InvalidTargetField.getCode() + "");
                        m_log.setFilename(defaultData.getFilename());
                        m_log.setPk_id(PkSet.getPks(rows[i]));
                        m_log.warn(log);
                        continue;
                    }


                    if (targetColumn.isPk()) {
                        if (!pks.equalsIgnoreCase("")) {
                            pks += ":";
                        }
                        Value value = srcColumn.getValue();
                        String valueString = value == null ? "" : value.getValueString();
                        //valueString = new String(Base64.encodeBase64(valueString.getBytes()));
                        if (!pks.equalsIgnoreCase("")) {
                            pks = pks + ";" + targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                        } else {
                            pks = targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                        }
                        if (m_logger.isDebugEnabled())
                            m_logger.debug("the value type is " + srcColumn.getDbType());
                    }
                    // todo : 目标字段设置为NULLABLE
                    if (srcColumn.isNull()) {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("DbChangeSource column values is null.");
                        }
                        targetColumn.setValue(null);
                    } else {
                        targetColumn.setValue(srcColumn.getValue());
                    }
                    islob = islob || targetColumn.isLobType();

                }

                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Process the data.");
                }

                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Start to get connection.");
                }
                // process the data
//                Connection conn = null;
                if (!isOkay()) {
                    Message Message = new Message("数据库异常 {0}.", m_dbName);
                    m_logger.error(Message.toString());
                    if (m_dbSource != null) {
                        m_log.setDest_ip(m_dbSource.getDbHost());
                        m_log.setUserName(m_dbSource.getDbUser());
                    }
                    m_log.setOperate("savedb");
                    m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                    m_log.setFilename(defaultData.getFilename());
                    m_log.setPk_id(PkSet.getPks(rows[i]));
                    m_log.setDbName(tableMap.getTargetDb());
                    m_log.setTableName(tableMap.getTargetTable());
                    m_log.error(Message.toString());
                    sleepTime();
                    testDb();
                    throw new Ex().set(E.E_DatabaseConnectionError);
                }
//                conn = getConnection();
//                if (conn == null) {
//                    throw new Ex().set(E.E_DatabaseConnectionError);
//                }
                try {
                    if (bOnlyInsert) {
                        DbopUtil.setNotAutoCommit(conn);
                        if (isMssqlDB()){
                            insertFullData(tableMap, conn,null);
                        }else if(isOracleDB()){
                            insertData(tableMap, conn,connection);
                        }else {
                            insertData(tableMap, conn,null);

                        }

                        //insucepks[i] = pks;
                        operator = Operator.Str_OPERATOR__INSERT;
                        //uditProces(tableMap.getSourceDb(), tableMap.getSourceTable(), tableMap.getTargetDb(), tableMap.getTargetTable(), pks, Operator.Str_OPERATOR__INSERT, 1);
                    } else {
                        if (rows[i].isDeleteAction()) {
                            if (bDeleteEnable) {
                                toCache(tableMap, Operator.OPERATOR__DELETE);
                                deleteRecord(tableMap, conn);
                                operator = Operator.Str_OPERATOR__DELETE;
                                //desucepks[i] = pks;

                            }
                        } else {
                            if (isRowExists(tableMap)) {
                                toCache(tableMap, Operator.OPERATOR__UPDATE);
                                if(isOracleDB()){
                                    updateData(tableMap, conn, connection);
                                }else {
                                    updateData(tableMap, conn,null);
                                }
                                operator = Operator.Str_OPERATOR__UPDATE;

                                //upsucepks[i] = pks;
                            } else {

                                toCache(tableMap, Operator.OPERATOR__INSERT);
                                if (isMssqlDB()) {
                                    insertFullData(tableMap, conn,null);
                                }else{
                                    if(isOracleDB()){
                                        insertData(tableMap, conn,connection);
                                    } else {
                                        insertData(tableMap, conn,null);
                                    }
                                }
                                //insucepks[i] = pks;
                                operator = Operator.Str_OPERATOR__INSERT;
                            }
                        }
                    }
//                    commit(conn);
                } catch (SQLException e) {
                    EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to process basic data for  database/table {0}/{1} .", tableMap.getTargetDb(), tableMap.getTargetTable()));
                    EStatus status = EStatus.E_DATABASEERROR;
                    m_logger.error(new Message("Failed to process basic data for  database/table {0}/{1} .", tableMap.getTargetDb(), tableMap.getTargetTable()).toString());
                    m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                    if (m_dbSource != null) {
                        m_log.setDest_ip(m_dbSource.getDbHost());
                        m_log.setUserName(m_dbSource.getDbUser());
                    }
                    m_log.setOperate("savedb");
                    m_log.setFilename(defaultData.getFilename());
                    m_log.setPk_id(PkSet.getPks(rows[i]));
                    m_log.setDbName(tableMap.getTargetDb());
                    m_log.setTableName(tableMap.getTargetTable());
                    m_log.error(new Message("Failed to process basic data for  database/table {0}/{1} .", tableMap.getTargetDb(), tableMap.getTargetTable()).toString());
                    status.setDbInfo(m_dbName, m_dbDesc);
                    m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
                    testBadConnection(conn, Exsql);
                    throw Exsql;
                } finally {
                    //DbopUtil.closeConnection(conn);
//                    returnConnection(conn);
                }
            } // else {}
        }
        commit(conn);
        try{
            conn.commit();
            DbopUtil.setAutoCommit(conn);
//                isRollback = true;
        } catch (Exception e) {
            try {
                conn.rollback();
            } catch (SQLException sqlE) {

            }
//                isRollback = false;
        }
        try {
            if(connection != null){
                connection.close();
            }
        } catch (SQLException e) {
        }
        returnConnection(conn);
        /**if (fr.getResultSize() > 0) {
         Logger auditLogger = Logger.getLogger("ichange.audit.database");
         StringBuffer buff = new StringBuffer();
         buff.append("Data Target Audit Info/(ChangeType,TargetDb/Table)/");
         buff.append(m_changeType.getType());
         buff.append("/");
         buff.append(m_dbName);
         buff.append("/");
         buff.append(tableMap.getTargetTable());
         buff.append("/");
         buff.append(String.valueOf(fr.getResultSize()) + " record.");
         EStatus status = EStatus.E_OK;
         if (m_dbSource != null) {
         m_log.setDest_ip(m_dbSource.getDbHost());
         m_log.setUserName(m_dbSource.getDbUser());
         }
         status.setDbInfo(m_dbName, m_dbDesc);
         m_changeMain.setStatus(m_changeType, EStatus.E_OK, null, false);
         auditLogger.info(buff.toString());
         m_log.setDbName(m_dbName);
         m_log.setTableName(tableMap.getTargetTable());
         m_log.setStatusCode(EStatus.E_OK.getCode() + "");
         m_log.setRecordCount(fr.getResultSize() + "");
         m_log.info(buff.toString());
         m_log.setStatusCode(null);


         }**/
        if (fr.getResultSize() > 0) {
            auditProces(tableMap.getSourceDb(), tableMap.getSourceTable(), tableMap.getTargetDb(), tableMap.getTargetTable(), pks, operator, fr.getResultSize());
        }
        return fr;
    }

    public FilterResult processSequenceData(DefaultData defaultData) throws Ex {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to process basic data.");
        }
        boolean passed = true;
        String sourceSchema = defaultData.getSchemaName();
        String sourceTable = defaultData.getTableName();
        InputStream basicContentIs = defaultData.getContentStream();
        InputStream basicContentIs2 = new RBufferedInputStream(basicContentIs, (int) defaultData.getContentLength());
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to parse basic xml document. the length is " + defaultData.getContentLength());
        }
        String pks = "";
        String operator = "";
        XmlSaxParser parser = new XmlSaxParser();
        Rows basic = parser.parse(basicContentIs2);
        Row[] rows = basic.getRowArray();
        FilterResult fr = new FilterResult(rows.length);
        TableMap tableMap = findTableMap(sourceSchema, sourceTable);
        tableMap.setTotalnumber(rows.length);
        Connection conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }


        for (int i = 0; i < rows.length; i++) {
            try {
                String condition = tableMap.getCondition();
                if (!condition.equals("")) {
                    Parser conditionParser = new Parser(condition);
                    Expression express = conditionParser.getExpression();
                    express.setRowData(rows[i]);
                    passed = express.test();
                }
            } catch (SQLException sql) {
                // testBadConnection(conn, sqlEEx);      // No SQL operation
                m_changeMain.setStatus(m_changeType, EStatus.E_Data_FormatError, "", false);
                throw new Ex().set(E.E_FormatError, new Message(sql.getMessage()));
            }

            fr.setIth(i, passed);

            if (passed) {

                String targetTable = tableMap.getTargetTable();
                if (targetTable == null || targetTable.equals("")) {
                    targetTable = sourceTable;
                }

                Column[] srcColumns = rows[i].getColumnArray();
                Column srcColumn = srcColumns[0];
                int srcId = Integer.valueOf(srcColumn.getValue().getValueString());
                String sqlStr = "select * from all_sequences where SEQUENCE_OWNER = '"+m_schemaName.toUpperCase()
                +"' and SEQUENCE_NAME = '"+targetTable.toUpperCase()+"'";
                PreparedStatement stmt = null;
                ResultSet rs = null;
                try {
                    stmt = (PreparedStatement)conn.prepareStatement(sqlStr);
                    rs = stmt.executeQuery();
                    rs.next();
                    int increment = rs.getInt("increment_by");
                    int id = rs.getInt("last_number");
                    int cacheSize = rs.getInt("cache_size");
                    String cache = "cache";
                    if(cacheSize == 0) {
                        cache = "nocache";
                    } else {
                        cache = "cache " + cacheSize;
                    }
                    if(id < srcId) {
                        int newIncrement = srcId - id;
                        System.out.println();
                        if(newIncrement != increment){
                            String sql_1 = "alter sequence "+targetTable+" increment by "+newIncrement+" nocache";
                            String sql_2 = "select "+targetTable+".nextval from dual";
                            String sql_3 = "alter sequence "+targetTable+" increment by "+increment+" " + cache;
                            stmt = (PreparedStatement)conn.prepareStatement(sql_1);
                            stmt.execute();
                            stmt = (PreparedStatement)conn.prepareStatement(sql_2);
                            stmt.execute();
                            stmt = (PreparedStatement)conn.prepareStatement(sql_3);
                            stmt.execute();
                        } else {
                            String sql_2 = "select "+targetTable+".nextval from dual";
                            stmt = (PreparedStatement)conn.prepareStatement(sql_2);
                            stmt.execute();
                        }

                    }

                } catch (SQLException e) {
                    EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", targetTable));
                    EStatus status = EStatus.E_DATABASECONNECTIONERROR;
                    status.setDbInfo(m_dbName, m_dbDesc);
                    m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + targetTable, true);
                    testBadConnection(conn, Exsql);
                    throw Exsql;
                } finally {
                    try {
                        if(rs !=null){
                            rs.close();
                        }
                    } catch (SQLException e) {
                    }
                    DbopUtil.closeStatement(stmt);
                    returnConnection(conn);
                }
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Process the data.");
                }

                if (!isOkay()) {
                    Message Message = new Message("数据库异常 {0}.", m_dbName);
                    m_logger.error(Message.toString());
                    if (m_dbSource != null) {
                        m_log.setDest_ip(m_dbSource.getDbHost());
                        m_log.setUserName(m_dbSource.getDbUser());
                    }
                    m_log.setOperate("savedb");
                    m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                    m_log.setFilename(defaultData.getFilename());
                    m_log.setPk_id(PkSet.getPks(rows[i]));
                    m_log.setDbName(tableMap.getTargetDb());
                    m_log.setTableName(tableMap.getTargetTable());
                    m_log.error(Message.toString());
                    sleepTime();
                    testDb();
                    throw new Ex().set(E.E_DatabaseConnectionError);
                }
            } // else {}
        }
        returnConnection(conn);
        if (fr.getResultSize() > 0) {
            auditProces(tableMap.getSourceDb(), tableMap.getSourceTable(), tableMap.getTargetDb(), tableMap.getTargetTable(), pks, operator, fr.getResultSize());
        }
        return fr;
    }

    public FilterResult processBasicData_old(DefaultData defaultData) throws Ex {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to process basic data.");
        }

        boolean passed = true;

        String sourceSchema = defaultData.getSchemaName();
        String sourceTable = defaultData.getTableName();

        String syncTime = defaultData.getHeadValue(DbChangeSource.Str_ChangeTime);


        InputStream basicContentIs = defaultData.getContentStream();
        InputStream basicContentIs2 = new RBufferedInputStream(basicContentIs, (int) defaultData.getContentLength());
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Start to parse basic xml document. the length is " + defaultData.getContentLength());
        }
        String pks = "";
        String operator = "";
        XmlSaxParser parser = new XmlSaxParser();
        Rows basic = parser.parse(basicContentIs2);
        Row[] rows = basic.getRowArray();
        if (syncTime != null) {
            int size = 0;
            for (int i = 0; i < rows.length; i++) {
                long tempTime = 0;
                try {
                    tempTime = new Long(syncTime).longValue() - new Long(rows[i].getOptime()).longValue();
                } catch (NumberFormatException e) {
                    throw new Ex().set(E.E_FormatError, new Message("Change Time or Process Time format error."));
                }
                DbChangeSource dbChangeSource = (DbChangeSource) m_inputAdapter;
                TableMap tablemap = findTableMap(sourceSchema, sourceTable);
                SourceDbOperation sourceOperation = dbChangeSource.findOperationBySource(tablemap.getTargetDb());

                if (sourceOperation != null && sourceOperation.isTwoWayChange()) {
                    if (rows.length > 0) {
                        boolean isTodoNew = sourceOperation.isTempRowNew(tablemap.getTargetDb(), tablemap.getTargetTable(), rows[i].getPkColumnArray(), tempTime);
                        if (isTodoNew) {
                            m_logger.info("This record is invalid for database/table :" + tablemap.getTargetDb() + "/" + tablemap.getTargetTable());
                            if (m_dbSource != null) {
                                m_log.setDest_ip(m_dbSource.getDbHost());
                                m_log.setUserName(m_dbSource.getDbUser());
                            }
                            m_log.setOperate("insert");
                            m_log.setDbName(tablemap.getTargetDb());
                            m_log.setTableName(tablemap.getTargetTable());
                            m_log.setStatusCode(EStatus.E_InvalidData.getCode() + "");
                            m_log.setFilename(defaultData.getFilename());
                            m_log.setPk_id(PkSet.getPks(rows[i]));
                            m_log.info("This record is invalid for database/table :" + tablemap.getTargetDb() + "/" + tablemap.getTargetTable());
                            size++;
                        }
                    }
                }
            }
            if (size > 0) {
                return new FilterResult(size);
            }
        }
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Row size:" + rows.length);
        }
        FilterResult fr = new FilterResult(rows.length);
        TableMap tableMap = findTableMap(sourceSchema, sourceTable);
        tableMap.setTotalnumber(rows.length);

        for (int i = 0; i < rows.length; i++) {
            try {

                String condition = tableMap.getCondition();
                if (!condition.equals("")) {
                    Parser conditionParser = new Parser(condition);
                    Expression express = conditionParser.getExpression();
                    express.setRowData(rows[i]);
                    passed = express.test();
                }
            } catch (SQLException sql) {
                // testBadConnection(conn, sqlEEx);      // No SQL operation
                m_changeMain.setStatus(m_changeType, EStatus.E_Data_FormatError, "", false);
                throw new Ex().set(E.E_FormatError, new Message(sql.getMessage()));
            }

            fr.setIth(i, passed);

            if (passed) {
                boolean bDeleteEnable = tableMap.isDeleteEnable();
                boolean bOnlyInsert = tableMap.isOnlyInsert();
                // String targetSchema = tableMap.getTargetDb();
                String targetTable = tableMap.getTargetTable();
                if (targetTable == null || targetTable.equals("")) {
                    targetTable = sourceTable;
                }
                boolean islob = false;
                // set value into tableMap

                Column[] srcColumns = rows[i].getColumnArray();
                for (int j = 0; j < srcColumns.length; j++) {
                    Column srcColumn = srcColumns[j];
                    Column targetColumn = tableMap.findColumnBySourceField(srcColumn.getName());

                    if (targetColumn == null) {
                        m_logger.warn(srcColumn.getName() + " Column not found destField.");
//                        m_log.setDbName(tableMap.getTargetDb());
//                        m_log.setTableName(tableMap.getTargetTable());
//                        m_log.setStatusCode(EStatus.E_InvalidTargetField.getCode() + "");
//                        m_log.setFilename(defaultData.getFilename());
//                        m_log.setPk_id(PkSet.getPks(rows[i]));
//                        m_log.warn(srcColumn.getName() + " Column not found destField.");
                        continue;
                    }
                    if (!DbopUtil.verifierColumn(srcColumn, targetColumn)) {
                        String log = tableMap.getTargetDb() + "数据源中" + tableMap.getTargetTable() + "数据表的" + "源字段(" + srcColumn.getName() + ")类型(" + srcColumn.getJdbcType() + ")与目标字段("
                                + targetColumn.getName() + ")类型(" + targetColumn.getJdbcType() + ")不匹配";
                        m_logger.warn(log);
                        if (m_dbSource != null) {
                            m_log.setDest_ip(m_dbSource.getDbHost());
                            m_log.setUserName(m_dbSource.getDbUser());
                        }
                        m_log.setOperate("savedb");
                        m_log.setDbName(tableMap.getTargetDb());
                        m_log.setTableName(tableMap.getTargetTable());
                        m_log.setStatusCode(EStatus.E_InvalidTargetField.getCode() + "");
                        m_log.setFilename(defaultData.getFilename());
                        m_log.setPk_id(PkSet.getPks(rows[i]));
                        m_log.warn(log);
                        continue;
                    }


                    if (targetColumn.isPk()) {
                        if (!pks.equalsIgnoreCase("")) {
                            pks += ":";
                        }
                        Value value = srcColumn.getValue();
                        String valueString = value == null ? "" : value.getValueString();
                        //valueString = new String(Base64.encodeBase64(valueString.getBytes()));
                        if (!pks.equalsIgnoreCase("")) {
                            pks = pks + ";" + targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                        } else {
                            pks = targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                        }
                        if (m_logger.isDebugEnabled())
                            m_logger.debug("the value type is " + srcColumn.getDbType());
                    }
                    // todo : 目标字段设置为NULLABLE
                    if (srcColumn.isNull()) {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("DbChangeSource column values is null.");
                        }
                        targetColumn.setValue(null);
                    } else {
                        targetColumn.setValue(srcColumn.getValue());
                    }
                    islob = islob || targetColumn.isLobType();

                }

                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Process the data.");
                }

                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Start to get connection.");
                }
                // process the data
                Connection conn = null;
                if (!isOkay()) {
                    Message Message = new Message("数据库异常 {0}.", m_dbName);
                    m_logger.error(Message.toString());
                    if (m_dbSource != null) {
                        m_log.setDest_ip(m_dbSource.getDbHost());
                        m_log.setUserName(m_dbSource.getDbUser());
                    }
                    m_log.setOperate("savedb");
                    m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                    m_log.setFilename(defaultData.getFilename());
                    m_log.setPk_id(PkSet.getPks(rows[i]));
                    m_log.setDbName(tableMap.getTargetDb());
                    m_log.setTableName(tableMap.getTargetTable());
                    m_log.error(Message.toString());
                    sleepTime();
                    testDb();
                    throw new Ex().set(E.E_DatabaseConnectionError);
                }
                Connection connection = null;
                try {
                    if(connection == null&&isOracleDB()) {
                        String password = null;
                        try {
                            Class.forName(jdbcInfo.getDriverClass());
                            DesEncrypterAsPassword deap = new DesEncrypterAsPassword(StringUtil.S_PWD_ENCRYPT_CODE);
                            password = new String(deap.decrypt(jdbcInfo.getDbPassword().getBytes()));
                        } catch (Exception e) {
                        }
                        connection = DriverManager.getConnection(
                                jdbcInfo.getDriverUrl(),
                                jdbcInfo.getDbUser(), password);
                    }
                } catch (Exception e) {
                    m_logger.error("建立连接错误",e);
                }

                conn = getConnection();
                if (conn == null) {
                    throw new Ex().set(E.E_DatabaseConnectionError);
                }
                try {
                    if (bOnlyInsert) {
                        DbopUtil.setNotAutoCommit(conn);
                        if (isMssqlDB()) {
                            insertFullData(tableMap, conn,null);
                        } else if(isOracleDB()){
                            insertData(tableMap,conn,connection);
                        }else{
                            insertData(tableMap, conn,null);
                        }
                        //insucepks[i] = pks;
                        operator = Operator.Str_OPERATOR__INSERT;
                        //uditProces(tableMap.getSourceDb(), tableMap.getSourceTable(), tableMap.getTargetDb(), tableMap.getTargetTable(), pks, Operator.Str_OPERATOR__INSERT, 1);
                    } else {
                        if (rows[i].isDeleteAction()) {
                            if (bDeleteEnable) {
                                toCache(tableMap, Operator.OPERATOR__DELETE);
                                deleteRecord(tableMap, conn);
                                operator = Operator.Str_OPERATOR__DELETE;
                                //desucepks[i] = pks;

                            }
                        } else {
                            if (isRowExists(tableMap)) {
                                toCache(tableMap, Operator.OPERATOR__UPDATE);
                                if(isOracleDB()){
                                    updateData(tableMap, conn,connection);
                                } else {
                                    updateData(tableMap, conn,null);
                                }
                                operator = Operator.Str_OPERATOR__UPDATE;

                                //upsucepks[i] = pks;
                            } else {

                                toCache(tableMap, Operator.OPERATOR__INSERT);
                                if (isMssqlDB()){
                                    insertFullData(tableMap, conn,null);
                                }else if(isOracleDB()){
                                    insertData(tableMap, conn,connection);
                                }else {
                                    insertData(tableMap, conn,null);
                                }

                                //insucepks[i] = pks;
                                operator = Operator.Str_OPERATOR__INSERT;
                            }
                        }
                    }
                    commit(conn);
                } catch (SQLException e) {
                    EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to process basic data for  database/table {0}/{1} .", tableMap.getTargetDb(), tableMap.getTargetTable()));
                    EStatus status = EStatus.E_DATABASEERROR;
                    m_logger.error(new Message("Failed to process basic data for  database/table {0}/{1} .", tableMap.getTargetDb(), tableMap.getTargetTable()).toString());
                    m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
                    if (m_dbSource != null) {
                        m_log.setDest_ip(m_dbSource.getDbHost());
                        m_log.setUserName(m_dbSource.getDbUser());
                    }
                    m_log.setOperate("savedb");
                    m_log.setFilename(defaultData.getFilename());
                    m_log.setPk_id(PkSet.getPks(rows[i]));
                    m_log.setDbName(tableMap.getTargetDb());
                    m_log.setTableName(tableMap.getTargetTable());
                    m_log.error(new Message("Failed to process basic data for  database/table {0}/{1} .", tableMap.getTargetDb(), tableMap.getTargetTable()).toString());
                    status.setDbInfo(m_dbName, m_dbDesc);
                    m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
                    testBadConnection(conn, Exsql);
                    throw Exsql;
                } finally {

                    //DbopUtil.closeConnection(conn);
                    returnConnection(conn);
                    if(connection!=null){
                        try {
                            connection.close();
                        } catch (SQLException e) {
                        }
                    }
                }
            } // else {}
        }
        /**if (fr.getResultSize() > 0) {
         Logger auditLogger = Logger.getLogger("ichange.audit.database");
         StringBuffer buff = new StringBuffer();
         buff.append("Data Target Audit Info/(ChangeType,TargetDb/Table)/");
         buff.append(m_changeType.getType());
         buff.append("/");
         buff.append(m_dbName);
         buff.append("/");
         buff.append(tableMap.getTargetTable());
         buff.append("/");
         buff.append(String.valueOf(fr.getResultSize()) + " record.");
         EStatus status = EStatus.E_OK;
         if (m_dbSource != null) {
         m_log.setDest_ip(m_dbSource.getDbHost());
         m_log.setUserName(m_dbSource.getDbUser());
         }
         status.setDbInfo(m_dbName, m_dbDesc);
         m_changeMain.setStatus(m_changeType, EStatus.E_OK, null, false);
         auditLogger.info(buff.toString());
         m_log.setDbName(m_dbName);
         m_log.setTableName(tableMap.getTargetTable());
         m_log.setStatusCode(EStatus.E_OK.getCode() + "");
         m_log.setRecordCount(fr.getResultSize() + "");
         m_log.info(buff.toString());
         m_log.setStatusCode(null);


         }**/
        if (fr.getResultSize() > 0) {
            auditProces(tableMap.getSourceDb(), tableMap.getSourceTable(), tableMap.getTargetDb(), tableMap.getTargetTable(), pks, operator, fr.getResultSize());
        }
        return fr;
    }

    public void insertOrUpdateBlobData(ByteLargeObjectData byteLargeObjectData, FilterResult f) throws Ex {
        if (!f.getResult(0)) {        // process only the first record, the unique record
            return;
        }

        String sourceSchema = byteLargeObjectData.getSchemaName();
        String sourceTable = byteLargeObjectData.getTableName();
        String fieldName = byteLargeObjectData.getHeadValue(DataInformation.Str_FieldName);
        String pkString = byteLargeObjectData.getLobPkString();
        long length = byteLargeObjectData.getBlobLength();
        TableMap tableMap = findTableMap(sourceSchema, sourceTable);
        // todo: onlyinsert, deleteEnable and condition.
        if (!isOkay()) {
            Message Message = new Message("数据库异常 {0}.", m_dbName);
            m_logger.error(Message.toString());
            if (m_dbSource != null) {
                m_log.setDest_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setOperate("入库");
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.setDbName(tableMap.getTargetDb());
            m_log.setTableName(tableMap.getTargetTable());
            m_log.setPk_id(pkString);
            m_log.setFilename(byteLargeObjectData.getFilename());
            m_log.error(Message.toString());
            sleepTime();
            testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Connection conn = null;
        InputStream in = null;
        conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        try {
            PkSet pks = new PkSet(pkString);
            Column[] pkColumns = pks.getPkArray();
            int pkSize = pkColumns.length;
            String whereSqlString = lobWhereString(pkColumns, tableMap);
            Object[] params = new Object[pkSize];
            for (int j = 0; j < pkSize; j++) {
                params[j] = pkColumns[j].getObject();
            }

            Column lobColumn = tableMap.findColumnBySourceField(fieldName);
            if (lobColumn == null) {
                m_logger.warn("insertOrUpdateBlobData lobcolumn nanme get target Column is null.name:" + fieldName);
            }

            // put the this object into cache
            toCache(tableMap, Operator.OPERATOR__UPDATE);
            // todo: length: int->long


            if (lobColumn.getJdbcType() == Types.LONGVARBINARY ||
                    lobColumn.getJdbcType() == Types.VARBINARY || lobColumn.getJdbcType() == Types.BLOB) {
                in = byteLargeObjectData.getBlobInputStream();
            } else {
                in = byteLargeObjectData.getImageInputStream();
            }
            if (lobColumn != null) {
                if ((m_I_dbOp instanceof OracleIDbOp) && (lobColumn.getJdbcType() == Types.LONGVARBINARY ||
                        lobColumn.getJdbcType() == Types.VARBINARY)) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Start to insert/update longrow field.");
                    }
                    OracleIDbOp oracleDbms = new OracleIDbOp(conn);

                    oracleDbms.updateLongRawColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, in, (int) length);

                } else if ((m_I_dbOp instanceof OracleIDbOp) && (lobColumn.getJdbcType() == Types.BLOB)) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Start to insert/update blob field.");
                    }
                    m_I_dbOp.updateBlobColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, in, (int) length);
                } else {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Start to insert/update image field.");
                    }
                    m_I_dbOp.updateBlobColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, in, (int) length);
                }

                EStatus status = EStatus.E_OK;
                status.setDbInfo(m_dbName, m_dbDesc);
                m_changeMain.setStatus(m_changeType, status, null, false);
            }
        } catch (SQLException sqlEEx) {
            Message mess = new Message("A SQL exception occured during processing blob data for database/table  " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + ".");
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, mess);
            EStatus status = EStatus.E_DATABASEERROR;
            m_logger.error(mess.toString());
            if (m_dbSource != null) {
                m_log.setDest_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setOperate("入库");
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.setDbName(tableMap.getTargetDb());
            m_log.setTableName(tableMap.getTargetTable());
            m_log.setPk_id(pkString);
            m_log.setFilename(byteLargeObjectData.getFilename());
            m_log.error(mess.toString());
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } catch (IOException ioEEx) {
            throw new Ex().set(E.E_IOException, ioEEx, new Message("An IO exception occured during processing blob data for database/table  ", tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    m_logger.warn("InsertOrUpdateBlobData IOException.", e);
                }
            }
            returnConnection(conn);
        }

    }


    public void insertOrUpdateClobData(CharLargeObjectData charLargeObjectData, FilterResult f) throws Ex {
        if (!f.getResult(0)) {      // Process the only first row
            return;
        }
        String sourceSchema = charLargeObjectData.getSchemaName();
        String sourceTable = charLargeObjectData.getTableName();
        String fieldName = charLargeObjectData.getHeadValue(DataInformation.Str_FieldName);
        String pkString = charLargeObjectData.getLobPkString();
        long length = charLargeObjectData.getClobLength();

        TableMap tableMap = findTableMap(sourceSchema, sourceTable);

        Connection conn = null;
        if (!isOkay()) {
            Message Message = new Message("数据库异常 {0}.", m_dbName);
            m_logger.warn(Message.toString());
            if (m_dbSource != null) {
                m_log.setDest_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setOperate("入库");
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.setDbName(tableMap.getTargetDb());
            m_log.setTableName(tableMap.getTargetTable());
            m_log.setPk_id(pkString);
            m_log.setFilename(charLargeObjectData.getFilename());
            m_log.warn(Message.toString());
            sleepTime();
            testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        try {

            PkSet pks = new PkSet(pkString);
            Column[] pkColumns = pks.getPkArray();
            int pkSize = pkColumns.length;
            String whereSqlString = lobWhereString(pkColumns, tableMap);
            Object[] params = new Object[pkSize];
            for (int j = 0; j < pkSize; j++) {
                params[j] = pkColumns[j].getObject();
            }

            Column lobColumn = tableMap.findColumnBySourceField(fieldName);
            toCache(tableMap, Operator.OPERATOR__UPDATE);
            InputStreamReader reader = new InputStreamReader(charLargeObjectData.getClobInputStream(), DataInformation.Str_CharacterSet);
            if (lobColumn != null) {
                if ((m_I_dbOp instanceof OracleIDbOp) && lobColumn.getJdbcType() == Types.LONGVARCHAR) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Start to insert/update longrow field.");
                    }
                    OracleIDbOp oracleDbms = new OracleIDbOp(conn);
                    oracleDbms.updateLongColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, reader, (int) length);
                } else {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Start to insert/update clob field.");
                    }

                    m_I_dbOp.updateClobColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, reader, (int) length);
                }
                if (reader != null) {
                    reader.close();
                }
                EStatus status = EStatus.E_OK;
                status.setDbInfo(m_dbName, m_dbDesc);
                m_changeMain.setStatus(m_changeType, status, null, false);
            }
        } catch (SQLException e) {
            Message mess = new Message("An error occured in processing CLOB data for  database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + ".");
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, mess);
            EStatus status = EStatus.E_DATABASEERROR;
            m_logger.error(mess.toString());
            if (m_dbSource != null) {
                m_log.setDest_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setOperate("入库");
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.setDbName(tableMap.getTargetDb());
            m_log.setTableName(tableMap.getTargetTable());
            m_log.setPk_id(pkString);
            m_log.setFilename(charLargeObjectData.getFilename());
            m_log.error(mess.toString());
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } catch (IOException ioEEx) {
            throw new Ex().set(E.E_IOException, ioEEx);
        } finally {
            returnConnection(conn);
        }

    }

    private void commit(Connection conn) throws Ex {
        try {
            boolean nowCommit = conn.getAutoCommit();
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("now auto commit = " + nowCommit);
            }
            if (!nowCommit) {
                conn.commit();
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during processing commit data for database: {0}.", m_schemaName));
            testBadConnection(conn, Exsql);
            throw Exsql;
        }
    }


    private void toCache(TableMap tableMap, Operator operator) {
        if (tableMap.isInputTriggerEnable()) {
            TableInfo inputTableInfo = tableMap.getInputTableInfo();
            if (inputTableInfo != null) {
                if ((inputTableInfo.isMonitorDelete() && operator == Operator.OPERATOR__DELETE) ||
                        (inputTableInfo.isMonitorInsert() && operator == Operator.OPERATOR__INSERT) ||
                        (inputTableInfo.isMonitorUpdate() && operator == Operator.OPERATOR__UPDATE)) {
                    SourceObject ob = new SourceObject(tableMap.getTargetDb(), tableMap.getTargetTable(), operator);

                    Column[] pkColumns = tableMap.getPkFieldMap();
                    for (int i = 0; i < pkColumns.length; i++) {
                        Column c = pkColumns[i];
                        ob.addPk(c.getName(), c.getObject());
                    }
                    if (m_objectCache != null) {
                        m_objectCache.add(ob);
                        if (m_logger.isDebugEnabled()) {
                            m_logger.debug("an object is added into cache, cache size=" + m_objectCache.size());
                        }
                    }
                }
            }
        }
    }

    private void deleteRecord(TableMap tableMap, Connection conn) throws Ex {
        // Connection conn = m_dbPool.getConnection();
        PreparedStatement prepStmt = null;
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        ArrayList columns = new ArrayList();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (c.isPk()) {
                columns.add(fieldMaps[i].getTargetColumn());
            }
        }

        try {
            // String sqlString = "delete from {0}"
            String sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTable,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Get delete record query table string:" + sqlString);
            }

            String strWhere = getWherePkStringFromFieldMap(fieldMaps);
            sqlString += strWhere;
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Delete record sql string:" + sqlString);
            }

            prepStmt = m_I_dbOp.getSatement(conn, sqlString, (Column[]) columns.toArray(new Column[0]),null);
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            prepStmt.executeUpdate();
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during deleting a row for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }


    private String getWherePkStringFromFieldMap(ColumnMap[] fieldMaps) throws SQLException {

        StringBuffer pkSb = new StringBuffer();
        pkSb.append(" where ");
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("All column are " + c.getName());
            }
            if (c.isPk()) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Pk column is " + c.getName());
                }
                pkSb.append(m_I_dbOp.formatColumnName(c.getName()));
                pkSb.append("=?");
                pkSb.append(" and ");
            } //else {}
        }
        String strWhere = StringUtil.getFirstSubString(pkSb.toString(), " and ");

        return strWhere;
    }

    private boolean isRowExists(TableMap tableMap) throws Ex {
        ArrayList pkColumns = new ArrayList();
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        for (int i = 0; i < fieldMaps.length; i++) {
            ColumnMap columnMap = fieldMaps[i];
            Column c = columnMap.getTargetColumn();
            if (c.isPk()) {
                pkColumns.add(c);
            }
        }

        Connection conn = null;
        PreparedStatement prepStmt = null;
        if (!isOkay()) {
            Message Message = new Message("数据库异常 {0}.", m_dbName);
            m_logger.warn(Message.toString());
            if (m_dbSource != null) {
                m_log.setDest_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setOperate("入库");
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.warn(Message.toString());
            sleepTime();
            testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        conn = getConnection();
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }

        try {
            // String  sqlQuery = "select * from {0}"

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectFromTable,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});

            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Select sql query:" + sqlQuery);
            }

            String strWhere = getWherePkStringFromFieldMap(fieldMaps);
            sqlQuery += strWhere;
            if (m_logger.isDebugEnabled()) {
                m_logger.info("is Row exits All sql string:" + sqlQuery);
            }
            prepStmt = m_I_dbOp.getReadOnlySatement(conn, sqlQuery, (Column[]) pkColumns.toArray(new Column[0]),null);
            if (prepStmt == null) {
                throw new Ex().set(E.E_OperationError, new Message("GetSatement is null by testing a row's existence for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            }
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            prepStmt.setMaxRows(1);
            ResultSet rs = prepStmt.executeQuery();


            return rs.next();
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured while testing a row's existence for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
            //DbopUtil.closeConnection(conn);
            returnConnection(conn);
        }
    }

    //todo:修改处理机制（所有字段都通过Steamtment执行）
    private void insertFullData(TableMap tableMap, Connection conn,Connection connection) throws Ex {
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        PreparedStatement prepStmt = null;
        ArrayList valueColumns = new ArrayList();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            valueColumns.add(c);
//            if (DbopUtil.isLobType(c.getJdbcType())) {
//                valueColumns.add(c);
//            }
        }

        try {
            // String sqlString = "insert into {0}"
            String sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_InsertIntoTable, new String[]{
                    m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});

            StringBuffer result = new StringBuffer();
            result.append(sqlString);
            result.append(getInsertSelectColumns(fieldMaps));
            result.append(getColumnFullValues(fieldMaps));
            String sql = result.toString();
            if (m_logger.isDebugEnabled()) {
                m_logger.info("insert data sql string:" + sql);
            }

            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Value column size :" + valueColumns.size());
            }

            prepStmt = m_I_dbOp.getSatement(conn, sql, (Column[]) valueColumns.toArray(new Column[0]),connection);
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            int rowInserted = prepStmt.executeUpdate();
            if (rowInserted != 1) {
                throw new Ex().set(E.E_DatabaseError, new Message("insert data error:" + result.toString()));
            }
        } catch (SQLException sqlEEx) {
            m_logger.error("insert error 1 ",sqlEEx);
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during inserting data for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    private void insertData(TableMap tableMap, Connection conn,Connection connection) throws Ex {
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        PreparedStatement prepStmt = null;
        ArrayList valueColumns = new ArrayList();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (!DbopUtil.isLobType(c.getJdbcType())) {
                valueColumns.add(c);
            }
        }

        try {
            // String sqlString = "insert into {0}"
            String sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_InsertIntoTable, new String[]{
                    m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});

            StringBuffer result = new StringBuffer();
            result.append(sqlString);
            result.append(getInsertSelectColumns(fieldMaps));
            result.append(getColumnValues(fieldMaps));
            String sql = result.toString();
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("insert data sql string:" + sql);
            }

            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Value column size :" + valueColumns.size());
            }

            prepStmt = m_I_dbOp.getSatement(conn, sql, (Column[]) valueColumns.toArray(new Column[0]),connection);
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            int rowInserted = prepStmt.executeUpdate();
            if (rowInserted != 1) {
                throw new Ex().set(E.E_DatabaseError, new Message("insert data error:" + result.toString()));
            }
        } catch (SQLException sqlEEx) {
            m_logger.error("insert error 2",sqlEEx);
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during inserting data for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    private String getInsertSelectColumns(ColumnMap[] fieldMaps) throws SQLException {

        String strSelectColumns = " (";
        HashMap maptest = new HashMap();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (!maptest.containsKey(m_I_dbOp.formatColumnName(c.getName()))) {
                strSelectColumns += m_I_dbOp.formatColumnName(c.getName());
                if (i != fieldMaps.length - 1) {
                    strSelectColumns += ",";
                }
            }
            maptest.put(m_I_dbOp.formatColumnName(c.getName()), m_I_dbOp.formatColumnName(c.getName()));

        }
        maptest.clear();
        strSelectColumns += ")";

        if (m_logger.isDebugEnabled()) {
        m_logger.info("insert select columns sql: " + strSelectColumns);
        }

        return strSelectColumns;
    }


    private String getColumnValues(ColumnMap[] fieldMaps) {
        StringBuffer valueSb = new StringBuffer();
        valueSb.append(" values (");

        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();

            if (DbopUtil.isBlobType(c.getJdbcType())) {
                // LONGVARBINARY -->"null,"
                if (m_I_dbOp instanceof OracleIDbOp && (c.getJdbcType() == Types.LONGVARBINARY ||
                        c.getJdbcType() == Types.VARBINARY)) {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append("null,");
                    } else {
                        valueSb.append("null");
                    }
                } else {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append(m_I_dbOp.getBlobInitializer() + ",");
                    } else {
                        valueSb.append(m_I_dbOp.getBlobInitializer());
                    }
                }

            } else if (DbopUtil.isClobType(c.getJdbcType())) {
                if (m_I_dbOp instanceof OracleIDbOp && c.getJdbcType() == Types.LONGVARCHAR) {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append("null,");
                    } else {
                        valueSb.append("null");
                    }
                } else {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append(m_I_dbOp.getClobInitializer() + ",");
                    } else {
                        valueSb.append(m_I_dbOp.getClobInitializer());
                    }
                }
            } else {
                if (i != fieldMaps.length - 1) {
                    valueSb.append("?,");
                } else {
                    valueSb.append("?");
                }
            }


        }
        // valueSb.append(StringUtil.getFirstSubString(valueSb.toString(), ","));
        valueSb.append(")");
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Column value " + valueSb.toString());
        }
        return valueSb.toString();
    }

    private String getColumnFullValues(ColumnMap[] fieldMaps) {
        StringBuffer valueSb = new StringBuffer();
        valueSb.append(" values (");
        HashMap maptest = new HashMap();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (!maptest.containsKey(c.getName())) {
                if (DbopUtil.isBlobType(c.getJdbcType())) {
                    // LONGVARBINARY -->"null,"
                    if (m_I_dbOp instanceof OracleIDbOp && (c.getJdbcType() == Types.LONGVARBINARY ||
                            c.getJdbcType() == Types.VARBINARY)) {
                        if (i != fieldMaps.length - 1) {
                            valueSb.append("null,");
                        } else {
                            valueSb.append("null");
                        }
                    } else {
                        if (i != fieldMaps.length - 1) {
                            valueSb.append("?,");
                        } else {
                            valueSb.append("?");
                        }
                    }

                } else if (DbopUtil.isClobType(c.getJdbcType())) {
                    if (m_I_dbOp instanceof OracleIDbOp && c.getJdbcType() == Types.LONGVARCHAR) {
                        if (i != fieldMaps.length - 1) {
                            valueSb.append("null,");
                        } else {
                            valueSb.append("null");
                        }
                    } else {
                        if (i != fieldMaps.length - 1) {
                            valueSb.append("?" + ",");
                        } else {
                            valueSb.append("?");
                        }
                    }
                } else {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append("?,");
                    } else {
                        valueSb.append("?");
                    }
                }
            }
            maptest.put(c.getName(), c.getName());
        }
        // valueSb.append(StringUtil.getFirstSubString(valueSb.toString(), ","));
        valueSb.append(")");
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Column value " + valueSb.toString());
        }
        return valueSb.toString();
    }

    private void updateData(TableMap tableMap, Connection conn,Connection connection) throws Ex {

        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        ArrayList valueRows = new ArrayList();
        ArrayList pkRows = new ArrayList();
        // Connection conn  = m_dbPool.getConnection();
        PreparedStatement prepStmt = null;

        for (int i = 0; i < fieldMaps.length; i++) {
            ColumnMap columnMap = fieldMaps[i];
            Column c = columnMap.getTargetColumn();
            if (c.isPk()) {
                pkRows.add(c);
            } else {
                if (!DbopUtil.isLobType(c.getJdbcType())) {
                    valueRows.add(c);
                } //else {}
            }
        }
        valueRows.addAll(pkRows);

        try {
            String updateSql = updateBasicDataSqlString(tableMap);
            if (updateSql.equalsIgnoreCase("")) {
                return;
            } else {
                prepStmt = m_I_dbOp.getSatement(conn, updateSql, (Column[]) valueRows.toArray(new Column[0]),connection);
                prepStmt.setQueryTimeout(I_StatementTimeOut);
                //prepStmt.setMaxRows(1);
                prepStmt.executeUpdate();
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during updating data for datababse/table:  " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + tableMap.getTargetDb() + "table:" + tableMap.getTargetTable(), false);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    private String updateBasicDataSqlString(TableMap tableMap) throws SQLException {
        boolean bSetable = false;
        StringBuffer result = new StringBuffer();
        StringBuffer valueSb = new StringBuffer();
        StringBuffer pkSb = new StringBuffer();

        result.append("update ");
        result.append(m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable()));
        result.append(" set ");

        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        for (int i = 0; i < fieldMaps.length; i++) {
            ColumnMap columnMap = fieldMaps[i];
            Column c = columnMap.getTargetColumn();
            if (c.isPk()) {
                pkSb.append(m_I_dbOp.formatColumnName(c.getName()));
                pkSb.append("=? and ");
            } else {
                if (DbopUtil.isBlobType(c.getJdbcType())) {

                } else if (DbopUtil.isClobType(c.getJdbcType())) {

                } else {
                    valueSb.append(m_I_dbOp.formatColumnName(c.getName()));
                    valueSb.append("=?,");
                    bSetable = true;
                }
            }
        }

        if (!bSetable) {
            return "";
        }

        result.append(StringUtil.getFirstSubString(valueSb.toString(), ','));
        result.append(" where ");
        result.append(StringUtil.getFirstSubString(pkSb.toString(), " and "));
        String sql = result.toString();
        if (m_logger.isDebugEnabled()) {
            m_logger.info("update sql string:" + sql);
        }

        return sql;
    }


    private String lobWhereString(Column[] pkColumns, TableMap tableMap) throws SQLException {
        StringBuffer pkSb = new StringBuffer();
        for (int j = 0; j < pkColumns.length; j++) {
            Column pkColumn = pkColumns[j];
            Column pkTargetColumn = tableMap.findColumnBySourceField(pkColumn.getName());
            pkColumn.setJdbcType(pkTargetColumn.getJdbcType());
            pkTargetColumn.setValue(pkColumn.getValue());
            pkSb.append(m_I_dbOp.formatColumnName(pkTargetColumn.getName()));
            pkSb.append("=? and ");
        }
        String whereSqlString = "where " + StringUtil.getFirstSubString(pkSb.toString(), " and ");
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Lob where string:" + whereSqlString);
        }

        return whereSqlString;
    }


    private TableMap findTableMap(String sourceSchema, String sourceTable) throws Ex {
        TableMap tableMap = null;
        for (int j = 0; j < m_tableMaps.length; j++) {
            tableMap = m_tableMaps[j];
            if (sourceSchema.equalsIgnoreCase(tableMap.getSourceDb()) &&
                    sourceTable.equalsIgnoreCase(tableMap.getSourceTable())) {
                return tableMap;
            }
        }

        if (tableMap == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("Can not find DbChangeSource database {0} and " +
                    "DbChangeSource table {1} in the config mapper", sourceSchema, sourceTable));
        }

        return tableMap;
    }

    private void auditProces(String srcdb, String srctable, String tardb, String tartable, String pks, String operator, int size) {
        /**Logger auditLogger = Logger.getLogger("ichange.audit.database");
         StringBuffer buff = new StringBuffer();
         buff.append("Data Target Audit Info/(ChangeType,TargetDb/Table)/");
         buff.append(m_changeType.getType());
         buff.append("/");
         buff.append(m_dbName);
         buff.append("/");
         buff.append(tartable);
         buff.append("/");
         buff.append(String.valueOf(size) + " record.");
         EStatus status = EStatus.E_OK;*/
        if (m_dbSource != null) {
            m_log.setDest_ip(m_dbSource.getDbHost());
            m_log.setUserName(m_dbSource.getDbUser());
        }
        m_log.setSourceDbName(srcdb);
        m_log.setSourceTableName(srctable);
        m_log.setDestDbName(tardb);
        m_log.setDestTableName(tartable);
        m_log.setPks(pks);
        m_log.setOperate(operator);
        m_log.setOpertion(operator);
        m_log.setOperType(operator);
        m_log.setStatusCode(EStatus.E_OK.getCode() + "");
        m_log.setRecordCount(size + "");
        m_log.info(operator + "to  table:" + tartable + " " + size + " record.");
    }

}
