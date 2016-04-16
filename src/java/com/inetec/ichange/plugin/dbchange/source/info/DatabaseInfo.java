/*=============================================================
 * 文件名称: DatabaseInfo.java
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
package com.inetec.ichange.plugin.dbchange.source.info;


import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.common.exception.Ex;
import com.inetec.common.config.nodes.*;


public class DatabaseInfo {
    public static int I_MaxRecord = 800;
    private String name;
    private String dbType;
    private int interval;
    private int maxRecord;

    private boolean enable;
    private boolean delete;
    private boolean specifyFlag;
    private boolean triggerEnable;
    private boolean allTableEnable;
    private boolean oldStep;
    private boolean timeSync;
    private boolean sequenceEnable;
    private String encoding;


    private String todoTable;
    private String flagName;
    private TableInfo[] tablesInfo;
    private JdbcInfo jdbcInfo;

    private boolean m_twoWay;
    private final String m_flagName = "ICHANGE_FLAG";


    private boolean m_btwowayChange = false;

    public boolean isAllTableEnable() {
        return allTableEnable;
    }

    public DatabaseInfo(DataBase database, Jdbc jdbc) throws Ex {

        name = database.getDbName();
        interval = database.getInterval();
        maxRecord = database.getMaxRecords();
        if (maxRecord > I_MaxRecord) {
            maxRecord = I_MaxRecord;
        }

        enable = database.isEnable();
        String operation = database.getOperation();
        if (operation == null) {
            operation = "";
        }
        delete = operation.equalsIgnoreCase(DataBase.Str_DeleteOperation) ? true : false;
        specifyFlag = operation.equalsIgnoreCase(DataBase.Str_FlagOperation) ? true : false;
        triggerEnable = operation.equalsIgnoreCase(DataBase.Str_TriggerOperation) ? true : false;
        sequenceEnable = operation.equalsIgnoreCase(DataBase.Str_SequenceOperation) ? true : false;
        allTableEnable = operation.equalsIgnoreCase(DataBase.Str_AllTableOperation) ? true : false;
        timeSync = operation.equalsIgnoreCase(DataBase.Str_TimeSyncOperation) ? true : false;
        oldStep = database.isOldStep();


        todoTable = database.getTempTable();
        flagName = m_flagName;

        Table[] tables = (Table[]) database.getAllTables();
        tablesInfo = new TableInfo[tables.length];
        for (int i = 0; i < tables.length; i++) {
            tablesInfo[i] = new TableInfo(tables[i], operation);
        }
        jdbcInfo = new JdbcInfo(jdbc);
        encoding = jdbcInfo.getDbCharset();
        dbType = jdbcInfo.getDbServerVender();
        m_twoWay = database.isTwoway();
    }

    public JdbcInfo getJdbcInfo() {
        return jdbcInfo;
    }

    public TableInfo[] getTableInfo() {
        return tablesInfo;
    }


    public TableInfo find(String tableName) {
        TableInfo[] tableInfos = tablesInfo;
        for (int i = 0; i < tableInfos.length; i++) {
            if (tableInfos[i].getName().equalsIgnoreCase(tableName)) {
                return tableInfos[i];
            }
        }

        return null;
    }

    public Column find(String tableName, String fieldName) {
        TableInfo ti = find(tableName);

        if (ti != null) {
            return ti.find(fieldName);
        }

        return null;
    }

    public long getNeExtSleepTime() {
        long sleepTime = 0;
        long nowTime = System.currentTimeMillis();
        TableInfo[] tableInfos = tablesInfo;
        for (int i = 0; i < tableInfos.length; i++) {
            long nextTime = tableInfos[i].getNeExtTime();
            if (nextTime < nowTime) {
                return -1;
            } else {
                long diffTime = nextTime - nowTime;
                if (sleepTime == 0) {
                    sleepTime = diffTime;
                } else {
                    if (diffTime < sleepTime) {
                        sleepTime = diffTime;
                    }
                }
            }
        }

        return sleepTime;
    }


    public String getName() {
        return name;
    }

    public String getDbType() {
        return dbType;
    }

    public int getInterval() {
        return interval;
    }

    public int getMaxRecord() {
        return maxRecord;
    }

    public boolean isEnable() {
        return enable;
    }


    public boolean isDelete() {
        return delete;
    }

    public boolean isSpecifyFlag() {
        return specifyFlag;
    }

    public boolean isSequenceEnable() {
        return sequenceEnable;
    }

    public boolean isTriggerEnable() {
        return triggerEnable;
    }

    public boolean isOldStep() {
        return oldStep;
    }

    public boolean isTimeSync() {
        return timeSync;
    }

    public void setTimeSync(boolean timeSync) {
        this.timeSync = timeSync;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getTodoTable() {
        return todoTable;
    }

    public String getFlagName() {
        return flagName;
    }


    public boolean isTwoway() {
        return m_twoWay;
    }

    public void setTwowayChange() {
        m_btwowayChange = true;
    }

    public boolean isTwowayChange() {
        return m_btwowayChange;
    }

    public void resetTwowayChange() {
        m_btwowayChange = false;
    }
}
