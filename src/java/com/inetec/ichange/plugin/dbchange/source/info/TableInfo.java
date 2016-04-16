
package com.inetec.ichange.plugin.dbchange.source.info;

import com.inetec.common.exception.Ex;
import com.inetec.common.config.nodes.Table;
import com.inetec.common.config.nodes.DataBase;

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;


import org.apache.log4j.Category;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class TableInfo {


    private String name;
    private boolean delete;
    private boolean specifyFlag;
    private boolean triggerEnable;
    private boolean allTableEnable;
    private boolean isTimeSync;

    private int sequence;
    private int interval;
    private boolean monitorInsert;
    private boolean monitorUpdate;
    private boolean monitorDelete;
    private boolean mergeTable = false;

    private Column[] basicColumns = new Column[0];
    private Column[] clobColumns = new Column[0];
    private Column[] blobColumns = new Column[0];
    private Map mergeTables = new HashMap();
    private Column[] pkColumns = new Column[0];

    private long nextTime = System.currentTimeMillis();

    private boolean m_twoWay;
    private Category m_logger = Category.getInstance(TableInfo.class);

    public TableInfo(Table tableConfigNode, String operation) throws Ex {
        name = tableConfigNode.getTableName();
        String strValue;
        delete = tableConfigNode.isDeleteEnable();
        sequence = tableConfigNode.getSeqNumber();
        interval = tableConfigNode.getInterval();
        monitorInsert = tableConfigNode.isMonitorInsert();
        monitorUpdate = tableConfigNode.isMonitorUpdate();
        monitorDelete = tableConfigNode.isMonitorDelete();
        m_twoWay = tableConfigNode.isTwoway();
        triggerEnable = monitorInsert || monitorDelete || monitorUpdate;
        delete = operation.equalsIgnoreCase(DataBase.Str_DeleteOperation) ? true : false;
        specifyFlag = operation.equalsIgnoreCase(DataBase.Str_FlagOperation) ? true : false;
        triggerEnable = operation.equalsIgnoreCase(DataBase.Str_TriggerOperation) ? true : false;
        allTableEnable = operation.equalsIgnoreCase(DataBase.Str_AllTableOperation) ? true : false;
        isTimeSync = operation.equalsIgnoreCase(DataBase.Str_TimeSyncOperation) ? true : false;

        ArrayList basicFieldList = new ArrayList();
        ArrayList clobFieldList = new ArrayList();
        ArrayList blobFieldList = new ArrayList();
        ArrayList pkFieldList = new ArrayList();
        if (tableConfigNode.getAllMergeTables().length > 0) {
            mergeTable = true;
            for (int i = 0; i < tableConfigNode.getAllMergeTables().length; i++) {
                setMergeTableInfo(MergeTableInfo.MergeTableInfo(tableConfigNode.getAllMergeTables()[i], name));
            }
        }

        com.inetec.common.config.nodes.Field[] fields = (com.inetec.common.config.nodes.Field[]) tableConfigNode.getAllFields();
        for (int i = 0; i < fields.length; i++) {
            int jdbcType = DbopUtil.getJdbcType(fields[i].getJdbcType());
            Column c = new Column(fields[i].getFieldName(), DbopUtil.getJdbcType(fields[i].getJdbcType()),
                    fields[i].getDbType(), fields[i].isPk());
            if (c.isPk()) {
                pkFieldList.add(c);
            }
            if (c.isBlobType()) {
                blobFieldList.add(c);
            } else if (c.isClobType()) {
                clobFieldList.add(c);
            } else {

                basicFieldList.add(c);
            }

        }
        basicColumns = (Column[]) basicFieldList.toArray(new Column[0]);
        clobColumns = (Column[]) clobFieldList.toArray(new Column[0]);
        blobColumns = (Column[]) blobFieldList.toArray(new Column[0]);
        pkColumns = (Column[]) pkFieldList.toArray(new Column[0]);

        //checkConfigInfo();

    }


    public Column find(String fieldName) {

        for (int i = 0; i < basicColumns.length; i++) {
            if (basicColumns[i].getName().trim().equalsIgnoreCase(fieldName.trim())) {
                return basicColumns[i];
            }
        }
        for (int i = 0; i < clobColumns.length; i++) {
            if (clobColumns[i].getName().trim().equalsIgnoreCase(fieldName.trim())) {
                return clobColumns[i];
            }
        }
        for (int i = 0; i < blobColumns.length; i++) {
            if (blobColumns[i].getName().trim().equalsIgnoreCase(fieldName.trim())) {
                return blobColumns[i];
            }
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Can not find field name.");
        }
        return null;
    }

    public Column[] getBasicColumns() {
        return basicColumns;
    }

    public Column[] getBlobColumns() {
        return blobColumns;
    }

    public Column[] getClobColumns() {
        return clobColumns;
    }

    public Column[] getPkColumns() {
        return pkColumns;
    }

    public Column getTimeSyncTimeField() {
        Column result = null;
        if (isTimeSync)
            for (int i = 0; i < pkColumns.length; i++) {
                if (pkColumns[i].getJdbcType() == Types.DATE || pkColumns[i].getJdbcType() == Types.TIME || pkColumns[i].getJdbcType() == Types.TIMESTAMP) {
                    result = pkColumns[i];
                }
            }
        return result;
    }
    /*
    //todo: check the code
    public void setTableMetadata(TableMetadata tableMetadata) {
        this.tableMetadata = tableMetadata;
        if (specifyFields) {
            Set keySet = fieldProp.keySet();
            Iterator it = keySet.iterator();
            while (it.hasNeExt()) {
                String srcField = (String)it.next();
                Column c = tableMetadata.find(srcField);
                if (c.isLobType()) {
                    fieldsInfo.addLast(srcField);
                } else {
                    fieldsInfo.addFirst(srcField);
                }
            }
        } else {
            Column[] cs = tableMetadata.getColumns();
            int size = cs.length;
            for (int i=0; i<size; i++) {
                String srcField = cs[i].getName();
                Column c = tableMetadata.find(srcField);
                if (c.isLobType()) {
                    fieldsInfo.addLast(srcField);
                } else {
                    fieldsInfo.addFirst(srcField);
                }
            }

        }

    }
    */


    public void checkConfigInfo() {

        //boolean invalid = monitorDelete
        if (monitorDelete && delete) {
            //if (m_logger.isDebugEnabled()) {
            m_logger.warn("monitorDelete and delete value are not allowed to both set true value, table: " + name);
            //}
        }
        if ((monitorInsert || monitorUpdate) && specifyFlag) {
            //if (m_logger.) {
            m_logger.warn("monitorInsert/monitorUpdate and specifyFlag value are not allowed to both set true value, table: " + name);
            //}
        }
        if (!(monitorDelete || delete || monitorUpdate || monitorInsert || specifyFlag)) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("monitorInsert, monitorUpdate, monitorDelete, specifyFlag and delete value are not allowed to all set false value, table: " + name);
            }
        }

        delete = delete && (!monitorDelete) && !(specifyFlag && (monitorInsert || monitorUpdate));
        specifyFlag = specifyFlag && (!delete) && (!(monitorInsert || monitorUpdate));
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("delete are set to " + delete + " in table " + name);
            m_logger.debug("specifyFlag are set to " + specifyFlag + " in table " + name);
        }
        /*if(!( m_twoWay&&triggerEnable)) {
            m_logger.warn("Twoway and monitor allowed to both set true value,table:"+name);
        }*/
    }


    public long getNeExtTime() {
        return nextTime;
    }

    public void nextTime() {
        nextTime += interval * 1000;
    }


    public String getName() {
        return name;
    }

    public boolean isDelete() {
        return delete;
    }

    public boolean isSpecifyFlag() {
        return specifyFlag;
    }

    public boolean isTriggerEnable() {
        return triggerEnable;
    }


    public int getSequence() {
        return sequence;
    }

    public int getInterval() {
        return interval;
    }

    public boolean isMonitorInsert() {
        return monitorInsert;
    }

    public boolean isMonitorUpdate() {
        return monitorUpdate;
    }

    public boolean isMonitorDelete() {
        return monitorDelete;
    }

    public boolean isTwoway() {
        return m_twoWay;
    }

    public boolean isMergeTable() {
        return mergeTable;
    }

    public boolean isTimeSync() {
        return isTimeSync;
    }

    public void setTimeSync(boolean timeSync) {
        isTimeSync = timeSync;
    }

    public void setMergeTableInfo(MergeTableInfo info) {
        mergeTables.put(info.getMergeTableName(), info);
    }

    public MergeTableInfo[] getAllMergeTable() {

        return (MergeTableInfo[]) mergeTables.values().toArray(new MergeTableInfo[0]);
    }

    public MergeTableInfo getMergeTableByName(String name) {

        return (MergeTableInfo) mergeTables.get(name);
    }

}
