package com.inetec.ichange.plugin.dbchange.target.info;

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.source.info.TableInfo;

import java.util.ArrayList;


/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class TableMap {


    private String sourceDb;
    private String targetDb;
    private String sourceTable;
    private String targetTable;
    private boolean inputTriggerEnable;
    private TableInfo inputTableInfo;
    private ArrayList listFieldMap = new ArrayList();

    private boolean deleteEnable = false;
    private boolean specifyField = true;
    private boolean onlyinsert = false;
    private String condition;
    private String encoding;


    public TableMap(String sourceDb, String sourceTable, String targetDb, String targetTable) {
        this.sourceDb = sourceDb;
        this.sourceTable = sourceTable;
        this.targetDb = targetDb;
        this.targetTable = targetTable;
    }

    public int getTotalnumber() {
        return totalnumber;
    }

    public void setTotalnumber(int totalnumber) {
        this.totalnumber = totalnumber;
    }

    public int getInCount() {
        return inCount;
    }

    public void setInCount(int inCount) {
        this.inCount = inCount;
    }

    public int getInFailed() {
        return inFailed;
    }

    public void setInFailed(int inFailed) {
        this.inFailed = inFailed;
    }

    public int getUpCount() {
        return upCount;
    }

    public void setUpCount(int upCount) {
        this.upCount = upCount;
    }

    public int getUpFailed() {
        return upFailed;
    }

    public void setUpFailed(int upFailed) {
        this.upFailed = upFailed;
    }

    public int getDeCount() {
        return deCount;
    }

    public void setDeCount(int deCount) {
        this.deCount = deCount;
    }

    public int getDeFailed() {
        return deFailed;
    }

    public void setDeFailed(int deFailed) {
        this.deFailed = deFailed;
    }


    public byte[] getSelectPk() {
        return selectPk;
    }

    public void setSelectPk(byte[] selectPk) {
        this.selectPk = selectPk;
    }

    public byte[] getInSucessPk() {
        return inSucessPk;
    }

    public void setInSucessPk(byte[] inSucessPk) {
        this.inSucessPk = inSucessPk;
    }

    public byte[] getInFailedPk() {
        return inFailedPk;
    }

    public void setInFailedPk(byte[] inFailedPk) {
        this.inFailedPk = inFailedPk;
    }

    public byte[] getUpSucessPk() {
        return upSucessPk;
    }

    public void setUpSucessPk(byte[] upSucessPk) {
        this.upSucessPk = upSucessPk;
    }

    public byte[] getUpFailedPk() {
        return upFailedPk;
    }

    public void setUpFailedPk(byte[] upFailedPk) {
        this.upFailedPk = upFailedPk;
    }

    public byte[] getDeSucessPk() {
        return deSucessPk;
    }

    public void setDeSucessPk(byte[] deSucessPk) {
        this.deSucessPk = deSucessPk;
    }

    public byte[] getDeFailedPk() {
        return deFailedPk;
    }

    public void setDeFailedPk(byte[] deFailedPk) {
        this.deFailedPk = deFailedPk;
    }

    public void setInputTableInfo(TableInfo inputTableInfo) {
        this.inputTableInfo = inputTableInfo;
    }

    public void setInputTriggerEnable(boolean value) {
        inputTriggerEnable = value;
    }

    public TableInfo getInputTableInfo() {
        return inputTableInfo;
    }

    public boolean isInputTriggerEnable() {
        return inputTriggerEnable;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public String getTargetDb() {
        return targetDb;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public boolean isDeleteEnable() {
        return deleteEnable;
    }

    public void setDeleteEnable(boolean deleteEnable) {
        this.deleteEnable = deleteEnable;
    }

    public boolean isSpecifyField() {
        return specifyField;
    }

    public void setSpecifyField(boolean specifyField) {
        this.specifyField = specifyField;
    }

    public boolean isOnlyInsert() {
        return onlyinsert;
    }

    public void setOnlyInsert(boolean onlyinsert) {
        this.onlyinsert = onlyinsert;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }


    public void AddFieldMap(ColumnMap map) {
        listFieldMap.add(map);
    }

    public ColumnMap[] getFieldMap() {
        return (ColumnMap[]) listFieldMap.toArray(new ColumnMap[0]);
    }

    public Column[] getPkFieldMap() {
        ArrayList pkList = new ArrayList();
        for (int i = 0; i < listFieldMap.size(); i++) {
            ColumnMap map = (ColumnMap) listFieldMap.get(i);
            Column tc = map.getTargetColumn();
            if (tc.isPk()) {
                pkList.add(tc);
            }
        }

        return (Column[]) pkList.toArray(new Column[0]);
    }


    public Column findColumnByTargetField(String targetField) {
        for (int i = 0; i < listFieldMap.size(); i++) {
            ColumnMap map = (ColumnMap) listFieldMap.get(i);
            if (map.getTargetField().equalsIgnoreCase(targetField)) {
                return map.getTargetColumn();
            }
        }

        return null;
    }

    public Column findColumnBySourceField(String sourceField) {
        for (int i = 0; i < listFieldMap.size(); i++) {
            ColumnMap map = (ColumnMap) listFieldMap.get(i);
            if (map.getSourceField().equalsIgnoreCase(sourceField)) {
                return map.getTargetColumn();
            }
        }

        return null;
    }

    //count the record number for audit
    private int totalnumber = 0;

    private int inCount = 0;
    private int inFailed = 0;
    private int upCount = 0;
    private int upFailed = 0;
    private int deCount = 0;
    private int deFailed = 0;

    private byte[] selectPk = null;

    private byte[] inSucessPk = null;
    private byte[] inFailedPk = null;
    private byte[] upSucessPk = null;
    private byte[] upFailedPk = null;
    private byte[] deSucessPk = null;
    private byte[] deFailedPk = null;


}
