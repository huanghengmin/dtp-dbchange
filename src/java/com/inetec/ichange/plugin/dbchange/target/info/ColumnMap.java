package com.inetec.ichange.plugin.dbchange.target.info;

import com.inetec.ichange.plugin.dbchange.datautils.db.Column;


/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class ColumnMap {


    private String sourceField;
    private Column targetColumn;

    public ColumnMap(String sf, Column c) {
        sourceField = sf;
        targetColumn = c;
    }

    public String getSourceField() {
        return sourceField;
    }

    public String getTargetField() {
        return targetColumn.getName();
    }

    public Column getTargetColumn() {
        return targetColumn;
    }


}
