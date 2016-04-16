package com.ichange.dbchange;

import com.inetec.ichange.api.IChangeType;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2007-6-5
 * Time: 23:48:38
 * To change this template use File | Settings | File Templates.
 */
public class ChangeTypeImp implements IChangeType {
    private String type;

    public String getType() {
        return type;
    }

    public ChangeTypeImp(String type) {
        this.type = type;
    }
}
