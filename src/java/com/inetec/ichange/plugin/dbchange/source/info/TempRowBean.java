package com.inetec.ichange.plugin.dbchange.source.info;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2008-11-29
 * Time: 19:09:36
 * To change this template use File | Settings | File Templates.
 */
public class TempRowBean {
    private String ids = "";
    private int length = 0;
    private long beanid = System.currentTimeMillis();
    private long maxId = 0;

    public long getMaxId() {
        return maxId;
    }

    public void setMaxId(long maxId) {
        this.maxId = maxId;
    }

    public String getIds() {
        return ids;
    }

    public void setIds(String ids) {
        this.ids = ids;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
