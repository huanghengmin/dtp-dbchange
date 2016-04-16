package com.inetec.ichange.plugin.dbchange.source.info;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2010-5-18
 * Time: 20:56:29
 * To change this template use File | Settings | File Templates.
 */
public class TimeSyncBean {
    /**
     * 开始时间
     */
    private Date begintime;
    /**
     * 结束时间
     */
    private Date endTime;
    /**
     * 开始PKID
     */
    private String beginid;
    /**
     * 结束PKID
     */
    private String endid;
    /**
     * 时间间隔
     */
    private int invite;

    public Date getBegintime() {
        return begintime;
    }

    public void setBegintime(Date begintime) {
        this.begintime = begintime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getBeginid() {
        return beginid;
    }

    public void setBeginid(String beginid) {
        this.beginid = beginid;
    }

    public String getEndid() {
        return endid;
    }

    public void setEndid(String endid) {
        this.endid = endid;
    }

    public int getInvite() {
        return invite;
    }

    public void setInvite(int invite) {
        this.invite = invite;
    }
}
