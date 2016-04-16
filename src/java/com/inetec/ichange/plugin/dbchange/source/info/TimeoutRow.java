package com.inetec.ichange.plugin.dbchange.source.info;

import com.inetec.ichange.plugin.dbchange.datautils.db.Row;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 2006-5-28
 * Time: 22:14:31
 * To change this template use File | Settings | File Templates.
 */
public class TimeoutRow {

    public Row getRow() {
        return m_row;
    }

    public long getTime() {
        return m_time;
    }


    private Row m_row;
    private long m_time;

    public TimeoutRow(Row row) {
        m_row = row;
        m_time = System.currentTimeMillis();
    }

    public boolean verifier() {
        boolean result = false;
        long currentTime = System.currentTimeMillis();
        if (currentTime - m_time >= 30 * 60 * 1000) {
            result = true;
        } else {
            result = false;
        }

        return result;
    }

}
