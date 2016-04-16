package com.inetec.ichange.plugin.dbchange.source.info;

import com.inetec.ichange.plugin.dbchange.source.twoway.SourceObject;

import java.util.HashMap;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class TempRowBeanCache {

    private ArrayList m_obArray = new ArrayList();

    public  void add(TempRowBean ob) {
        m_obArray.add(ob);
    }

    public  boolean remove(TempRowBean ob) {
        boolean result = false;

        if (m_obArray.contains(ob)) {
            m_obArray.remove(ob);
            result = true;
        }
        return result;
    }


    public  int size() {
        return m_obArray.size();
    }
}