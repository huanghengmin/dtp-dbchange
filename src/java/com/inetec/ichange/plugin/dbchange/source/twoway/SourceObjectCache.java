package com.inetec.ichange.plugin.dbchange.source.twoway;

import java.util.HashMap;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class SourceObjectCache {

    private ArrayList m_obArray = new ArrayList();

    public synchronized void add(SourceObject ob) {
        m_obArray.add(ob);
    }

    public synchronized boolean remove(SourceObject ob) {
        boolean result = false;
          if(m_obArray.size()==0)
              return result;

        if (m_obArray.contains(ob)) {
            m_obArray.remove(ob);
            result = true;
        }
        return result;
    }


    public synchronized int size() {
        return m_obArray.size();
    }
}
