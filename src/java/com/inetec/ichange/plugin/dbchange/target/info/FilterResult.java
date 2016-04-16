package com.inetec.ichange.plugin.dbchange.target.info;

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class FilterResult {

    public FilterResult(int n) {
        result = new boolean[n];
        for (int i = 0; i < n; i++) {
            result[i] = true;           // default to true.
        }
    }

    public boolean getResult(int i) throws Ex {
        if (i > result.length) {
            throw new Ex().set(E.E_Unknown, new Message("Index is out of bound while getting the filter result; i={0}, bound={1}.", "" + i, "" + result.length));
        }
        return result[i];
    }

    public void setIth(int i, boolean b) throws Ex {
        if (i > result.length) {
            throw new Ex().set(E.E_Unknown, new Message("Index is out of bound while setting the filter result; i={0}, bound={1}.", "" + i, "" + result.length));
        }
        result[i] = b;
    }

    public int getResultSize() throws Ex {
        if (result != null)
            return result.length;
        else
            return 0;
    }

    boolean[] result = null;

}
