/*=============================================================
 * 文件名称: ArrayListInputStream.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-10-17
 * ============================================================
 * <p>版权所有  (c) 2005 杭州网科信息工程有限公司</p>
 * <p>
 * 本源码文件作为杭州网科信息工程有限公司所开发软件一部分，它包涵
 * 了本公司的机密很所有权信息，它只提供给本公司软件的许可用户使用。
 * </p>
 * <p>
 * 对于本软件使用，必须遵守本软件许可说明和限制条款所规定的期限和
 * 条件。
 * </p>
 * <p>
 * 特别需要指出的是，您可以从本公司软件，或者该软件的部件，或者合
 * 作商取得并有权使用本程序。但是不得进行复制或者散发本文件，也不
 * 得未经本公司许可修改使用本文件，或者进行基于本程序的开发，否则
 * 我们将在最大的法律限度内对您侵犯本公司版权的行为进行起诉。
 * </p>
 * ==========================================================*/
package com.inetec.ichange.plugin.dbchange.datautils;

import java.util.ArrayList;
import java.io.InputStream;
import java.io.IOException;

public class ArrayListInputStream extends InputStream {

    private ArrayList isList = new ArrayList();
    private InputStream curInputStream = null;
    private int indeEx = 0;
    private boolean end = false;


    public void addInputStream(InputStream is) {
        isList.add(is);
    }

    /**
     * Reads the next byte of data from this source stream. The value
     * byte is returned as an <code>int</code> in the range
     * <code>0</code> to <code>255</code>. If no byte is available
     * because the end of the stream has been reached, the value
     * <code>-1</code> is returned.
     * <p/>
     * This <code>read</code> method
     * cannot block.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     *         stream has been reached.
     */
    public synchronized int read() throws IOException {
        if (end) {
            return -1;
        }

        if (curInputStream == null && isList.size() > 0) {
            curInputStream = (InputStream) isList.get(0);
            indeEx = 0;
        }
        if (curInputStream == null) {
            return -1;
        }
        int value = curInputStream.read();
        if (value == -1) {
            if (indeEx == isList.size() - 1) {
                end = true;
            } else {
                curInputStream = (InputStream) isList.get(++indeEx);
                value = curInputStream.read();
                // todo: value == -1
            }
        }

        return value;

    }

    public int available() throws IOException {
        int size = 0;
        for (int i = 0; i < isList.size(); i++) {
            InputStream temp = (InputStream) isList.get(i);
            size = size + temp.available();
        }
        return size;
    }

    public void close() throws IOException {
        super.close();
        for (int i = 0; i < isList.size(); i++) {
            InputStream temp = (InputStream) isList.get(i);
            if (temp != null) {
                temp.close();

            }
            temp = null;
            isList.remove(i);
        }
        isList.clear();
        isList = new ArrayList();
        indeEx = 0;
        end = false;
        if (curInputStream != null) {
            curInputStream.close();
            curInputStream = null;
        }
    }

}
