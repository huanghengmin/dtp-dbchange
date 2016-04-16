/*=============================================================
 * �ļ�����: ArrayListInputStream.java
 * ��    ��: 1.0
 * ��    ��: bluewind
 * ����ʱ��: 2005-10-17
 * ============================================================
 * <p>��Ȩ����  (c) 2005 ����������Ϣ�������޹�˾</p>
 * <p>
 * ��Դ���ļ���Ϊ����������Ϣ�������޹�˾���������һ���֣�������
 * �˱���˾�Ļ��ܺ�����Ȩ��Ϣ����ֻ�ṩ������˾���������û�ʹ�á�
 * </p>
 * <p>
 * ���ڱ����ʹ�ã��������ر�������˵���������������涨�����޺�
 * ������
 * </p>
 * <p>
 * �ر���Ҫָ�����ǣ������Դӱ���˾��������߸�����Ĳ��������ߺ�
 * ����ȡ�ò���Ȩʹ�ñ����򡣵��ǲ��ý��и��ƻ���ɢ�����ļ���Ҳ��
 * ��δ������˾����޸�ʹ�ñ��ļ������߽��л��ڱ�����Ŀ���������
 * ���ǽ������ķ����޶��ڶ����ַ�����˾��Ȩ����Ϊ�������ߡ�
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
