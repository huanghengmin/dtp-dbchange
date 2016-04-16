/*=============================================================
 * �ļ�����: Value.java
 * ��    ��: 1.0
 * ��    ��: bluewind
 * ����ʱ��: 2005-11-12
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
package com.inetec.ichange.plugin.dbchange.datautils.db;


import java.io.*;

public class Value {

    public final static int Int_Value_Basic = 1;
    public final static int Int_Value_Blob = 2;
    public final static int Int_Value_Clob = 3;

    protected int type = Int_Value_Basic;
    protected boolean isnull = true;

    // 1. basic
    protected String value;

    // 2. blob
    protected InputStream blobStream = null;
    protected long blobStreamLen = 0;

    // 3. clob
    protected Reader clobReader = null;
    protected long clobReaderLen = 0;

    public Value(String value) {
        this.value = value;
        if (value == null) {
            isnull = true;
        } else {
            isnull = false;
        }
        type = Int_Value_Basic;
    }

    public Value(InputStream is, long len) {
        this.blobStream = is;
        blobStreamLen = len;
        if (is == null) {
            isnull = true;
        } else {
            isnull = false;
        }
        type = Int_Value_Blob;
    }


    public Value(Reader is, long len) {
        this.clobReader = is;
        clobReaderLen = len;
        if (is == null) {
            isnull = true;
        } else {
            isnull = false;
        }
        type = Int_Value_Clob;
    }

    public int getType() {
        return type;
    }

    public boolean isNull() {
        return isnull;
    }

    public String getValueString() {
        if (isnull) {
            return null;
        } else {
            return value;
        }
    }

    public InputStream getInputStream() {
        return blobStream;
    }

    public long getInputStreamLength() {
        return blobStreamLen;
    }

    public Reader getReader() {
        return clobReader;
    }

    public long getReaderLength() {
        return clobReaderLen;
    }

    public InputStream getBlobStream() {
        return new ByteArrayInputStream("".getBytes());
    }

    public Reader getClobReader() {
        try {
            return new InputStreamReader(new ByteArrayInputStream("".getBytes()), "GBK");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }

}

