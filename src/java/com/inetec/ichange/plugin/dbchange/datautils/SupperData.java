/*=============================================================
 * �ļ�����: SupperData.java
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
package com.inetec.ichange.plugin.dbchange.datautils;

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.db.pk.PkSet;

import java.io.*;
import java.util.Properties;
import java.util.Iterator;
import java.util.Set;


public class SupperData {
    public static final String Str_DataBaseName = "DataBaseName";
    public static final String Str_TableName = "TableName";
    protected Properties header = new Properties();
    protected PkSet m_pks = null;
    private InputStream m_is = null;
    protected String filename = "";

    public SupperData() {
    }

    public SupperData(DataInformation dataInformation) throws Ex {
        try {
            header.putAll(dataInformation.getHeader());
            m_is = dataInformation.getContentStream();
        } catch (Exception eEx) {
            throw new Ex().set(eEx);
        }
    }

    public long getContentLength() {
        return new Integer(getHeadValue("Length")).intValue();
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public InputStream getContentStream() {
        return m_is;
    }

    public void setContentStream(InputStream is, long len) {
        if (m_is != null) {
            try {
                m_is.close();
            } catch (IOException e) {
                // okay.
            }
        }
        m_is = is;
        header.put("Length", len + "");
    }

    public void setContentStream(InputStream is) {
        if (m_is != null) {
            try {
                m_is.close();
            } catch (IOException e) {
                // okay.
            }
        }
        m_is = is;
    }

    public void setPks(PkSet pks) {
        this.m_pks = pks;
    }

    public Column[] getPkColumn() throws Ex {
        return m_pks.getPkArray();
    }

    public String getLobPkString() {
        return header.getProperty(DataInformation.Str_Pks);
    }

    public InputStream getDataStream() throws IOException {

        StringBuffer result = new StringBuffer();
        Set keySet = header.keySet();
        Iterator it = keySet.iterator();
        while (it.hasNext()) {
            String name = (String) it.next();
            String value = header.getProperty(name);
            result.append(MDataUtil.constructLine(name, value));
        }
        InputStream headerIs;
        if (result.length() > 0) {
            result.append(MDataUtil.constructEmptyLine());

        }
        headerIs = new ByteArrayInputStream(result.toString().getBytes(DataInformation.Str_CharacterSet));
        ArrayListInputStream mis = new ArrayListInputStream();
        if (m_is == null) {
            return headerIs;
        } else {
            mis.addInputStream(headerIs);
            mis.addInputStream(m_is);
            return mis;
        }

    }

    public String getHeadValue(String name) {
        return header.getProperty(name);
    }

    protected void setHeadValue(String name, String value) {
        header.put(name, value);
    }

    public void close() {
        if (m_is != null) {
            try {
                m_is.close();
            } catch (IOException e) {
                // okay.
            }
        }
    }

    public int getHeaderLength() throws Ex {
        StringBuffer result = new StringBuffer();
        Set keySet = header.keySet();
        Iterator it = keySet.iterator();
        while (it.hasNext()) {
            String name = (String) it.next();
            String value = header.getProperty(name);
            result.append(MDataUtil.constructLine(name, value));
        }
        result.append(MDataUtil.constructEmptyLine());
        int size = 0;
        try {
            size = result.toString().getBytes(DataInformation.Str_CharacterSet).length;
        } catch (UnsupportedEncodingException e) {
            throw new Ex().set(E.E_FormatError, new Message("ת���������."));
        }
        return size;
    }

}
