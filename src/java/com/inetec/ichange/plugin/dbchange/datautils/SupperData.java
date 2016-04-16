/*=============================================================
 * 文件名称: SupperData.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-11-12
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
            throw new Ex().set(E.E_FormatError, new Message("转换编码出错."));
        }
        return size;
    }

}
