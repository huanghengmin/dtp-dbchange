/*=============================================================
 * 文件名称: DataHeaderAttributer.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-11-17
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

import java.util.Properties;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

public class DataHeaderAttributer {
    private Logger m_log = Logger.getLogger(DataHeaderAttributer.class);
    public static String Str_BasicDataLength = "BasicLength";
    public static String Str_LobDataCount = "LobDataCount";
    public static String Str_LobDataLength = "LobDataLength";
    public static String Str_LobDataPosition = "LobDataPosition";
    private int m_count = 0;
    private int m_size = 0;
    private Properties m_props = new Properties();

    public DataHeaderAttributer() {

    }

    public void setBasicLength(int length) {
        m_props.setProperty(Str_BasicDataLength, String.valueOf(length));
    }

    public void addLobHeader(long postion, long length) {
        m_size++;
        m_props.setProperty(Str_LobDataCount, m_size + "");
        m_props.setProperty(Str_LobDataPosition + "-" + m_size, String.valueOf(postion));
        m_props.setProperty(Str_LobDataLength + "-" + m_size, String.valueOf(length));
    }

    public void load(Properties props) throws Ex {
        m_props.clear();
        m_props = props;
        String version = m_props.getProperty("Version");
        if (version == null || version.equals("")) {
            throw new Ex().set(E.E_VersionError, new Message("DbChange Data Version error."));
        }
        m_size = 0;
        String temp = props.getProperty(Str_LobDataCount);
        if (temp == null || temp.equals("")) {
            m_size = 0;
        } else {
            temp = temp.trim();
            try {
                m_size = new Integer(temp).intValue();
            } catch (NumberFormatException e) {
                throw new Ex().set(E.E_FormatError, new Message("DataHeader LobDataCount format error."));
            }
        }
    }

    public long getLobDataLength() throws Ex {
        long length = 0;
        String temp = m_props.getProperty(Str_LobDataLength + "-" + m_count);
        if (temp == null || temp.equals("")) {
            length = 0;
        } else {
            temp = temp.trim();
            try {
                length = new Long(temp).longValue();
            } catch (NumberFormatException e) {
                throw new Ex().set(E.E_FormatError, new Message("DataHeader LobDataLength format error."));
            }
        }
        return length;
    }

    public long getLobDataPosition() throws Ex {
        long length = 0;
        String temp = m_props.getProperty(Str_LobDataPosition + "-" + m_count);
        if (temp == null || temp.equals("")) {
            length = 0;
        } else {
            temp = temp.trim();
            try {
                length = new Long(temp).longValue();
            } catch (NumberFormatException e) {
                throw new Ex().set(E.E_FormatError, new Message("DataHeader LobDataPosition  format error."));
            }
        }
        return length;
    }

    public long getBasicLength() throws Ex {
        long length = 0;
        String temp = m_props.getProperty(Str_BasicDataLength);
        if (temp == null || temp.equals("")) {
            length = 0;
        } else {
            temp = temp.trim();
            try {
                length = new Long(temp).longValue();
            } catch (NumberFormatException e) {
                throw new Ex().set(E.E_FormatError, new Message("DataHeader BasicLegth  format error.{0}", temp));
            }
        }
        return length;
    }

    public void updateHeader(DataHeaderAttributer header){
        String basicLength = m_props.getProperty(Str_BasicDataLength);
        if (basicLength == null) {
            basicLength = "0";
        }
        Properties props = header.getProperties();
        long basicPostion = 0;
        basicPostion = new Long(basicLength).longValue();
        String position = "";
        long lobPostion = 0;
        String length = "";
        for (int i = 1; i <= header.getLobSize(); i++) {
            position = props.getProperty(Str_LobDataPosition + "-" + i);
            lobPostion = new Long(position).longValue() + basicPostion;
            position = String.valueOf(lobPostion);
            length = props.getProperty(Str_LobDataLength + "-" + i);
            updateLobHeader(i, position, length);
        }
        m_props.setProperty(Str_LobDataCount, String.valueOf(header.getLobSize()));
    }
    private void updateLobHeader(int index,String postion, String length) {
        m_props.setProperty(Str_LobDataPosition + "-" +index,postion);
        m_props.setProperty(Str_LobDataLength + "-" + index,length);
    }

    public Properties getProperties(){
        return m_props;
    }
    public int getLobSize(){
        return m_size;
    }
    public boolean isNext() {
        m_count++;
        if (m_count > m_size) {
            return false;
        } else {
            return true;
        }
    }

    public InputStream headerToStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        m_props.store(out, "");
        return new ByteArrayInputStream(out.toByteArray());
    }
}
