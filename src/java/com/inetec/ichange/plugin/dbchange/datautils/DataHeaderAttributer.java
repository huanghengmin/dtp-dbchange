/*=============================================================
 * �ļ�����: DataHeaderAttributer.java
 * ��    ��: 1.0
 * ��    ��: bluewind
 * ����ʱ��: 2005-11-17
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
