package com.inetec.ichange.plugin.dbchange.datautils;
import com.inetec.ichange.api.HeaderProcess;
import com.inetec.ichange.api.DataAttributes;
import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;

import java.io.*;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DataInformation {
    private static Logger m_logger = Logger.getLogger(DataInformation.class);
    public final static String Str_DataType = "Data_Type";
    public final static String Str_DataType_Basic = "Basic";
    public final static String Str_DataType_Sequence = "Sequence";
    public final static String Str_DataType_Clob = "Clob";
    public final static String Str_DataType_Blob = "Blob";
    public final static String Str_DataType_Success = "Success";

    public final static String Str_FieldName = "FieldName";

    public final static String Str_Data_Length = "Length";

    public final static String Str_CharSet = "charset";

    public final static String Str_Pks = "pks";

    public final static String Str_CharacterSet = "utf-8";

    protected Properties header = new Properties();
    protected InputStream m_in = null;
    protected long contentPosition = 0;
    protected byte[] m_buff = null;
    protected File m_file = null;

    public DataInformation(InputStream in, long size) throws Ex {
        m_in = in;
        header = new HeaderProcess().parseHeader(m_in);
        if (size > MDataParseImp.I_lobDataSize)
            oscach();
        else
            cach();
        m_in = getContentStream();
    }

    public Properties getHeader() {
        return header;
    }

    public String getDateType() {
        return (String) header.getProperty(Str_DataType);
    }

    public int getContentLength() {
        return new Integer(header.getProperty(Str_Data_Length)).intValue();
    }

    public long getContentPosition() {
        return contentPosition;
    }

    public InputStream getContentStream() throws Ex {
        InputStream result = null;
        if (m_buff != null) {
            result = new ByteArrayInputStream(m_buff);
        }
        if (m_file != null) {
            try {
                result = new FileInputStream(m_file);
            } catch (FileNotFoundException e) {
                throw new Ex().set(E.E_FileNotFound, new Message("����ݴ��ȡʧ��."));
            }
        }
        return result;
    }

    public boolean isBasicData() {
        return getDateType().equals(Str_DataType_Basic);
    }

    public boolean isSequenceData() {
        return getDateType().equals(Str_DataType_Sequence);
    }

    public boolean isBlobData() {
        return getDateType().equals(Str_DataType_Blob);
    }

    public boolean isClobData() {
        return getDateType().equals(Str_DataType_Clob);
    }

    public boolean isSuccessData() {
        return getDateType().equals(Str_DataType_Success);
    }

    public void cach() throws Ex {
        try {
            m_buff = DataAttributes.readInputStream(m_in);

            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Data is:" + new String(m_buff));
            }
            //System.out.println("Data is:"+new String(m_buff));
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, new Message("����ݴ����."));
        }
    }

    public void oscach() throws Ex {
        FileOutputStream out = null;
        BufferedOutputStream buff = null;
        byte[] bytebuff = null;
        try {
            m_file = File.createTempFile("#dbchange-datainfo", "tmp");
            out = new FileOutputStream(m_file);
            buff = new BufferedOutputStream(out);
            int bufflength = 0;
            if (m_in.available() > 1024)
                bytebuff = new byte[1024];
            else {
                bufflength = (int) m_in.available();
                bytebuff = new byte[bufflength];
            }
            int rc = 0;
            long count = 0;
            long temp = 0;
            rc = m_in.read(bytebuff);
            while (rc > 0) {
                count = count + rc;
                buff.write(bytebuff);

                if (m_in.available() < 1024) {
                    bufflength = (int) m_in.available();
                    bytebuff = new byte[bufflength];
                } else {
                    bytebuff = new byte[1024];
                }

                rc = m_in.read(bytebuff);
            }
            buff.flush();
            buff.close();
            out.close();
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, e);
        }
    }

    public static InputStream readInputStream(InputStream in, long size) throws IOException {
        ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
        int bufflength = 0;
        byte[] buff = null;
        if (size > 1024)
            buff = new byte[1024];
        else {
            bufflength = (int) size;
            buff = new byte[bufflength];
        }
        int rc = 0;
        long count = 0;
        long temp = 0;
        rc = in.read(buff);
        while (count < size && rc > 0) {
            count = count + rc;
            swapStream.write(buff,0,rc);
            temp = size - count;
            if (temp < 1024) {
                bufflength = (int) temp;
                buff = new byte[bufflength];
            }
            rc = in.read(buff);

        }
        return new ByteArrayInputStream(swapStream.toByteArray());
    }

    public void close() {
        if(m_in!=null){
            try {
                m_in.close();
            } catch (IOException e) {
                 // okay.
            }
        }
        if (m_file != null)
            m_file.delete();
        if (m_buff != null)
            m_buff = null;
    }

}
