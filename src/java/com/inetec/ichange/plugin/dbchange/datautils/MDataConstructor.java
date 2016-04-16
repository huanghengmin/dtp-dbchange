
package com.inetec.ichange.plugin.dbchange.datautils;

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import com.inetec.common.io.ByteArrayBufferedInputStream;
import com.inetec.ichange.plugin.dbchange.datautils.db.Rows;
import com.inetec.ichange.plugin.dbchange.datautils.db.pk.PkSet;
import com.inetec.ichange.api.DataAttributes;


import java.io.*;
import java.util.Properties;
import java.util.Enumeration;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class MDataConstructor {

    public final static Logger m_logger = Logger.getLogger(MDataConstructor.class);

    private ArrayListInputStream mis = new ArrayListInputStream();
    private Rows m_basRows = null;
    private DefaultData m_defaultDate = null;
    private long m_position = 0;
    private DataHeaderAttributer m_header = new DataHeaderAttributer();
    private boolean m_bIsHeader = false;

    public MDataConstructor() {
        m_defaultDate = new DefaultData(new Properties());
    }

    public MDataConstructor(DataHeaderAttributer header, ArrayListInputStream in, Rows rows) {
        //m_header = header;
        mis = in;
        m_defaultDate = new DefaultData(new Properties());
        m_basRows = rows;
        m_bIsHeader = true;
    }

    public void setBasicData(String schemaName, String tableName, Rows basic) throws Ex {
        setBasicData(schemaName, tableName, basic, new Properties());
    }

    public void setSequenceData(String schemaName, String tableName, Rows basic) throws Ex {
        setSequenceData(schemaName, tableName, basic, new Properties());
    }

    public void setBasicData(String schemaName, String tableName, Rows basic, Properties props) throws Ex {

        m_basRows = basic;
        m_defaultDate = new DefaultData();
        // add heads
        m_defaultDate.setSchemaName(schemaName);
        m_defaultDate.setTableName(tableName);
        m_defaultDate.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Basic);
        //add headers Properps
        Enumeration keys = props.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            m_defaultDate.setHeadValue(key, props.getProperty(key));
        }

        // add content
        InputStream is = basic.getDataInputStream();
        try {
            int basicLen = is.available();
            m_defaultDate.setContentStream(is, basicLen);
            m_position = m_defaultDate.getHeaderLength() + basicLen;
            m_header.setBasicLength(m_defaultDate.getHeaderLength() + basicLen);
        } catch (IOException ioEEx) {
            throw new Ex().set(ioEEx);
        }
    }

    public void setSequenceData(String schemaName, String tableName, Rows basic, Properties props) throws Ex {

        m_basRows = basic;
        m_defaultDate = new DefaultData();
        // add heads
        m_defaultDate.setSchemaName(schemaName);
        m_defaultDate.setTableName(tableName);
        m_defaultDate.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Sequence);
        //add headers Properps
        Enumeration keys = props.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            m_defaultDate.setHeadValue(key, props.getProperty(key));
        }

        // add content
        InputStream is = basic.getDataInputStream();
        try {
            int basicLen = is.available();
            m_defaultDate.setContentStream(is, basicLen);
            m_position = m_defaultDate.getHeaderLength() + basicLen;
            m_header.setBasicLength(m_defaultDate.getHeaderLength() + basicLen);
        } catch (IOException ioEEx) {
            throw new Ex().set(ioEEx);
        }
    }

    public void addClobData(String schemaName, String tableName, String columnName,
                            String encoding, PkSet pks, Reader reader, long clobLen) throws Ex {
        if (reader != null) {
            ReaderInputStream inReader = new ReaderInputStream(reader, encoding);
            mis.addInputStream(getTwoLineInputStream());
            try {
                m_position = m_defaultDate.getDataStream().available() + mis.available();
            } catch (IOException e) {
                throw new Ex().set(E.E_IOException, new Message("Test Data Length error."));
            }

            CharLargeObjectData charLargeObjectData = new CharLargeObjectData();
            // add heads
            charLargeObjectData.setSchemaName(schemaName);
            charLargeObjectData.setTableName(tableName);
            charLargeObjectData.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Clob);
            charLargeObjectData.setHeadValue(DataInformation.Str_FieldName, columnName);
            charLargeObjectData.setHeadValue(DataInformation.Str_Data_Length, clobLen + "");
            charLargeObjectData.setHeadValue(DataInformation.Str_CharSet, encoding);
            charLargeObjectData.setHeadValue(DataInformation.Str_Pks, pks.getPkString());

            // add pks
            // charLargeObjectData.setPks(pks);

            // clob content
            DataAttributes temp = createStream(new ReaderInputStream(reader, DataInformation.Str_CharacterSet));
            clobLen = new Long(temp.getValue(DataAttributes.Str_FileSize)).longValue();
            charLargeObjectData.setContentStream(temp.getResultData(), clobLen);
            // charLargeObjectData.setContentStream(new ReaderInputStream(reader, "unicode"), clobLen);
            try {
                mis.addInputStream(charLargeObjectData.getDataStream());
                m_header.addLobHeader(m_position, clobLen + charLargeObjectData.getHeaderLength());
            } catch (IOException ioEEx) {
                throw new Ex().set(ioEEx);
            }
        }
    }

    public void addBlobData(String schemaName, String tableName, String columnName,
                            PkSet pks, InputStream is, long blobLen) throws Ex {
        if (is != null) {
            mis.addInputStream(getTwoLineInputStream());


            if (pks == null) {
                m_logger.debug("pkset==null");
            }
            try {
                m_position = m_defaultDate.getDataStream().available() + mis.available();
            } catch (IOException e) {
                throw new Ex().set(E.E_IOException, new Message("Test Data Length error."));
            }
            ByteLargeObjectData byteLargeObjectData = new ByteLargeObjectData();
            // add heads
            byteLargeObjectData.setSchemaName(schemaName);
            byteLargeObjectData.setTableName(tableName);
            byteLargeObjectData.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Blob);
            byteLargeObjectData.setHeadValue(DataInformation.Str_FieldName, columnName);
            byteLargeObjectData.setHeadValue(DataInformation.Str_Data_Length, blobLen + "");
            byteLargeObjectData.setHeadValue(DataInformation.Str_Pks, pks.getPkString());
            byteLargeObjectData.setContentStream(is, blobLen);
            try {
                mis.addInputStream(byteLargeObjectData.getDataStream());
                m_header.addLobHeader(m_position, blobLen + byteLargeObjectData.getHeaderLength());
            } catch (IOException ioEEx) {
                throw new Ex().set(ioEEx);
            }
        }
    }

    public InputStream getDataInputStream() throws Ex {
        ArrayListInputStream is = new ArrayListInputStream();
        try {

            is.addInputStream(m_header.headerToStream());
            if (!m_bIsHeader)
                addMutilDataVersion(is);
            //is.addInputStream(getTwoLineInputStream());
            if (m_defaultDate.getDataStream() != null)
                is.addInputStream(m_defaultDate.getDataStream());
            is.addInputStream(mis);
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, e, new Message("DBChange DataConstructor IOException."));
        }

        return is;
    }

    public InputStream getDataWhitoutBaseInputStream() throws Ex {

        return mis;
    }


    public DataHeaderAttributer getHeader() {
        return m_header;
    }

    public void updateHeader(DataHeaderAttributer header) {
        m_header.updateHeader(header);
    }

    private void addMutilDataVersion(ArrayListInputStream is) {
        StringBuffer version = new StringBuffer();
        version.append(MDataUtil.constructMultiDataVersion());
        version.append(MDataUtil.constructEmptyLine());
        version.append(MDataUtil.constructEmptyLine());
        StringReader versionReader = new StringReader(version.toString());
        ReaderInputStream versionIs = new ReaderInputStream(versionReader, DataInformation.Str_CharacterSet);
        is.addInputStream(versionIs);
    }


    private InputStream getTwoLineInputStream() throws Ex {
        String s = MDataUtil.Str_LineSeperator + MDataUtil.Str_LineSeperator;
        try {
            return new ByteArrayInputStream(s.getBytes(DataInformation.Str_CharacterSet));
        } catch (UnsupportedEncodingException e) {
            throw new Ex().set(e);
        }
    }

    public Rows getData() throws Ex {
        if (m_basRows == null) {
            throw new Ex().set(E.E_InvalidObject, new Message("Basic data is invalid."));
        }
        return m_basRows;
    }


    private DataAttributes createStream(InputStream is) throws Ex {
        try {
            ByteArrayOutputStream gzip = new ByteArrayOutputStream();
               IOUtils.copy(is, gzip);
            gzip.flush();
            DataAttributes result = new DataAttributes();
            result.putValue(DataAttributes.Str_FileSize, String.valueOf(gzip.size()));
            result.setResultData(gzip.toByteArray());
            gzip.close();
            return result;
        } catch (IOException Ex) {
            m_logger.error("IOException caught while creating  stream.", Ex);
            throw new Ex().set(E.E_OperationFailed, Ex, new Message("IOException caught while creating  stream."));
        }
    }

}
