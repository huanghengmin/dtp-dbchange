package com.inetec.ichange.plugin.dbchange.datautils;

import com.inetec.common.exception.Ex;
import com.inetec.ichange.plugin.dbchange.utils.RBufferedInputStream;

import java.io.Reader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;

public class CharLargeObjectData extends NoSystemData {

    public CharLargeObjectData() {
        header.put(DataInformation.Str_DataType, DataInformation.Str_DataType_Clob);
        setContentStream(null, 0);
    }

    public CharLargeObjectData(DataInformation dataInformation) throws Ex {
        super(dataInformation);
    }

    public String getCharset() {
        return getHeadValue(DataInformation.Str_CharSet);
    }

    public int getClobLength() {
        return new Integer(getHeadValue(DataInformation.Str_Data_Length)).intValue();
    }


    public Reader getClobReader() throws IOException {
        InputStream is = new RBufferedInputStream(getContentStream(), getClobLength());
        String charset = getCharset();
        if (charset == null || charset.equals("")) {
            return new InputStreamReader(is);
        } else {
            return new InputStreamReader(is, charset);
        }
    }

    public InputStream getClobInputStream() throws IOException {
        return new RBufferedInputStream(getContentStream(), getClobLength());
    }

}
