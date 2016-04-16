/*=============================================================
 * �ļ�����: ByteLargeObjectData.java
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

import com.inetec.common.exception.Ex;
import com.inetec.ichange.plugin.dbchange.utils.RBufferedInputStream;

import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;


public class ByteLargeObjectData extends NoSystemData {

    public ByteLargeObjectData() {
        header.put(DataInformation.Str_DataType, DataInformation.Str_DataType_Blob);
        setContentStream(null, 0);
    }

    public ByteLargeObjectData(DataInformation dataInformation) throws Ex {
        super(dataInformation);
    }

    public long getBlobLength() {
        return getContentLength();
    }

    public InputStream getBlobInputStream() throws IOException {
        return new RBufferedInputStream(getContentStream(), getBlobLength());
    }

    public InputStream getImageInputStream() throws IOException {
        InputStream is = getContentStream();
        int len = Integer.parseInt(header.getProperty("Length"));
        int avail = is.available();
        long rest = avail - len;
        if (rest >= 0) {
            //have another dataInfo
            byte[] bytes = new byte[len];
            int posi = 0;
            int len1 = len;
            while (len1 > 0) {
                int n = is.read(bytes, posi, len1);
                if (n > 0) {
                    posi += n;
                    len1 -= n;
                }
            }
            is.close();
            is = new ByteArrayInputStream(bytes);
        } else if (rest == 0) {
            //have no eExtra data; 'is' is okay
            // is = is;
        } else {
            throw new IOException("The specified length is less than data length.");
        }

        return is;
    }

}
