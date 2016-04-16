/*=============================================================
 * �ļ�����: ReaderInputStream.java
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

import java.io.IOException;
import java.io.Reader;
import java.io.InputStream;

public class ReaderInputStream extends InputStream {

    protected Reader reader;
    private static final int CHAR_BUF_SIZE = 1024;
    private String charset = "";
    private char[] charBuffer = new char[CHAR_BUF_SIZE];
    private byte[] byteBuffer;
    private int byteBufferIndeEx = 0;
    private int byteBufferbufEnd = 0;

    private int total = 0;

    public ReaderInputStream(Reader reader) {
        this.reader = reader;
    }

    public ReaderInputStream(Reader reader, String charset) {
        this.reader = reader;
        this.charset = charset;
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
        if (byteBufferIndeEx == byteBufferbufEnd) {
            byteBufferIndeEx = 0;
            int charBufferbufEnd = reader.read(charBuffer, 0, charBuffer.length);
            if (charBufferbufEnd < 0) {
                byteBufferbufEnd = 0;
                return -1;
            } else {
                String stringBuffer = new String(charBuffer, 0, charBufferbufEnd);
                if (charset == null || charset.equals("")) {
                    byteBuffer = stringBuffer.getBytes();
                } else {
                    byteBuffer = stringBuffer.getBytes(charset);
                }
                byteBufferbufEnd = byteBuffer.length;
            }

            total += byteBufferbufEnd;

            /*
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("ReaderInputStream total readed:" + total);
            }
            */
        }

        return byteBuffer[byteBufferIndeEx++];
    }

}

