package com.inetec.ichange.plugin.dbchange.utils;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;

public class RBufferedInputStream extends InputStream {

    private InputStream curInputStream = null;
    private long curLength = 0;
    private int curIndeEx = 0;


    public RBufferedInputStream(InputStream is, long length) {
        this.curInputStream = is;
        this.curLength = length;
        curIndeEx = 0;
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
        if (curIndeEx == curLength) {
            return -1;
        }

        int b = curInputStream.read();
        if (b != -1) {
            curIndeEx++;
        }
        return b;

    }

    public int read(byte[] bytes) throws IOException {
        int result = 0;
        if (curIndeEx == curLength) {
            return -1;
        }
        if (bytes.length <= curLength)
            result = curInputStream.read(bytes);
        else {
            byte[] bytebuff = new byte[1];
            int value = curInputStream.read(bytebuff);
            while (value != -1 && curIndeEx < curLength) {
                bytes[curIndeEx] = bytebuff[0];
                result = curInputStream.read(bytebuff);
                curIndeEx++;
            }
            result = curIndeEx;
        }
        return result;
    }

    public void close() throws IOException {
        super.close();
        if (curInputStream != null) {
            curInputStream.close();
            curInputStream = null;
        }
        curLength = 0;
        curIndeEx = 0;
    }

}
