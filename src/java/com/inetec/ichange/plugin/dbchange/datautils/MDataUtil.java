/*=============================================================
 * �ļ�����: MDataUtil.java
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

import java.io.*;
import java.util.Vector;


public class MDataUtil {


    public final static String Str_HeaderSeperator = "=";
    public final static String Str_LineSeperator = "\n";
    public final static char Char_HeaderSeperator = '=';
    public final static char Char_LineSeperator = '\n';

    public final static String Str_TwoLineSeperator = "\n\n";

    public final static String Str_MultiDataVersion = "Version=IChangeData 1.0";


    public static String constructMultiDataVersion() {
        return Str_MultiDataVersion;
    }

    public static String constructLine(String name, String value) {
        return name + Str_HeaderSeperator + value + Str_LineSeperator;
    }

    public static String constructEmptyLine() {
        return Str_LineSeperator;
    }

    public static void appendWithSeperator(OutputStream os, InputStream is) throws IOException {
        os.write(Str_TwoLineSeperator.getBytes());
        append(os, is);
    }

    public static void appendWithSeperator(OutputStream os, InputStream is, long length) throws IOException {
        os.write(Str_TwoLineSeperator.getBytes());
        append(os, is, length);
    }

    public static void append(OutputStream os, InputStream is, long length) throws IOException {
        long count = 0;
        while (count < length + 1) {
            int v = is.read();
            if (v != -1) {
                os.write(v);
            } else {
                break;
            }
            count++;
        }
    }

    public static void append(OutputStream os, InputStream is) throws IOException {
        while (true) {
            int v = is.read();
            if (v != -1) {
                os.write(v);
            } else {
                break;
            }
        }
    }

    public static int getReaderlength(Reader reader) throws Ex {
        int length = 0;
        try {
            Vector vResult = new Vector();
            BufferedReader bufferedReader = new BufferedReader(reader);

            String line;
            do {
                line = bufferedReader.readLine();
                if (line == null) {
                    break;
                } else {
                    vResult.addElement(line);
                }
            } while (true);
            reader.close();
            String result = new String(vResult.toString().getBytes(), "utf-8");
            length = result.length();
            return length;
        } catch (Exception e) {
            throw new Ex().set(e);
        }
    }

    public static InputStream getReaderAsInputStream(Reader reader) throws Ex {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            Writer writer = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));
            int c;
            while ((c = reader.read()) != -1) {
                writer.write(c);
            }
            reader.close();
            writer.flush();
            ByteArrayInputStream fis = new ByteArrayInputStream(os.toByteArray());
            writer.close();
            os.close();
            return fis;
        } catch (Exception e) {
            throw new Ex().set(e);
        }

    }


}
