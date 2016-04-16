/*=============================================================
 * 文件名称: MDataUtil.java
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
