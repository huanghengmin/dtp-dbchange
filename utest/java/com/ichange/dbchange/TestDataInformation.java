package com.ichange.dbchange;

import com.inetec.unitest.UniTestCase;
import com.inetec.ichange.plugin.dbchange.datautils.DataInformation;
import com.inetec.ichange.plugin.dbchange.datautils.MDataParseImp;
import com.inetec.ichange.plugin.dbchange.datautils.MDataConstructor;
import com.inetec.ichange.plugin.dbchange.datautils.ArrayListInputStream;
import com.inetec.ichange.plugin.dbchange.datautils.db.pk.PkSet;
import com.inetec.ichange.plugin.dbchange.datautils.db.Rows;
import com.inetec.ichange.plugin.dbchange.datautils.db.Row;
import com.inetec.ichange.plugin.dbchange.utils.ReaderUtil;
import com.inetec.ichange.api.DataAttributes;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-12-5
 * Time: 21:14:05
 * To change this template use File | Settings | File Templates.
 */
public class TestDataInformation extends UniTestCase {

    public TestDataInformation(String s) {
        super(s);
    }

    public void testDataParser() throws Exception {
        FileInputStream in = new FileInputStream("F:/ichange/data/test_s.tmp");
        MDataParseImp parse = new MDataParseImp(in);

        FileOutputStream basic = new FileOutputStream("F:/ichange/data/test_s_1.tmp");

        basic.write(DataAttributes.readInputStream(parse.getBasicDataInfo().getContentStream()));
        basic.close();
        DataInformation[] lobs = parse.getBlobDataInfo();
        String lobfile = "c:\\lob-";
        for (int i = 0; i < lobs.length; i++) {
            FileOutputStream lobdata = new FileOutputStream(lobfile + i);
            lobdata.write(DataAttributes.readInputStream(lobs[i].getContentStream()));
            lobdata.close();
        }
        DataInformation[] clobs = parse.getClobDataInfo();
        String clobsFile = "c:\\clob-";
        for (int i = 0; i < lobs.length; i++) {
            FileOutputStream lobdata = new FileOutputStream(clobsFile + i);
            lobdata.write(DataAttributes.readInputStream(clobs[i].getContentStream()));
            lobdata.close();
        }
    }

    public void testDataConstructor() throws Exception {
        MDataConstructor mdata = new MDataConstructor();
        MDataConstructor mdata2 = new MDataConstructor();
        FileReader reader = new FileReader("c:\\data2.data");

        mdata.addBlobData("testdb", "testTable", "testField", new PkSet("ss;String;varchar2;ss2"), new FileInputStream("c:\\data1.data"), 12556);
        mdata.addClobData("testdb", "testTable", "testField", "gb2312", new PkSet("ss;String;varchar2;ss6"), reader, 1399);
        reader = new FileReader("c:\\data2.data");
        mdata.addBlobData("testdb", "testTable", "testField", new PkSet("ss;String;varchar2;ss4"), new FileInputStream("c:\\data1.data"), 12556);
        mdata.addClobData("testdb", "testTable", "testField", "gb2312", new PkSet("ss;String;varchar2;ss5"), reader, 1399);

        Rows basic = new Rows();
        mdata2.setBasicData("testdb", "testTable", basic);
        mdata2.updateHeader(mdata.getHeader());
        ArrayListInputStream is = new ArrayListInputStream();
        is.addInputStream(mdata2.getDataInputStream());
        is.addInputStream(mdata.getDataWhitoutBaseInputStream());
        MDataConstructor mdata3 = new MDataConstructor(mdata2.getHeader(), is, basic);
        FileOutputStream out = new FileOutputStream("c:\\result.data");
        out.write(DataAttributes.readInputStream(mdata3.getDataInputStream()));
        out.close();

    }

    public void testReaderUtil() throws Exception {
        FileReader reader = new FileReader("c:\\clob-0");
        ReaderUtil readerUtil = new ReaderUtil(reader);
        System.out.print("reader Length:" + readerUtil.getLength());
        FileWriter writer = new FileWriter("c:\\resylt_clob.data");
        Reader tempReader = readerUtil.getReader();
        char [] cha = new char[1024];
        int i=0;
        while ((i=tempReader.read(cha))>0) {
            writer.write(cha);
        }
        writer.flush();
    }

    public void testDataParserByError() throws Exception {
        FileInputStream in = new FileInputStream("D:\\fartec\\ichange\\dtpdbchange\\utest\\resources\\@ChangeData@_Data_1389687075303");
        MDataParseImp parse = new MDataParseImp(in);
        FileOutputStream basic = new FileOutputStream("c:\\basic.data");
        basic.write(DataAttributes.readInputStream(parse.getBasicDataInfo().getContentStream()));
        basic.close();
        DataInformation[] lobs = parse.getBlobDataInfo();
        String lobfile = "c:\\lob-";
        for (int i = 0; i < lobs.length; i++) {
            FileOutputStream lobdata = new FileOutputStream(lobfile + i);
            lobdata.write(DataAttributes.readInputStream(lobs[i].getContentStream()));
            lobdata.close();
        }
        DataInformation[] clobs = parse.getClobDataInfo();
        String clobsFile = "c:\\clob-";
        for (int i = 0; i < lobs.length; i++) {
            FileOutputStream lobdata = new FileOutputStream(clobsFile + i);
            lobdata.write(DataAttributes.readInputStream(clobs[i].getContentStream()));
            lobdata.close();
        }
    }

}
