package com.ichange.dbchange;

import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import com.inetec.ichange.api.EStatus;
import com.inetec.ichange.plugin.dbchange.datautils.MDataParseImp;
import com.inetec.ichange.plugin.dbchange.datautils.DataInformation;
import com.inetec.ichange.api.DataAttributes;
import com.inetec.ichange.plugin.dbchange.source.info.TableInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2007-6-28
 * Time: 21:51:30
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    public void testDataParserByError() throws Exception {
        FileInputStream in = new FileInputStream("D:\\inetec\\ichange\\dbchange\\utest\\resources\\@ChangeData@_Data_1182931548562.recover");
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

    public static void main(String arg[]) throws Exception {
        Test test=new Test();
        test.testTimesync();
    }

    public void testTime(){
        System.out.println(new Timestamp(Long.parseLong("1289336871000")).toString());
        System.out.println(new Timestamp(Long.parseLong("1289336902000")).toString());
        System.out.println(new Timestamp(Long.parseLong("1289450173000")).toString());
        //
        System.out.println(new Timestamp(Long.parseLong("1278991697000")).toString());
        System.out.println(new Timestamp(Long.parseLong("1289450169000")).toString());
        System.out.println(new Timestamp(Long.parseLong("1289450168000")).toString());

        Properties  prosp=new Properties();
        try {
            prosp.load(new FileInputStream("c:/db1timesync.properties"));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        System.out.println("test:"+prosp.get("InitEndTime"));
        System.out.println(Timestamp.valueOf((String)prosp.get("InitEndTime")).toString());

        /*prosp.put("test",new Timestamp(Long.parseLong("1289450168000")).toString());
        try {
            prosp.store(new FileOutputStream("c:/props.properties"),"");
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }*/
        //System.out.println();
        //System.out.println(new Timestamp(Long.parseLong("1278924571000")).toString());
        //System.out.println(new Timestamp(Long.parseLong("1274284799000")).toString());
        ///System.out.println(Timestamp.valueOf("2010-07-13 11:28:16.0").toString());

        //System.out.println(Timestamp.valueOf("0").toString());
    }

    public void testTimesync(){
         //加载同步时间
        Properties timesyncProp = new Properties();
        //String ichangehome = System.getProperty("ichange.home");
        String propname = "c:/db1timesync.properties";
        File propfile = new File(propname);
        boolean isInittime = false;
        try {
            if (!propfile.exists()) {
                isInittime = true;
                propfile.createNewFile();
            }
            timesyncProp.load(new FileInputStream(propfile));

        } catch (IOException e) {
            //m_logger.warn(m_changeType.getType() + " type dbchange timesync operator init error!", e);
        }

        Timestamp begin = null;
        Timestamp end = null;

        String pkWhere = "";

       /* int tableSize = m_tableInfos.length;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("开始导出源表数据,Operator is TimeSync 源表个数为: " + tableSize);
        }
        try {
            testStatus();
        } catch (Ex ex) {
            m_logger.error(ex);
            return;
        }*/
        /*for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];

            String tableName = m_tableInfos[i].getName();
            if (isMergeTable(tableName)) {
                continue;
            }*/
            try {
                Timestamp initTime = new Timestamp(0);
                if (isInittime) {
                    //initTime = getTimeSyncInitDate(tableInfo.getTimeSyncTimeField().getName(), tableName);
                    if (initTime == null) {
                       // m_logger.warn("应用为：" + appType + "取到的时间标记为：0,请检查数据.");
                        return;
                    }
                } else {
                    initTime = Timestamp.valueOf(timesyncProp.getProperty("begintime"));
                }

                String begins = timesyncProp.getProperty("begintime", initTime.toString());
               /* if (m_logger.isDebugEnabled())
                    m_logger.info("开始时间标记：" + begins);*/
                end = new Timestamp(initTime.getTime() + (10 + 1) * 1000);
                String ends = timesyncProp.getProperty("endtime", end.toString());
                begin = Timestamp.valueOf(begins);
               /* if (m_logger.isDebugEnabled())
                    m_logger.info("结束时间标记：" + ends);*/
                end = Timestamp.valueOf(ends);
                String initEndTime = timesyncProp.getProperty("InitEndTime");
                long initEnd = Timestamp.valueOf(initEndTime).getTime();

                if (begin.getTime() == end.getTime()) {
                    if (initEnd >= end.getTime()) {
                        end = new Timestamp(end.getTime()+(10 + 1) * 1000);
                    } else {
                        System.out.println("updates");
                        //updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, 0);
                    }
                }


                pkWhere = timesyncProp.getProperty("pkwere", "");
                int rownum = Integer.parseInt(timesyncProp.getProperty("rownum", "0"));
                //exportWholeTableTimeSync(tableName, begin, end, pkWhere, rownum);
                timesyncProp.clear();
            }/* catch (Ex Ex) {
               *//* Message Message = new Message("导出源表 {0} Operator is TimeSync 数据出错.", tableName);
                m_logger.warn(Message.toString(), Ex);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), Ex);*//*
            } */catch (NullPointerException e) {
               /* Message Message = new Message("导出源表 {0} Operator is TimeSync 数据出错.", tableName);
                m_logger.warn(Message.toString(), e);
                m_log.setTableName(tableName);
                m_log.setStatusCode(EStatus.E_ExportDataFaild.getCode() + "");
                m_log.warn(Message.toString(), e);*/

            }
            //}
        //}

       /* if (m_logger.isDebugEnabled()) {
            m_logger.debug("完成源表数据的导出.");
        }*/
        return;
    }
}
