/*=============================================================
 * �ļ�����: SuccessData.java
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
import com.inetec.common.exception.E;

import java.util.Date;
import java.io.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class SuccessData extends SupperData {
    private static final String Str_ProcessTime="ProcessTime";
    private static final String Str_TargetDataBase="TargetDataBase";
    public SuccessData(DataInformation dataInformation) throws Ex {
        super(dataInformation);
    }

    public SuccessData(String schema) throws Ex {
        header.put(DataInformation.Str_DataType, DataInformation.Str_DataType_Success);
        addFailedInfo(schema);
    }

    public String getTargetSchema() {
        return getHeadValue(Str_TargetDataBase);
    }

    public Date getLatestDate() throws Ex {
        try {
            return new SimpleDateFormat().parse(getHeadValue(Str_ProcessTime));
        } catch (ParseException pe) {
            throw new Ex().set(E.E_FormatError, pe);
        }
    }


    public void addFailedInfo(String schema) throws Ex {
        setHeadValue(Str_TargetDataBase, schema);
        Date processTime = new Date(System.currentTimeMillis());
        setHeadValue(Str_ProcessTime,processTime.toString());
    }

}
