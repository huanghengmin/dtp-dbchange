/*=============================================================
 * �ļ�����: DbChangeSourceInfo.java
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
package com.inetec.ichange.plugin.dbchange.source.info;


import com.inetec.common.exception.Ex;
import com.inetec.common.config.nodes.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;
import java.util.Collection;

public class DbChangeSourceInfo {

    private ArrayList listDatabase = new ArrayList();

    public DbChangeSourceInfo(Plugin plugin, IChange ichange) throws Ex {
        DataBase database = plugin.getDataBase();
        if (database != null) {
            DatabaseInfo databaseInfo = new DatabaseInfo(database, ichange.getJdbc(database.getDbName()));
            listDatabase.add(databaseInfo);
        }

    }


    public DatabaseInfo[] getDatabases() {
        return (DatabaseInfo[]) listDatabase.toArray(new DatabaseInfo[0]);
    }

}
