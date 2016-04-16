/*=============================================================
 * 文件名称: DbChangeSourceInfo.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-10-17
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
