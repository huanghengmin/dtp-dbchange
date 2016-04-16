package com.ichange.dbchange;

import com.inetec.unitest.UniTestCase;
import com.inetec.common.config.ConfigParser;
import com.inetec.common.config.nodes.IChange;
import com.inetec.common.db.DatabaseUtil;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DefaultDBTypeMapper;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle.DBType;
import junit.framework.TestCase;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 2006-2-23
 * Time: 23:13:00
 * To change this template use File | Settings | File Templates.
 */
public class TestDbType extends TestCase {


    public  TestDbType(String s) {
        super(s);
    }

    public void testDbType()throws Exception {
        ConfigParser parser = new ConfigParser("D:\\fartec\\ichange\\dtpdbchange\\utest\\resources\\config.xml");
        IChange ichange = parser.getRoot();
        DatabaseUtil dbutill = new DatabaseUtil();
        dbutill.config(ichange.getJdbc("target_sql2008"));
        DBType dbtype = DefaultDBTypeMapper.getDBType(dbutill.getConnection());
        System.out.print("dbType:"+dbtype.getStringType());

    }
}
