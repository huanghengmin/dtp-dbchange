package com.ichange.dbchange;

import com.inetec.common.config.ConfigParser;
import com.inetec.common.config.nodes.IChange;
import com.inetec.ichange.api.DataAttributes;
import com.inetec.ichange.plugin.dbchange.DbChangeSource;
import com.inetec.ichange.plugin.dbchange.DbChangeTarget;
import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 14-12-24.
 */
public class TestDbChange extends TestCase {

    ChangeMainImp main = new ChangeMainImp();
    DbChangeSource source = new DbChangeSource();
    DbChangeTarget target = new DbChangeTarget();
    ChangeTypeImp type;
    public static String testConfigPath = "D:\\fartec\\ichange\\dtp-dbchange\\utest\\resources\\config.xml";

    public void testDbChange() {
        ConfigParser parser = null;
        try {
//            System.setProperty("privatenetwork", "false");
            System.setProperty("ichange.home", "F:/ichange");
            parser = new ConfigParser(testConfigPath);
            IChange ichange = parser.getRoot();
            type = new ChangeTypeImp("db_1_1");
            main.setTargetPlugin(target);
            target.init(main, type, source);
            source.init(main, type, target);
            target.config(ichange);
            source.config(ichange);
            source.start(new DataAttributes());
            while (true){
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
