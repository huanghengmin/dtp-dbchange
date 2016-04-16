/*=============================================================
 * �ļ�����: PkSet.java
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

package com.inetec.ichange.plugin.dbchange.datautils.db.pk;

import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;
import com.inetec.ichange.plugin.dbchange.datautils.db.Row;
import com.inetec.ichange.plugin.dbchange.datautils.db.Value;

import java.util.ArrayList;


public class PkSet {

    private String m_pkString;
    private Column[] m_columns;

    public final static String Str_Escape = "\\";
    public final static String Str_Unit_Seperator = ",";
    public final static String Str_Part_Seperator = ";";

    public final static char C_Escape = '\\';
    public final static char C_Unit_Seperator = ',';
    public final static char C_Part_Seperator = ';';


    public PkSet(String str) throws Ex {
        m_pkString = str;
        parse();
    }

    public Column[] getPkArray() throws Ex {
        if (m_columns == null) {
            throw new Ex().set(E.E_NullPointer, new Message("the pks string is not parsed successfully!"));
        }
        return m_columns;
    }

    public String getPkString() {
        return m_pkString;
    }

    public boolean isPk(String name) throws Ex {
        if (m_columns == null) {
            throw new Ex().set(E.E_NullPointer, new Message("the pks string is not parsed successfully!"));
        }

        for (int i = 0; i < m_columns.length; i++) {
            if (m_columns[i].getName().equalsIgnoreCase(name)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Descriptions: the m_pkString format is like that:  name1,name2,nam3;type1,type2,type3;value1,value2,value3;
     */
    private void parse() throws Ex {
        if (m_pkString == null || m_pkString.equals("")) {
            throw new Ex().set(E.E_FormatError, new Message("the pks string in todo table is empty"));
        }

        int partNumber = 0;
        ArrayList listUnit = new ArrayList();
        StringBuffer sbUnit = new StringBuffer();
        for (int i = 0; i < m_pkString.length(); i++) {
            char first = m_pkString.charAt(i);
            if (first == C_Escape) {
                if (i == m_pkString.length()) {
                    throw new Ex().set(E.E_FormatError, new Message("the last char should not be a escape char!"));
                }
                char second = m_pkString.charAt(i + 1);
                if (second == C_Unit_Seperator || second == C_Part_Seperator || second == C_Escape) {
                    sbUnit.append(second);
                    i++;
                } else {
                    throw new Ex().set(E.E_FormatError, new Message("the pks string is not correct after a escape char!"));
                }
            } else if (first == C_Part_Seperator) {
                listUnit.add(sbUnit.toString());
                partNumber++;
                sbUnit.delete(0, sbUnit.length());
            } else if (first == C_Unit_Seperator) {
                listUnit.add(sbUnit.toString());
                sbUnit.delete(0, sbUnit.length());
            } else {
                sbUnit.append(first);
            }
        }

        if (partNumber != 3) {
            throw new Ex().set(E.E_FormatError, new Message("the pks string has not three part!"));
        }

        int count = listUnit.size();
        if (count % partNumber != 0) {
            throw new Ex().set(E.E_FormatError, new Message("the pks string is not complete!"));
        }
        int unitNumber = count / partNumber;
        m_columns = new Column[unitNumber];
        for (int i = 0; i < unitNumber; i++) {
            String name = (String) listUnit.get(i);
            String type = (String) listUnit.get(unitNumber + i);
            String value = (String) listUnit.get(unitNumber * 2 + i);
            Column column = new Column(name, type, true);
            column.setValue(new Value(value));
            m_columns[i] = column;
        }
    }

    public static String getPks(Row row) {
        Column[] cs = row.getColumnArray();
        ArrayList listPk = new ArrayList();
        for (int i = 0; i < cs.length; i++) {
            if (cs[i].isPk()) {
                listPk.add(cs[i]);
            }
        }

        return getPks((Column[]) listPk.toArray(new Column[0]));

    }

    public static String getPks(Column[] pkColumns) {
        int size = pkColumns.length;
        String[] names = new String[size];
        String[] types = new String[size];
        String[] values = new String[size];
        for (int i = 0; i < size; i++) {
            Column c = pkColumns[i];
            names[i] = c.getName();
            types[i] = c.getJdbcTypeString();
            values[i] = c.getValue().getValueString();
        }

        StringBuffer result = new StringBuffer();
        for (int i = 0; i < size; i++) {
            String name = names[i];
            result.append(format(name));
            if (i == size - 1) {
                result.append(C_Part_Seperator);
            } else {
                result.append(C_Unit_Seperator);
            }
        }
        for (int i = 0; i < size; i++) {
            String type = types[i];
            result.append(format(type));
            if (i == size - 1) {
                result.append(C_Part_Seperator);
            } else {
                result.append(C_Unit_Seperator);
            }
        }
        for (int i = 0; i < size; i++) {
            String value = values[i];
            result.append(format(value));
            if (i == size - 1) {
                result.append(C_Part_Seperator);
            } else {
                result.append(C_Unit_Seperator);
            }
        }

        return result.toString();
    }

    public static String format(String s) {

        if (s == null || s.equalsIgnoreCase("")) {
            return "";
        }

        StringBuffer result = new StringBuffer();
        int size = s.length();
        for (int i = 0; i < size; i++) {
            char c = s.charAt(i);
            switch (c) {
                case C_Escape:
                case C_Unit_Seperator:
                case C_Part_Seperator:
                    result.append(C_Escape);
                    break;
            }
            result.append(c);
        }

        return result.toString();
    }
   public boolean equals(Object o){
       PkSet pkset = (PkSet)o;
       return m_pkString.equals(pkset.getPkString());
   }
}
