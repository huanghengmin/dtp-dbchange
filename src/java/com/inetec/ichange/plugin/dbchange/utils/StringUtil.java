package com.inetec.ichange.plugin.dbchange.utils;


import org.apache.log4j.Category;

public class StringUtil {

    protected static Category m_logger = Category.getInstance(StringUtil.class);

    public final static String S_PWD_ENCRYPT_CODE = "inetec~!@#$%^&*()_+";

    public static boolean eExist(String str, String sub) {
        int i = str.indexOf(sub);
        if (i != -1) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean eExist(String str, char c) {
        int i = str.indexOf(c);
        if (i != -1) {
            return true;
        } else {
            return false;
        }
    }

    public static String getLastSubString(String str, String sub) {
        int i = str.lastIndexOf(sub);
        if (i != -1) {
            return str.substring(i + sub.length());
        } else {
            return "";
        }
    }

    public static String getLastSubString(String str, char c) {
        int i = str.lastIndexOf(c);
        if (i != -1) {
            return str.substring(i + 1);
        } else {
            return "";
        }
    }


    public static String getFirstSubString(String str, String sub) {
        int i = str.lastIndexOf(sub);
        if (i != -1) {
            return str.substring(0, i);
        } else {
            return "";
        }
    }

    public static String getFirstSubString(String str, char c) {
        int i = str.lastIndexOf(c);
        if (i != -1) {
            return str.substring(0, i);
        } else {
            return "";
        }
    }

    public static String reverse(String str) {
        if (str == null || str.equalsIgnoreCase("")) {
            return "";
        }

        StringBuffer sb = new StringBuffer(str);
        return sb.reverse().toString();
    }

    public static String replace(String str, String pat, String val,
                                 int start) {
        if (val == null) {
            val = String.valueOf(val);        // "null"
        }

        // Look for pattern

        int indeEx = str.indexOf(pat, start);

        while (indeEx >= 0) {

            // Copy part before pattern, value and part after pattern

            StringBuffer buf = new StringBuffer(str.length());

            buf.append(str.substring(0, indeEx));
            buf.append(val);
            buf.append(str.substring(indeEx + pat.length()));

            str = buf.toString();

            // Look for pattern again, starting after value

            indeEx = str.indexOf(pat, indeEx + val.length());
        }

        return str;

    }        // End replace

}
