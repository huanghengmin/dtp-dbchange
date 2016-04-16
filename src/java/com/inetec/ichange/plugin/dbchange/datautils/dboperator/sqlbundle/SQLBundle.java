/*=============================================================
 * 文件名称: SQLBundle.java
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
package com.inetec.ichange.plugin.dbchange.datautils.dboperator.sqlbundle;

import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.util.Enumeration;
import java.util.Locale;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class SQLBundle extends ResourceBundle {

/*-----------------------------*/
/*--- Static access methods ---*/
/*-----------------------------*/

    /**
     * Returns a SQLResourceBundle, this could be either the db specific bundle or if that is not a available
     * and there is a base SQL bundle available then the base bundle will be returned. If neither is available
     * a {@link java.util.MissingResourceException} is thrown.
     *
     * @param dbType     a string representing the database type.
     * @param bundleName the name of the resource bundle. Similar to baseName in ResourceBundle.
     * @return the SQLResourceBundle
     * @deprecated Use of the string-based db type methods is deprecated programs using this class should
     *             use the {@link com.inetec.common.db.sqlbundle.DBType} based methods.
     */
    public static SQLBundle getSQLBundle(String dbType, String bundleName) throws MissingResourceException {
        try {
            return new SQLBundle(dbType, bundleName);
        } catch (MissingResourceException e) {
            return new SQLBundle(bundleName);
        }
    }

    /**
     * Returns a SQL string, this could be either the db specific SQL or if that is not a available
     * and there is a base SQL bundle available then the base SQL will be returned. If neither is available
     * a {@link MissingResourceException} is thrown.
     *
     * @param dbType     a string representing the database type.
     * @param bundleName the name of the resource bundle. Similar to baseName in ResourceBundle.
     * @param key        the key to lookup in the bundle.
     * @return a {@link String} containing the desired SQL.
     * @deprecated Use of the string-based db type methods is deprecated programs using this class should
     *             use the {@link com.inetec.common.db.sqlbundle.DBType} based methods.
     */
    public static String getSQL(String dbType, String bundleName, String key) throws MissingResourceException {
        String RBName = bundleName + "-" + dbType;
        try {
            ResourceBundle sql_bundle = ResourceBundle.getBundle(RBName);
            return sql_bundle.getString(key);
        } catch (MissingResourceException e) {
            ResourceBundle sql_bundle = ResourceBundle.getBundle(bundleName);
            return sql_bundle.getString(key);
        }
    }

    /**
     * Returns a SQLResourceBundle, this could be either the db specific bundle or if that is not a available
     * and there is a base SQL bundle available then the base bundle will be returned. If neither is available
     * a {@link MissingResourceException} is thrown.
     *
     * @param dbType     a DBType object for the desired database.
     * @param bundleName the name of the resource bundle. Similar to baseName in ResourceBundle.
     * @return the SQLResourceBundle
     */
    public static SQLBundle getSQLBundle(DBType dbType, String bundleName) throws MissingResourceException {
        try {
            return new SQLBundle(dbType, bundleName);
        } catch (MissingResourceException e) {
            return new SQLBundle(bundleName);
        }
    }

    /**
     * Returns a SQL string, this could be either the db specific SQL or if that is not a available
     * and there is a base SQL bundle available then the base SQL will be returned. If neither is available
     * a {@link MissingResourceException} is thrown.
     *
     * @param dbType     a DBType object for the desired database.
     * @param bundleName the name of the resource bundle. Similar to baseName in ResourceBundle.
     * @param key        the key to lookup in the bundle.
     * @return a {@link String} containing the desired SQL.
     */
    public static String getSQL(DBType dbType, String bundleName, String key) throws MissingResourceException {
        String RBName = bundleName + "-" + dbType.getStringType();
        try {
            ResourceBundle sql_bundle = ResourceBundle.getBundle(RBName);
            return sql_bundle.getString(key);
        } catch (MissingResourceException e) {
            ResourceBundle sql_bundle = ResourceBundle.getBundle(bundleName);
            return sql_bundle.getString(key);
        }
    }

/*-----------------------------*/
/*--- Bundle implementation ---*/
/*-----------------------------*/

    private ResourceBundle m_sql_bundle;
    private ResourceBundle m_parent_bundle;

    private SQLBundle(String dbType, String bundleName) throws MissingResourceException {
        String RBName = bundleName + "-" + dbType;

        m_sql_bundle = ResourceBundle.getBundle(RBName);
        try {
            setParent(new SQLBundle(bundleName));
        } catch (MissingResourceException e) {
            setParent(null);
            // this is okay
        }
    }

    private SQLBundle(DBType dbType, String bundleName) throws MissingResourceException {
        String RBName = bundleName + "-" + dbType.getStringType();
        //System.out.println("properties file ="+RBName);
        m_sql_bundle = ResourceBundle.getBundle(RBName);
        try {
            setParent(new SQLBundle(bundleName));
        } catch (MissingResourceException e) {
            setParent(null);
            // this is okay
        }
    }

    /**
     * Creates a base (not db specific) SQLResourceBundle.
     */
    private SQLBundle(String bundleName) throws MissingResourceException {
        m_sql_bundle = ResourceBundle.getBundle(bundleName);
        setParent(null);
    }

    /**
     * Return an enumeration of the keys.
     */
    public Enumeration getKeys() {
        // Not yet implemented -sorta
        return m_sql_bundle.getKeys();
    }

    /**
     * Return the Locale for this SQLResourceBundle.
     */
    public Locale getLocale() {
        return m_sql_bundle.getLocale();
    }

    /**
     * Gets the SQL.
     */
    public String getSQL(String key) throws MissingResourceException {
        return getString(key);
    }

    /**
     * Get an object from a ResourceBundle.
     */
    protected Object handleGetObject(String key) throws MissingResourceException {
        return m_sql_bundle.getObject(key);
    }

    /**
     * Set the parent bundle of this bundle.
     */
    protected void setParent(ResourceBundle parent) {
        super.setParent(parent);

        m_parent_bundle = parent;
    }


}
