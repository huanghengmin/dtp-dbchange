/*=============================================================
 * 文件名称: Rows.java
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
package com.inetec.ichange.plugin.dbchange.datautils.db;

import org.w3c.dom.Element;
import org.w3c.dom.Document;
import org.apache.xerces.dom.DocumentImpl;

import java.util.ArrayList;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

import com.inetec.common.exception.Ex;
import com.inetec.ichange.plugin.dbchange.utils.DomUtil;
import com.inetec.ichange.plugin.dbchange.datautils.DataInformation;


public class Rows {

    private ArrayList listRows = new ArrayList();

    public int size() {
        return listRows.size();
    }

    public void addRow(Row row) {
        listRows.add(row);
    }

    public void clear() {
        listRows.clear();
    }

    public Row[] getRowArray() {
        return (Row[]) listRows.toArray(new Row[0]);
    }

    public Document toDocument() throws Ex {
        Document doc = new DocumentImpl();
        doc.appendChild(toElement(doc));
        return doc;
    }

    public Element toElement(Document doc) throws Ex {
        Row[] rows = getRowArray();
        Element root = doc.createElement("rows");
        for (int i = 0; i < rows.length; i++) {
            Row row = rows[i];
            root.appendChild(row.toElement(doc));
        }

        return root;
    }

    public InputStream getDataInputStream() throws Ex {
        Document doc = toDocument();
        StringBuffer result = DomUtil.read(doc);
        InputStream is = null;
        try {
            is = new ByteArrayInputStream(result.toString().getBytes(DataInformation.Str_CharacterSet));
        } catch (UnsupportedEncodingException e) {
            throw new Ex().set(e);
        }
        return is;
    }

}
