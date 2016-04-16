/*=============================================================
 * �ļ�����: Rows.java
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
