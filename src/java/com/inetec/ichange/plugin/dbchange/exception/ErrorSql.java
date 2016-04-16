/*=============================================================
 * �ļ�����: ErrorSql.java
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
package com.inetec.ichange.plugin.dbchange.exception;

import com.inetec.common.exception.ErrorCode;
import com.inetec.common.i18n.Key;

public class ErrorSql extends ErrorCode {

    public final static int I_Other = 2000;
    public final static Key K_Other = new Key("Other sql Exception.");
    public final static ErrorSql ERROR___OTHER = new ErrorSql(I_Other);

    public final static int I_DbConnection = -2001;
    public final static Key K_DbConnection = new Key("Database connection Exception.");
    public final static ErrorSql ERROR___DB_CONNECTION = new ErrorSql(I_DbConnection);

    public final static int I_StatementTimeOut = -2002;
    public final static Key K_StatementTimeOut = new Key("Statement time out.");
    public final static ErrorSql ERROR___STATEMENT_TIME_OUT = new ErrorSql(I_StatementTimeOut);


    protected ErrorSql(int errcode) {
        super(errcode);
    }
}
