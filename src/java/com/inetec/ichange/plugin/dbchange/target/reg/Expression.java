/* Copyrights and Licenses
 *
 * This product includes Hypersonic SQL.
 * Originally developed by Thomas Mueller and the Hypersonic SQL Group. 
 *
 * Copyright (c) 1995-2000 by the Hypersonic SQL Group. All rights reserved. 
 * Redistribution and use in DbChangeSource and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: 
 *     -  Redistributions of DbChangeSource code must retain the above copyright notice, this list of conditions
 *         and the following disclaimer. 
 *     -  Redistributions in binary form must reproduce the above copyright notice, this list of
 *         conditions and the following disclaimer in the documentation and/or other materials
 *         provided with the distribution. 
 *     -  All advertising materials mentioning features or use of this software must display the
 *        following acknowledgment: "This product includes Hypersonic SQL." 
 *     -  Products derived from this software may not be called "Hypersonic SQL" nor may
 *        "Hypersonic SQL" appear in their names without prior written permission of the
 *         Hypersonic SQL Group. 
 *     -  Redistributions of any form whatsoever must retain the following acknowledgment: "This
 *          product includes Hypersonic SQL." 
 * This software is provided "as is" and any expressed or implied warranties, including, but
 * not limited to, the implied warranties of merchantability and fitness for a particular purpose are
 * disclaimed. In no event shall the Hypersonic SQL Group or its contributors be liable for any
 * direct, indirect, incidental, special, eExemplary, or consequential damages (including, but
 * not limited to, procurement of substitute goods or services; loss of use, data, or profits;
 * or business interruption). However caused any on any theory of liability, whether in contract,
 * strict liability, or tort (including negligence or otherwise) arising in any way out of the use of this
 * software, even if advised of the possibility of such damage. 
 * This software consists of voluntary contributions made by many individuals on behalf of the
 * Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2002, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in DbChangeSource and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of DbChangeSource code must retain the above copyright notice, this
 * list of conditions and the following disclaimer, including earlier
 * license statements (above) and comply with all above license conditions.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution, including earlier
 * license statements (above) and comply with all above license conditions.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EExPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EExEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package com.inetec.ichange.plugin.dbchange.target.reg;


import com.inetec.ichange.plugin.dbchange.datautils.db.Row;
import com.inetec.ichange.plugin.dbchange.datautils.db.Column;

import java.sql.SQLException;
import java.sql.Types;

import org.apache.oro.text.regex.*;

// fredt@users 20020215 - patch 1.7.0 by fredt
// to preserve column size etc. when SELECT INTO TABLE is used

/**
 * Expression class declaration
 *
 * @version 1.7.0
 */
public class Expression {

    // leaf types
    static final int VALUE = 1,
            COLUMN = 2,
            QUERY = 3,
            TRUE = 4,
            VALUELIST = 5,
            ASTERIEx = 6,
            FUNCTION = 7;

    // operations
    static final int NEGATE = 9,
            ADD = 10,
            SUBTRACT = 11,
            MULTIPLY = 12,
            DIVIDE = 14,
            CONCAT = 15;

    // logical operations
    static final int NOT = 20,
            EQUAL = 21,
            BIGGER_EQUAL = 22,
            BIGGER = 23,
            SMALLER = 24,
            SMALLER_EQUAL = 25,
            NOT_EQUAL = 26,
            LIKE = 27,
            AND = 28,
            OR = 29,
            IN = 30,
            EExISTS = 31,
            MATCH = 32;

    // aggregate functions
    static final int COUNT = 40,
            SUM = 41,
            MIN = 42,
            MAEx = 43,
            AVG = 44,
            DIST_COUNT = 45;

    // system functions
    static final int IFNULL = 60,
            CONVERT = 61,
            CASEWHEN = 62;

    // temporary used during paring
    static final int PLUS = 100,
            OPEN = 101,
            CLOSE = 102,
            SELECT = 103,
            COMMA = 104,
            STRINGCONCAT = 105,
            BETWEEN = 106,
            CAST = 107,
            END = 108;
    private int iType;

    // nodes
    private Expression arg,
            eArg2;

    // VALUE, VALUELIST
    private Object oData;
    private int iDataType;


    // COLUMN
    private String sTable;
    private String sColumn;
    //private String      sValue;
    private Object oValue;

    // data
    private Row m_dataRow;


    Expression(int type, Expression e, Expression e2) {
        iType = type;
        arg = e;
        eArg2 = e2;
    }

    Expression(String table, String column) {
        iType = COLUMN;
        sTable = table;
        sColumn = column;
    }


    Expression(int datatype, Object o) {
        iType = VALUE;
        iDataType = datatype;
        oData = o;
    }

    public void setRowData(Row row) {
        if (arg != null) {
            arg.setRowData(row);
        }

        if (eArg2 != null) {
            eArg2.setRowData(row);
        }

        if (iType == COLUMN) {
            Column[] cs = row.getColumnArray();
            for (int i = 0; i < cs.length; i++) {
                Column c = cs[i];
                if (c.getName().equalsIgnoreCase(sColumn)) {
                    if (c.isNull()) {
                        oValue = null;
                    } else {
                        oValue = c.getObject();
                    }
                    iDataType = c.getJdbcType();
                }
            }
        }
    }

    int getType() {
        return iType;
    }

    Expression getArg() {
        return arg;
    }

    Expression getArg2() {
        return eArg2;
    }

    String getTableName() {
        return sTable;
    }

    String getColumnName() {
        return sColumn;
    }

    int getDataType() {
        return iDataType;
    }

    static boolean isCompare(int i) {

        switch (i) {

            case EQUAL :
            case BIGGER_EQUAL :
            case BIGGER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
            case MATCH:
                return true;
        }

        return false;
    }


    Object getValue(int type) throws SQLException {

        Object o = getValue();

        if ((o == null) || (iDataType == type)) {
            return o;
        }

        return Utils.convertObject(o, type);
    }


    Object getValue() throws SQLException {

        switch (iType) {

            case VALUE :
                return oData;

            case COLUMN :
                return oValue;

        }

        // todo: simplify this
        Object a = null,
                b = null;

        if (arg != null) {
            a = arg.getValue(iDataType);
        }

        if (eArg2 != null) {
            b = eArg2.getValue(iDataType);
        }

        switch (iType) {

            case ADD :
                return Utils.add(a, b, iDataType);

            case SUBTRACT :
                return Utils.subtract(a, b, iDataType);

            case MULTIPLY :
                return Utils.multiply(a, b, iDataType);

            case DIVIDE :
                return Utils.divide(a, b, iDataType);

            case CONCAT :
                return Utils.concat(a, b);

            case IFNULL :
                return (a == null) ? b
                        : a;

            default :

                // must be comparisation
                // todo: make sure it is
                return new Boolean(test());
        }
    }

    public boolean test() throws SQLException {

        switch (iType) {

            case TRUE :
                return true;

            case NOT :
                Trace.doAssert(eArg2 == null, "Expression.test");

                return !arg.test();

            case AND :
                return arg.test() && eArg2.test();

            case OR :
                return arg.test() || eArg2.test();

            case MATCH:
                String s = (String) eArg2.getValue(Types.VARCHAR);
                String c = (String) arg.getValue(Types.VARCHAR);


                PatternCompiler compiler = new Perl5Compiler();
                Pattern pattern = null;
                try {
                    pattern = compiler.compile(s);
                    PatternMatcher matcher = new Perl5Matcher();
                    return matcher.matches(c, pattern);

                } catch (MalformedPatternException e) {
                    throw new SQLException("Failed to eExecute match operation: " + c + " match " + s);
                }

                /*
                 case LIKE :

                     // todo: now for all tests a new 'like' object required!
                     String s    = (String) eArg2.getValueString(Types.VARCHAR);
                     int    type = argtaType;
                     Like l = new Like(s, cLikeEscape,
                                       type == Utils.VARCHAR_IGNORECASE);
                     String c = (String) argValueString(Types.VARCHAR);

                     return l.compare(c);

                 case IN :
                     return eArg2.testValueList(argValueString(), argtaType);

                 case EExISTS :
                     Result r = arglect.getResult(1);    // 1 is already enough

                     return r.rRoot != null;
                */
        }

        Trace.check(arg != null, Trace.GENERAL_ERROR);

        Object o = arg.getValue();
        int type = arg.iDataType;

        Trace.check(eArg2 != null, Trace.GENERAL_ERROR);

        Object o2 = eArg2.getValue(type);
        int result = Utils.compare(o, o2, type);


        switch (iType) {

            case EQUAL :
                return result == 0;

            case BIGGER :
                return result > 0;

            case BIGGER_EQUAL :
                return result >= 0;

            case SMALLER_EQUAL :
                return result <= 0;

            case SMALLER :
                return result < 0;

            case NOT_EQUAL :
                return result != 0;

        }

        Trace.doAssert(false, "Expression.test2");

        return false;
    }


}
