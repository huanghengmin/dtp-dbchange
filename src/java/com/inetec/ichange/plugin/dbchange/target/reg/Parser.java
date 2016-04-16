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

import java.sql.SQLException;

// fredt@users 20020130 - patch 491987 by jimbag@users - made optional
// changes applied to different parts of this method
// fredt@users 20020215 - patch 1.7.0 by fredt - quoted identifiers
// support for sql standard quoted identifiers for column and table names
// fredt@users 20020218 - patch 1.7.0 by fredt - DEFAULT keyword
// support for default values for table columns
// fredt@users 20020425 - patch 548182 by skitt@users - DEFAULT enhancement
// thertz@users 20020320 - patch 473613 by thertz - outer join condition bug
// fredt@users 20020420 - patch 523880 by leptipre@users - VIEW support
// fredt@users 20020525 - patch 559914 by fredt@users - SELECT INTO logging

/**
 * Class declaration
 *
 * @version 1.7.0
 */
public class Parser {

    private Tokenizer tTokenizer;
    private String sTable;
    private String sToken;
    private Object oData;
    private int iType;
    private int iToken;
    private Expression m_expression;


    public Parser(String s) throws SQLException {
        tTokenizer = new Tokenizer(s);
        m_expression = parsexpression();
    }

    public Expression getExpression() {
        return m_expression;
    }


    private Expression parsexpression() throws SQLException {

        read();

        Expression r = readOr();

        tTokenizer.back();

        return r;
    }


    private Expression readOr() throws SQLException {

        Expression r = readAnd();

        while (iToken == Expression.OR) {
            int type = iToken;
            Expression a = r;

            read();

            r = new Expression(type, a, readAnd());
        }

        return r;
    }


    private Expression readAnd() throws SQLException {

        Expression r = readCondition();

        while (iToken == Expression.AND) {
            int type = iToken;
            Expression a = r;

            read();

            r = new Expression(type, a, readCondition());
        }

        return r;
    }

    private Expression readCondition() throws SQLException {

        if (iToken == Expression.NOT) {
            int type = iToken;

            read();

            return new Expression(type, readCondition(), null);
        } else {
            Expression a = readConcat();
            boolean not = false;

            if (iToken == Expression.NOT) {
                not = true;

                read();
            }

            /*
            if (iToken == Expression.LIKE) {
                read();

                Expression b      = readConcat();
                char       escape = 0;

                if (sToken.equals("ESCAPE")) {
                    read();

                    Expression c = readTerm();

                    Trace.check(c.getType() == Expression.VALUE,
                                Trace.INVALID_ESCAPE);

                    String s = (String) c.getValueString(Types.VARCHAR);

                    if (s == null || s.length() < 1) {
                        throw Trace.error(Trace.INVALID_ESCAPE, s);
                    }

                    escape = s.charAt(0);
                }

                a = new Expression(Expression.LIKE, a, b);

                a.setLikeEscape(escape);
            } else if (iToken == Expression.BETWEEN) {
                read();

                Expression l = new Expression(Expression.BIGGER_EQUAL, a,
                                              readConcat());

                readThis(Expression.AND);

                Expression h = new Expression(Expression.SMALLER_EQUAL, a,
                                              readConcat());

                a = new Expression(Expression.AND, l, h);
            }
            */
            /*
            else if (iToken == Expression.IN) {
                int type = iToken;

                read();
                readThis(Expression.OPEN);

                Expression b = null;

                if (iToken == Expression.SELECT) {
                    b = new Expression(parseSelect());

                    read();
                } else {
                    tTokenizer.back();

                    Vector v = new Vector();

                    while (true) {
                        v.addElement(getValueString(Types.VARCHAR));
                        read();

                        if (iToken != Expression.COMMA) {
                            break;
                        }
                    }

                    b = new Expression(v);
                }

                readThis(Expression.CLOSE);

                a = new Expression(type, a, b);
            }
            */
            else {
                Trace.check(!not, Trace.UNEExPECTED_TOKEN);

                if (Expression.isCompare(iToken)) {

                    int type = iToken;

                    read();

                    return new Expression(type, a, readConcat());
                }

                return a;
            }

            if (not) {
                a = new Expression(Expression.NOT, a, null);
            }

            return a;
        }
    }

    /**
     * Method declaration
     *
     * @param type
     * @throws SQLException
     */
    private void readThis(int type) throws SQLException {
        Trace.check(iToken == type, Trace.UNEExPECTED_TOKEN);
        read();
    }

    /**
     * Method declaration
     *
     * @return
     * @throws SQLException
     */
    private Expression readConcat() throws SQLException {

        Expression r = readSum();

        while (iToken == Expression.STRINGCONCAT) {
            int type = Expression.CONCAT;
            Expression a = r;

            read();

            r = new Expression(type, a, readSum());
        }

        return r;
    }

    /**
     * Method declaration
     *
     * @return
     * @throws SQLException
     */
    private Expression readSum() throws SQLException {

        Expression r = readFactor();

        while (true) {
            int type;

            if (iToken == Expression.PLUS) {
                type = Expression.ADD;
            } else if (iToken == Expression.NEGATE) {
                type = Expression.SUBTRACT;
            } else {
                break;
            }

            Expression a = r;

            read();

            r = new Expression(type, a, readFactor());
        }

        return r;
    }

    /**
     * Method declaration
     *
     * @return
     * @throws SQLException
     */
    private Expression readFactor() throws SQLException {

        Expression r = readTerm();

        while (iToken == Expression.MULTIPLY || iToken == Expression.DIVIDE) {
            int type = iToken;
            Expression a = r;

            read();

            r = new Expression(type, a, readTerm());
        }

        return r;
    }

    /**
     * Method declaration
     *
     * @return
     * @throws SQLException
     */
    private Expression readTerm() throws SQLException {

        Expression r = null;

        if (iToken == Expression.COLUMN) {
            String name = sToken;

            r = new Expression(sTable, sToken);

            read();

            /*
            if (iToken == Expression.OPEN) {
                Function f = new Function(dDatabase.getAlias(name), cSession);
                int      len = f.getArgCount();
                int      i   = 0;

                read();

                if (iToken != Expression.CLOSE) {
                    while (true) {
                        f.setArgument(i++, readOr());

                        if (iToken != Expression.COMMA) {
                            break;
                        }

                        read();
                    }
                }

                readThis(Expression.CLOSE);

                r = new Expression(f);
            }
            */
        } else if (iToken == Expression.NEGATE) {
            int type = iToken;

            read();

            r = new Expression(type, readTerm(), null);
        } else if (iToken == Expression.PLUS) {
            read();

            r = readTerm();
        } else if (iToken == Expression.OPEN) {
            read();

            r = readOr();

            if (iToken != Expression.CLOSE) {
                throw Trace.error(Trace.UNEExPECTED_TOKEN, sToken);
            }

            read();
        } else if (iToken == Expression.VALUE) {
            r = new Expression(iType, oData);
            read();
        }
        /*
        else if (iToken == Expression.SELECT) {
            r = new Expression(parseSelect());

            read();
        }
        */
        else if (iToken == Expression.MULTIPLY) {
            r = new Expression(sTable, null);

            read();
        } else if (iToken == Expression.IFNULL
                || iToken == Expression.CONCAT) {
            int type = iToken;

            read();
            readThis(Expression.OPEN);

            r = readOr();

            readThis(Expression.COMMA);

            r = new Expression(type, r, readOr());

            readThis(Expression.CLOSE);
        } else if (iToken == Expression.CASEWHEN) {
            int type = iToken;

            read();
            readThis(Expression.OPEN);

            r = readOr();

            readThis(Expression.COMMA);

            Expression thenelse = readOr();

            readThis(Expression.COMMA);

            // thenelse part is never evaluated; only init
            thenelse = new Expression(type, thenelse, readOr());
            r = new Expression(type, r, thenelse);

            readThis(Expression.CLOSE);
        }
        /*
        else if (iToken == Expression.CONVERT) {
            int type = iToken;

            read();
            readThis(Expression.OPEN);

            r = readOr();

            readThis(Expression.COMMA);

            int t = Utils.getTypeNr(sToken);

            r = new Expression(type, r, null);

            r.setDataType(t);
            read();
            readThis(Expression.CLOSE);
        } else if (iToken == Expression.CAST) {
            read();
            readThis(Expression.OPEN);

            r = readOr();

            Trace.check(sToken.equals("AS"), Trace.UNEExPECTED_TOKEN, sToken);
            read();

            int t = Utils.getTypeNr(sToken);

            r = new Expression(Expression.CONVERT, r, null);

            r.setDataType(t);
            read();
            readThis(Expression.CLOSE);
        }

        */
        else {
            throw Trace.error(Trace.UNEExPECTED_TOKEN, sToken);
        }

        return r;
    }

    /**
     * Method declaration
     *
     * @throws SQLException
     */

// fredt@users 20020130 - patch 497872 by Nitin Chauhan
// reordering for speed
    private void read() throws SQLException {

        sToken = tTokenizer.getString();

        if (tTokenizer.wasValue()) {
            iToken = Expression.VALUE;
            oData = tTokenizer.getAsValue();
            iType = tTokenizer.getType();
        } else if (tTokenizer.wasName()) {
            iToken = Expression.COLUMN;
            sTable = null;
        } else if (tTokenizer.wasLongName()) {
            sTable = tTokenizer.getLongNameFirst();
            sToken = tTokenizer.getLongNameLast();

            if (sToken.equals("*")) {
                iToken = Expression.MULTIPLY;
            } else {
                iToken = Expression.COLUMN;
            }
        } else if (sToken.length() == 0) {
            iToken = Expression.END;
        } else if (sToken.equals(",")) {
            iToken = Expression.COMMA;
        } else if (sToken.equals("=")) {
            iToken = Expression.EQUAL;
        } else if (sToken.equals("<>") || sToken.equals("!=")) {
            iToken = Expression.NOT_EQUAL;
        } else if (sToken.equals("<")) {
            iToken = Expression.SMALLER;
        } else if (sToken.equals(">")) {
            iToken = Expression.BIGGER;
        } else if (sToken.equals("<=")) {
            iToken = Expression.SMALLER_EQUAL;
        } else if (sToken.equals(">=")) {
            iToken = Expression.BIGGER_EQUAL;
        } else if (sToken.equalsIgnoreCase("MATCH")) {
            iToken = Expression.MATCH;
        } else if (sToken.equals("AND")) {
            iToken = Expression.AND;
        } else if (sToken.equals("OR")) {
            iToken = Expression.OR;
        } else if (sToken.equals("NOT")) {
            iToken = Expression.NOT;
        } else if (sToken.equals("IN")) {
            iToken = Expression.IN;
        } else if (sToken.equals("EExISTS")) {
            iToken = Expression.EExISTS;
        } else if (sToken.equals("BETWEEN")) {
            iToken = Expression.BETWEEN;
        } else if (sToken.equals("+")) {
            iToken = Expression.PLUS;
        } else if (sToken.equals("-")) {
            iToken = Expression.NEGATE;
        } else if (sToken.equals("*")) {
            iToken = Expression.MULTIPLY;
            sTable = null;    // in case of ASTERIEx
        } else if (sToken.equals("/")) {
            iToken = Expression.DIVIDE;
        } else if (sToken.equals("||")) {
            iToken = Expression.STRINGCONCAT;
        } else if (sToken.equals("(")) {
            iToken = Expression.OPEN;
        } else if (sToken.equals(")")) {
            iToken = Expression.CLOSE;
        } else if (sToken.equals("SELECT")) {
            iToken = Expression.SELECT;
        } else if (sToken.equals("IS")) {
            sToken = tTokenizer.getString();

            if (sToken.equals("NOT")) {
                iToken = Expression.NOT_EQUAL;
            } else {
                iToken = Expression.EQUAL;

                tTokenizer.back();
            }
        } else if (sToken.equals("LIKE")) {
            iToken = Expression.LIKE;
        } else if (sToken.equals("COUNT")) {
            iToken = Expression.COUNT;
        } else if (sToken.equals("SUM")) {
            iToken = Expression.SUM;
        } else if (sToken.equals("MIN")) {
            iToken = Expression.MIN;
        } else if (sToken.equals("MAEx")) {
            iToken = Expression.MAEx;
        } else if (sToken.equals("AVG")) {
            iToken = Expression.AVG;
        } else if (sToken.equals("IFNULL")) {
            iToken = Expression.IFNULL;
        } else if (sToken.equals("CONVERT")) {
            iToken = Expression.CONVERT;
        } else if (sToken.equals("CAST")) {
            iToken = Expression.CAST;
        } else if (sToken.equals("CASEWHEN")) {
            iToken = Expression.CASEWHEN;

// fredt@users 20020215 - patch 514111 by fredt
        } else if (sToken.equals("CONCAT")) {
            iToken = Expression.CONCAT;
        } else {
            iToken = Expression.END;
        }
    }
}
