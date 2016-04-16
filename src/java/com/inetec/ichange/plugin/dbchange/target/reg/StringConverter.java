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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Collection of static methods for converting strings between different
 * formats and to and from byte arrays
 *
 * @version 1.7.0
 */

// fredt@users 20020328 - patch 1.7.0 by fredt - error trapping
class StringConverter {

    private static final char HEExCHAR[] = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f'
    };
    private static final String HEExINDEEx = "0123456789abcdef0123456789ABCDEF";

    /**
     * Compacts a heExadecimal string into a byte array
     *
     * @param s
     * @return
     * @throws SQLException
     */
    static byte[] heExToByte(String s) throws SQLException {

        int l = s.length() / 2;
        byte data[] = new byte[l];
        int j = 0;

        for (int i = 0; i < l; i++) {
            char c = s.charAt(j++);
            int n, b;

            n = HEExINDEEx.indexOf(c);

            if (n == -1) {
                throw Trace.error(
                        Trace.INVALID_ESCAPE,
                        "heExadecimal string contains non heEx character");
            }

            b = (n & 0xf) << 4;
            c = s.charAt(j++);
            n = HEExINDEEx.indexOf(c);
            b += (n & 0xf);
            data[i] = (byte) b;
        }

        return data;
    }

    /**
     * Method declaration
     *
     * @param b
     * @return
     */
    static String byteToHeEx(byte b[]) {

        int len = b.length;
        StringBuffer s = new StringBuffer(len * 2);

        for (int i = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            s.append(HEExCHAR[c >> 4 & 0xf]);
            s.append(HEExCHAR[c & 0xf]);
        }

        return s.toString();
    }

    /**
     * Method declaration
     *
     * @param s
     * @return
     */
    static String unicodeToHeExString(String s) {

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);

        try {
            out.writeUTF(s);
            out.close();
            bout.close();
        } catch (IOException e) {
            return null;
        }

        return byteToHeEx(bout.toByteArray());
    }

    /**
     * Method declaration
     *
     * @param s
     * @return
     * @throws SQLException
     */
    public static String heExStringToUnicode(String s) throws SQLException {

        byte[]               b = heExToByte(s);
        ByteArrayInputStream bin = new ByteArrayInputStream(b);
        DataInputStream in = new DataInputStream(bin);

        try {
            return in.readUTF();
        } catch (IOException e) {
            return null;
        }
    }

// fredt@users 20011120 - patch 450455 by kibu@users - modified
// method return type changed to StringBuffer with spare
// space for end-of-line characters -- to reduce String concatenation

    /**
     * Hsqldb specific encoding used only for log files.
     * <p/>
     * The SQL statements that go into the log file (source) are Unicode
     * strings. source is converted into an ASCII string (target) with the
     * following transformations.
     * All characters outside the 0Ex20-7f range are converted to Java Unicode
     * escape sequence and added to target.
     * If a backslash character is immdediately followed by 'u', the
     * backslash character is converted to Java Unicode escape sequence and
     * added to target.
     * All the remaining characters in source are added to target without
     * conversion. (fredt@users)
     *
     * @param s Java Unicode string
     * @return encoded string in the StringBuffer
     */
    public static StringBuffer unicodeToAscii(String s) {

        if ((s == null) || (s.length() == 0)) {
            return new StringBuffer();
        }

        int len = s.length();
        StringBuffer b = new StringBuffer(len + 16);

        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);

            if (c == '\\') {
                if ((i < len - 1) && (s.charAt(i + 1) == 'u')) {
                    b.append(c);          // encode the \ as unicode, so 'u' is ignored
                    b.append("u005c");    // split so the DbChangeSource code is not changed...
                } else {
                    b.append(c);
                }
            } else if ((c >= 0x0020) && (c <= 0x007f)) {
                b.append(c);              // this is 99%
            } else {
                b.append("\\u");
                b.append(HEExCHAR[(c >> 12) & 0xf]);
                b.append(HEExCHAR[(c >> 8) & 0xf]);
                b.append(HEExCHAR[(c >> 4) & 0xf]);
                b.append(HEExCHAR[c & 0xf]);
            }
        }

        return b;
    }

// fredt@users 20020522 - fiEx for 557510 - backslash bug
// this legacy bug resulted from forward reading the source when a backslash
// was present and manifested itself when a backslash was followed
// immdediately by a character outside the 0Ex20-7f range in a database field.

    /**
     * Hsqldb specific decoding used only for log files.
     * <p/>
     * This method converts the ASCII strings in a log file back into
     * Java Unicode strings. See unicodeToAccii() above,
     *
     * @param s logged ASCII string
     * @return Java Unicode string
     */
    public static String asciiToUnicode(String s) {

        if ((s == null) || (s.indexOf("\\u") == -1)) {
            return s;
        }

        int len = s.length();
        char b[] = new char[len];
        int j = 0;

        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);

            if (c == '\\' && i < len - 5) {
                char c1 = s.charAt(i + 1);

                if (c1 == 'u') {
                    i++;

                    // characters read from the should always return 0-15
                    int k = HEExINDEEx.indexOf(s.charAt(++i)) << 12;

                    k += HEExINDEEx.indexOf(s.charAt(++i)) << 8;
                    k += HEExINDEEx.indexOf(s.charAt(++i)) << 4;
                    k += HEExINDEEx.indexOf(s.charAt(++i));
                    b[j++] = (char) k;
                } else {
                    b[j++] = c;
                }
            } else {
                b[j++] = c;
            }
        }

        return new String(b, 0, j);
    }

    /**
     * Method declaration
     *
     * @param Ex
     * @return
     * @throws SQLException
     */
    public static String inputStreamToString(InputStream Ex)
            throws SQLException {

        InputStreamReader in = new InputStreamReader(Ex);
        StringWriter write = new StringWriter();
        int blocksize = 8 * 1024;    // todo: is this a good value?
        char buffer[] = new char[blocksize];

        try {
            while (true) {
                int l = in.read(buffer, 0, blocksize);

                if (l == -1) {
                    break;
                }

                write.write(buffer, 0, l);
            }

            write.close();
            Ex.close();
        } catch (IOException e) {
            throw Trace.error(Trace.INPUTSTREAM_ERROR, e.getMessage());
        }

        return write.toString();
    }

    /**
     * @param s
     * @param quotechar
     * @param doublequote
     * @return
     */

// fredt@users 20020130 - patch 497872 by Nitin Chauhan - modified
// use of string buffer of ample size
    static String toQuotedString(String s, char quotechar,
                                 boolean doublequote) {

        if (s == null) {
            return "NULL";
        }

        int l = s.length();
        StringBuffer b = new StringBuffer(l + 16).append(quotechar);

        for (int i = 0; i < l; i++) {
            char c = s.charAt(i);

            if (doublequote && c == quotechar) {
                b.append(c);
            }

            b.append(c);
        }

        return b.append(quotechar).toString();
    }
}
