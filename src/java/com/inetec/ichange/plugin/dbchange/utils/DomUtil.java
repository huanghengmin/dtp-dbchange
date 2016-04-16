package com.inetec.ichange.plugin.dbchange.utils;

import org.w3c.dom.*;

import java.lang.reflect.Method;

public class DomUtil {

    //
    // Constants
    //

    // feature ids

    /**
     * Namespaces feature id (http://Exml.org/saEx/features/namespaces).
     */
    protected static final String NAMESPACES_FEATURE_ID = "http://xml.org/sax/features/namespaces";

    /**
     * Validation feature id (http://Exml.org/saEx/features/validation).
     */
    protected static final String VALIDATION_FEATURE_ID = "http://xml.org/sax/features/validation";

    /**
     * Schema validation feature id (http://apache.org/Exml/features/validation/schema).
     */
    protected static final String SCHEMA_VALIDATION_FEATURE_ID = "http://apache.org/xml/features/validation/schema";

    /**
     * Schema full checking feature id (http://apache.org/Exml/features/validation/schema-full-checking).
     */
    protected static final String SCHEMA_FULL_CHECKING_FEATURE_ID = "http://apache.org/xml/features/validation/schema-full-checking";

    // property ids

    /**
     * LeExical handler property id (http://Exml.org/saEx/properties/leExical-handler).
     */
    protected static final String LEExICAL_HANDLER_PROPERTY_ID = "http://xml.org/sax/properties/lexical-handler";

    // default settings

    /**
     * Default parser name.
     */
    protected static final String DEFAULT_PARSER_NAME = "dom.wrappers.xerces";

    /**
     * Default namespaces support (true).
     */
    protected static final boolean DEFAULT_NAMESPACES = true;

    /**
     * Default validation support (false).
     */
    protected static final boolean DEFAULT_VALIDATION = false;

    /**
     * Default Schema validation support (false).
     */
    protected static final boolean DEFAULT_SCHEMA_VALIDATION = false;

    /**
     * Default Schema full checking support (false).
     */
    protected static final boolean DEFAULT_SCHEMA_FULL_CHECKING = false;

    /**
     * Default canonical target (false).
     */
    protected static final boolean DEFAULT_CANONICAL = false;

    //
    // Rows
    //

    /**
     * Canonical target.
     */
    public final static boolean fCanonical = true;

    /**
     * Processing ExML 1.1 document.
     */
    public final static boolean fxML11 = true;


    /**
     * get the specified node, recursively.
     */
    public static StringBuffer read(Node node) {
        StringBuffer result = new StringBuffer();
        // is there anything to do?
        if (node == null) {
            return result;
        }

        short type = node.getNodeType();
        switch (type) {
            case Node.DOCUMENT_NODE: {
                Document document = (Document) node;
                //boolean fExML11 = "1.1".equals(getVersion(document));
                //if (!fCanonical) {
                if (fxML11) {
                    result.append("<?xml version=\"1.1\" encoding=\"utf-8\"?>\n");
                } else {
                    result.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
                }
                result.append(read(document.getDoctype()));
                //}
                result.append(read(document.getDocumentElement()));
                break;
            }

            case Node.DOCUMENT_TYPE_NODE: {
                DocumentType doctype = (DocumentType) node;
                result.append("<!DOCTYPE ");
                result.append(doctype.getName());
                String publicId = doctype.getPublicId();
                String systemId = doctype.getSystemId();
                if (publicId != null) {
                    result.append(" PUBLIC '");
                    result.append(publicId);
                    result.append("' '");
                    result.append(systemId);
                    result.append('\'');
                } else if (systemId != null) {
                    result.append(" SYSTEM '");
                    result.append(systemId);
                    result.append('\'');
                }
                String internalSubset = doctype.getInternalSubset();
                if (internalSubset != null) {
                    result.append(" [\n");
                    result.append(internalSubset);
                    result.append(']');
                }
                result.append(">\n");
                break;
            }

            case Node.ELEMENT_NODE: {
                result.append('<');
                result.append(node.getNodeName());
                Attr attrs[] = sortAttributes(node.getAttributes());
                for (int i = 0; i < attrs.length; i++) {
                    Attr attr = attrs[i];
                    result.append(' ');
                    result.append(attr.getNodeName());
                    result.append("=\"");
                    result.append(normalizeAndPrint(attr.getNodeValue(), true));
                    result.append('"');
                }
                result.append('>');

                Node child = node.getFirstChild();
                while (child != null) {
                    result.append(read(child));
                    child = child.getNextSibling();
                }
                break;
            }

            case Node.ENTITY_REFERENCE_NODE: {
                if (fCanonical) {
                    Node child = node.getFirstChild();
                    while (child != null) {
                        result.append(read(child));
                        child = child.getNextSibling();
                    }
                } else {
                    result.append('&');
                    result.append(node.getNodeName());
                    result.append(';');
                }
                break;
            }

            case Node.CDATA_SECTION_NODE: {
                if (fCanonical) {

                    result.append("\"");

                    String value = node.getNodeValue();
                    if (value != null) {
                        value = value.trim();
                        if (value.equals("")) {
                            result.append("");
                        } else {
                            result.append(normalizeAndPrint(node.getNodeValue(), false));
                        }
                    } //  else {}

                    result.append("\"");

                    //result.append(normalizeAndPrint(node.getNodeValue(), false));
                } else {
                    result.append("<![CDATA[\"");

                    String value = node.getNodeValue();
                    if (value != null) {
                        value = value.trim();
                        if (value.equals("")) {
                            result.append("");
                        } else {
                            result.append(value);
                        }
                    } //  else {}


                    result.append("\"]]>");
                }
                break;
            }

            case Node.TEXT_NODE: {
                result.append(normalizeAndPrint(node.getNodeValue(), false));
                break;
            }

            case Node.PROCESSING_INSTRUCTION_NODE: {
                result.append("<?");
                result.append(node.getNodeName());
                String data = node.getNodeValue();
                if (data != null && data.length() > 0) {
                    result.append(' ');
                    result.append(data);
                }
                result.append("?>");
                break;
            }

            case Node.COMMENT_NODE: {
                if (!fCanonical) {
                    result.append("<!--");
                    String comment = node.getNodeValue();
                    if (comment != null && comment.length() > 0) {
                        result.append(comment);
                    }
                    result.append("-->");
                }
            }
        }

        if (type == Node.ELEMENT_NODE) {
            result.append("</");
            result.append(node.getNodeName());
            result.append('>');
        }

        return result;
    } // write(Node)

    /**
     * Returns a sorted list of attributes.
     */
    protected static Attr[] sortAttributes(NamedNodeMap attrs) {

        int len = (attrs != null) ? attrs.getLength() : 0;
        Attr array[] = new Attr[len];
        for (int i = 0; i < len; i++) {
            array[i] = (Attr) attrs.item(i);
        }
        for (int i = 0; i < len - 1; i++) {
            String name = array[i].getNodeName();
            int indeEx = i;
            for (int j = i + 1; j < len; j++) {
                String curName = array[j].getNodeName();
                if (curName.compareTo(name) < 0) {
                    name = curName;
                    indeEx = j;
                }
            }
            if (indeEx != i) {
                Attr temp = array[i];
                array[i] = array[indeEx];
                array[indeEx] = temp;
            }
        }

        return array;

    } // sortAttributes(NamedNodeMap):Attr[]

    //
    // Protected methods
    //

    /**
     * Normalizes and prints the given string.
     */
    protected static StringBuffer normalizeAndPrint(String s, boolean isAttValue) {
        int len = (s != null) ? s.length() : 0;
        StringBuffer result = new StringBuffer();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            result.append(normalizeAndPrint(c, isAttValue));
        }

        return result;
    } // normalizeAndPrint(String,boolean)

    /**
     * Normalizes and print the given character.
     */
    protected static StringBuffer normalizeAndPrint(char c, boolean isAttValue) {

        StringBuffer result = new StringBuffer();
        switch (c) {
            case '<': {
                result.append("&lt;");
                break;
            }
            case '>': {
                result.append("&gt;");
                break;
            }
            case '&': {
                result.append("&amp;");
                break;
            }
            case '"': {
                // A '"' that appears in character data
                // does not need to be escaped.
                if (isAttValue) {
                    result.append("&quot;");
                } else {
                    result.append("\"");
                }
                break;
            }
            case '\r': {
                // If CR is part of the document's content, it
                // must not be printed as a literal otherwise
                // it would be normalized to LF when the document
                // is reparsed.
                result.append("&#xD;");
                break;
            }
            case '\n': {
                if (fCanonical) {
                    result.append("&#xA;");
                    break;
                }
                // else, default print char
            }
            default: {
                // In ExML 1.1, control chars in the ranges [#Ex1-#Ex1F, #Ex7F-#Ex9F] must be escaped.
                //
                // Escape space characters that would be normalized to #Ex20 in attribute values
                // when the document is reparsed.
                //
                // Escape NEL (0Ex85) and LSEP (0Ex2028) that appear in content
                // if the document is ExML 1.1, since they would be normalized to LF
                // when the document is reparsed.
                if (fxML11 && ((c >= 0x01 && c <= 0x1F && c != 0x09 && c != 0x0A)
                        || (c >= 0x7F && c <= 0x9F) || c == 0x2028)
                        || isAttValue && (c == 0x09 || c == 0x0A)) {
                    result.append("&#x");
                    result.append(Integer.toHexString(c).toUpperCase());
                    result.append(";");
                } else {
                    result.append(c);
                }
            }
        }

        return result;
    } // normalizeAndPrint(char,boolean)

    /**
     * EExtracts the ExML version from the Document.
     */
    protected static String getVersion(Document document) {
        if (document == null) {
            return null;
        }
        String version = null;
        Method getExMLVersion = null;
        try {
            getExMLVersion = document.getClass().getMethod("getxmlVersion", new Class[]{});
            // If Document class implements DOM L3, this method will eExist.
            if (getExMLVersion != null) {
                version = (String) getExMLVersion.invoke(document, null);
            }
        }
        catch (Exception e) {
            // Either this locator object doesn't have
            // this method, or we're on an old JDK.
        }
        return version;
    } // getVersion(Document)


}
