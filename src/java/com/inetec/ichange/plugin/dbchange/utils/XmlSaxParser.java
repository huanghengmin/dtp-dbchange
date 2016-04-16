package com.inetec.ichange.plugin.dbchange.utils;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.*;
import org.apache.log4j.Category;

import java.io.InputStream;
import java.io.IOException;

import com.inetec.ichange.plugin.dbchange.datautils.db.*;
import com.inetec.ichange.plugin.dbchange.datautils.dboperator.DbopUtil;
import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;


public class XmlSaxParser extends DefaultHandler {

    /**
     * Validation feature id (http://Exml.org/saEx/features/validation).
     */
    protected static final String VALIDATION_FEATURE_ID = "http://xml.org/sax/features/validation";
    /**
     * LeExical handler property id (http://Exml.org/saEx/properties/leExical-handler).
     */
    protected static final String LEExICAL_HANDLER_PROPERTY_ID = "http://xml.org/sax/properties/lexical-handler";

    /**
     * Default parser name.
     */
    protected static final String DEFAULT_PARSER_NAME = "org.apache.xerces.parsers.SAXParser";

    private final static Category m_logger = Category.getInstance(XmlSaxParser.class);
    private Rows m_rows = new Rows();
    private Row m_curRow;
    private Column m_curColumn;
    private String m_lastData = "";


    //Constructor
    public XmlSaxParser() {
        super();
    }


    public Rows parse(InputStream is) throws Ex {

        try {
            //SAExParserFactory factory = SAExParserFactory.newInstance();
            XMLReader ExmlReader = XMLReaderFactory.createXMLReader(DEFAULT_PARSER_NAME);
            //factory.setValidating(false);
            //SAExParser parser = factory.newSAExParser();
            //ExMLReader ExmlReader = parser.getExMLReader();
            ExmlReader.setFeature(VALIDATION_FEATURE_ID, false);
            //ExmlReader.setProperty(LEExICAL_HANDLER_PROPERTY_ID, false);
            ExmlReader.setContentHandler(this);
            ExmlReader.setErrorHandler(this);
            InputSource inputSource = new InputSource(is);
            ExmlReader.parse(inputSource);
        } catch (IOException io) {
            throw new Ex().set(E.E_IOException, io);
        } catch (SAXException eExc) {
            throw new Ex().set(eExc);
        }
        return m_rows;
    }

    //Response the startDocument event
    public void startDocument() {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("start document");
        }
    }

    public void endDocument() {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("end document");
        }
    }

    public void startElement(String uri, String localName, String qName, Attributes attrs) {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("satart element:" + qName);
        }
        if (qName.equalsIgnoreCase("row")) {
            m_curRow = new Row(attrs.getValue("database"), attrs.getValue("table"));
            String operator = attrs.getValue(Operator.Str_Operator);
            m_curRow.setAction(Operator.getOperator(operator));
            String op_time = attrs.getValue("op_time");
            m_curRow.setOp_time(new Long(op_time).longValue());
        }
        if (qName.equalsIgnoreCase("field")) {
            String name = attrs.getValue("name");
            String jdbcType = attrs.getValue("jdbctype");
            String dbType = attrs.getValue("dbtype");
            String ispk = attrs.getValue("ispk");
            String isnull = attrs.getValue("isnull");
            m_curColumn = new Column(name, DbopUtil.getJdbcType(jdbcType), dbType, ispk.equals("true"));
            if (!isnull.equals("false")) {
                m_curColumn.setValue(null);
            } else {
                m_curColumn.setValue(new Value(""));
            }
        }
    }

    public void characters(char[] ch, int start, int length) {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("read data:" + new String(ch, start, length));
        }
        m_lastData = m_lastData + new String(ch, start, length);
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("end element:" + qName);
        }
        if (qName.equalsIgnoreCase("row")) {
            m_rows.addRow(m_curRow);
        }
        if (qName.equalsIgnoreCase("field")) {
            // if (!m_curColumn.isNull()) {
            int len = m_lastData.length();
            //m_lastData = m_lastData.trim();
            if (!m_curColumn.isNull()) {
                if (!m_lastData.equals("")) {
                    if (m_lastData.charAt(0) != '\"' || m_lastData.charAt(len - 1) != '\"') {
                        m_logger.error("The data in the xml is not complete:" + m_lastData);
                    } else {
                        m_lastData = m_lastData.substring(1, len - 1);
                    }
                    m_curColumn.setValue(new Value(m_lastData.trim()));
                } // else {}
            }  // else {}
            m_lastData = "";

            m_curRow.addColumn(m_curColumn);
        }
    }

    public void fatalError(SAXParseException e) {
        m_logger.error("faltal error of xml sax parser.", e);
    }

    public void error(SAXParseException e) {
        m_logger.error("error of xml sax parser.", e);
    }

    public void warning(SAXParseException e) {
        m_logger.warn("faltal error of xml sax parser.", e);
    }


}
