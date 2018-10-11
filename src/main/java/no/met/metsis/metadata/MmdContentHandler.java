/*
 *  Copyright (C) 2018 MET Norway
 *  Contact information:
 *  Norwegian Meteorological Institute
 *  Henrik Mohns Plass 1
 *  0313 OSLO
 *  NORWAY
 *
 * This file is part of metsis-metadata
 * metsis-metadata is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * metsis-metadata is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with metsis-metadata; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

package no.met.metsis.metadata;

import com.google.common.collect.Multimap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class MmdContentHandler extends DefaultHandler {

    private String xPath = "/";
    private XMLReader xmlReader;
    private MmdContentHandler parent;
    private final StringBuilder characters = new StringBuilder();
    private Multimap<String, String> xpathMap;
    private Config config;
    private final Map<String, String> elementMap = new TreeMap<>();

    public MmdContentHandler(Config config, XMLReader xmlReader, Multimap<String, String> xpathMap) {
        this.config = config;
        this.xmlReader = xmlReader;
        this.xpathMap = xpathMap;
    }

    private MmdContentHandler(String xPath, XMLReader xmlReader, MmdContentHandler parent) {
        this(parent.config, xmlReader, parent.xpathMap);
        this.xPath = xPath;
        this.parent = parent;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        String childXPath = xPath + "_" + ((StringUtils.startsWithIgnoreCase(qName, "mmd:")) ? StringUtils.substringAfter(qName, ":") : qName);

        MmdContentHandler child = new MmdContentHandler(childXPath, xmlReader, this);
        xmlReader.setContentHandler(child);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String value = characters.toString().trim();
        if (value.length() > 0) {
            Set<Map.Entry<String, ConfigValue>> entrySet = config.entrySet();
            for (Map.Entry<String, ConfigValue> entry : entrySet) {
                String key = entry.getKey();
                String alteredKey = key.replace(".", "_");
                if (xPath.contains(alteredKey)) {
                    parent.elementMap.put(alteredKey, characters.toString());
                }

                if (xPath.contains(alteredKey) && config.getBoolean(key)) {
                    String keyPrefix = StringUtils.substringBeforeLast(alteredKey, "_");
                    String des = "";
                    String cacheDes = parent.elementMap.get(keyPrefix + "_description");
                    if (cacheDes != null) {
                        des = cacheDes;
                    }
                    value = "\"" + parent.elementMap.get(keyPrefix + "_type") + "\":\""
                            + ((alteredKey.contains("_description")) ? parent.elementMap.get(keyPrefix + "_resource") : characters.toString()) + "\",\"description\":\""
                            + des + "\"";
                }
            }
            xpathMap.put(StringUtils.substringAfter(xPath, "_"), value);
        }
        xmlReader.setContentHandler(parent);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        characters.append(ch, start, length);
    }

    public Multimap<String, String> getXpathMap() {
        return xpathMap;
    }

}
