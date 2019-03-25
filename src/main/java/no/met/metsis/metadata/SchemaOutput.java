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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.xmlbeans.SchemaParticle;
import org.apache.xmlbeans.SchemaProperty;
import org.apache.xmlbeans.SchemaType;
import org.apache.xmlbeans.SchemaTypeSystem;
import org.apache.xmlbeans.XmlBeans;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlOptions;
import org.apache.xmlbeans.impl.xb.xsdschema.SchemaDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaOutput {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOutput.class);
    static final Map<String, String> SOLR5_TYPE_MAP;
    static final Map<String, String> SOLR7_TYPE_MAP;


    private SchemaOutput() {
    }

    static {
        SOLR5_TYPE_MAP = new HashMap<>();
        SOLR5_TYPE_MAP.put("XmlString", "text_en");
        SOLR5_TYPE_MAP.put("XmlDouble", "double");
        SOLR5_TYPE_MAP.put("XmlDecimal", "double");
        SOLR5_TYPE_MAP.put("XmlDateTime", "tdate");
        SOLR5_TYPE_MAP.put("XmlDate", "tdate");

        SOLR7_TYPE_MAP = new HashMap<>();
        SOLR7_TYPE_MAP.put("XmlString", "text_en");
        SOLR7_TYPE_MAP.put("XmlDouble", "pdouble");
        SOLR7_TYPE_MAP.put("XmlDecimal", "pdouble");
        SOLR7_TYPE_MAP.put("XmlDateTime", "pdate");
        SOLR7_TYPE_MAP.put("XmlDate", "pdate");
    }

    private static List<IndexScehmaField> parseMmdSchema(int solrVersion, List<String> facetFields) throws XmlException, IOException {
        List<IndexScehmaField> scehmaFields = new ArrayList<>();
        XmlOptions xmlOptions = new XmlOptions();
        xmlOptions.setCompileDownloadUrls();
        xmlOptions.setLoadUseDefaultResolver();
        SchemaTypeSystem sts = XmlBeans.compileXsd(new XmlObject[]{
            SchemaDocument.Factory.parse(SchemaOutput.class.getResourceAsStream("/mmd.xsd"))},
                XmlBeans.getBuiltinTypeSystem(), xmlOptions);
        Map<String, SchemaType> schemaTypeMap = new HashMap<>();
        List allSeenTypes = new ArrayList();
        allSeenTypes.addAll(Arrays.asList(sts.documentTypes()));
        allSeenTypes.addAll(Arrays.asList(sts.attributeTypes()));
        allSeenTypes.addAll(Arrays.asList(sts.globalTypes()));
        for (int i = 0; i < allSeenTypes.size(); i++) {
            SchemaType sType = (SchemaType) allSeenTypes.get(i);
            if (sType.getName() != null) {
                schemaTypeMap.put(sType.getName().getLocalPart(), sType);
            }
            allSeenTypes.addAll(Arrays.asList(sType.getAnonymousTypes()));
        }
        SchemaType mmdType = schemaTypeMap.get("mmd_type");
        SchemaParticle[] particleChildren = mmdType.getContentModel().getParticleChildren();
        for (SchemaParticle pc : particleChildren) {
            if (pc.getParticleType() == SchemaParticle.CHOICE) {
                SchemaParticle[] optionalSchemaParticles = pc.getParticleChildren();
                for (SchemaParticle optionalSchemaParticle : optionalSchemaParticles) {
                    addSchemaField(solrVersion, optionalSchemaParticle, scehmaFields, schemaTypeMap, facetFields);
                }
            } else {
                addSchemaField(solrVersion, pc, scehmaFields, schemaTypeMap, facetFields);
            }
        }

        return scehmaFields;
    }

    private static void addSchemaField(int solrVerion, SchemaParticle particleChildren, List<IndexScehmaField> scehmaFields,
            Map<String, SchemaType> schemaTypeMap, List<String> facetFields) {
        if (particleChildren.getType() != null) {
            int maxOccurs = particleChildren.getIntMaxOccurs();
            if (particleChildren.getType().isPrimitiveType()) {
                IndexScehmaField indexScehmaField = new IndexScehmaField("mmd_" + particleChildren.getName().getLocalPart(),
                        ((particleChildren.getType().getShortJavaName() == null)
                        ? "string" : getSolrType(solrVerion, particleChildren.getType().getShortJavaName())));
                indexScehmaField.setMultiValued(((maxOccurs > 1)));
                scehmaFields.add(indexScehmaField);
            } else {
                SchemaType complexType = schemaTypeMap.get(particleChildren.getType().getName().getLocalPart());
                if (complexType.isSimpleType() || complexType.getContentType() == SchemaType.BTC_ANY_SIMPLE) {
                    String type = ((complexType.getName().getLocalPart().equals("multilang_string")) ? "text_en"
                            : ((complexType.getShortJavaName() == null)
                            ? "string" : getSolrType(solrVerion, complexType.getShortJavaName())));
                    IndexScehmaField indexScehmaField = new IndexScehmaField("mmd_" + particleChildren.getName().getLocalPart(), type);
                    indexScehmaField.setMultiValued(((maxOccurs > 1)));
                    scehmaFields.add(indexScehmaField);
                } else {
                    String cmplxName = particleChildren.getName().getLocalPart();//StringUtils.removeEnd(complexType.getName().getLocalPart(), "_type");
                    getInnerProperties(solrVerion, scehmaFields, cmplxName, maxOccurs, complexType, facetFields);
                }
            }
        }
    }

    private static void getInnerProperties(int solrVersion, List<IndexScehmaField> scehmaFields, String parentName, int parentMaxOccurs,
            SchemaType schemaType, List<String> facetFields) {
        SchemaProperty[] properties = schemaType.getProperties();
        for (SchemaProperty property : properties) {//
            if (property.getType().getContentType() == SchemaType.ELEMENT_CONTENT) {
                getInnerProperties(solrVersion, scehmaFields, parentName + "_" + property.getName().getLocalPart(), parentMaxOccurs,
                        property.getType(), facetFields);
            } else {
                int maxOccurs = property.getContainerType().getContentModel().getIntMaxOccurs();
                //String type = property.getType().getShortJavaName();
                String fieldName = "mmd_" + parentName + "_" + property.getName().getLocalPart();
                String type = getSchemaFieldType(solrVersion, fieldName, property, facetFields);
                IndexScehmaField indexScehmaField = new IndexScehmaField(fieldName, type);
                indexScehmaField.setMultiValued(((maxOccurs > 1) ? true : (parentMaxOccurs > 1) ? true : false));
                scehmaFields.add(indexScehmaField);
            }
        }
    }

    private static String getSchemaFieldType(int solrVersion, String fieldName, SchemaProperty property, List<String> facetFields) {
        String type = property.getType().getShortJavaName();
        if (type == null) { //if type null check the partent type
            type = property.getType().getBaseType().getShortJavaName();
            type = getSolrType(solrVersion, type);
            if (type == null) {
                type = "string"; //if parent type also null use string
            }
        } else {
            type = getSolrType(solrVersion, type);
        }
        if (facetFields.contains(fieldName)) {
            type = "string";
        }
        return type;
    }

    private static String getSolrType(int solrVersion, String xmlType) {
        return (solrVersion == 7) ? SOLR7_TYPE_MAP.get(xmlType) : SOLR5_TYPE_MAP.get(xmlType);
    }

    public static void outputSolrSchema(int solrVersion, Path outputPath, List<String> fullTextFields, List<String> facetFields)
            throws JDOMException, IOException, XmlException {
        SAXBuilder builder = new SAXBuilder();
        String solrSchema = (solrVersion == 7) ? "/solr/solr-v7-schema.xml" : "/solr/solr-v5-schema.xml";
        try (InputStream inputStream = SchemaOutput.class.getResourceAsStream(solrSchema)) {
            Document doc = (Document) builder.build(inputStream);
            Element rootNode = doc.getRootElement();
            List<IndexScehmaField> mdSchema = SchemaOutput.parseMmdSchema(solrVersion, facetFields);
            for (IndexScehmaField mdSchema1 : mdSchema) {
                rootNode.addContent(mdSchema1.toElement());
            }
            if (fullTextFields != null && !fullTextFields.isEmpty()) {
                IndexScehmaField fullTextField = new IndexScehmaField("full_text", "text_en");
                fullTextField.setIndexed(true);
                fullTextField.setStored(true);
                fullTextField.setMultiValued(true);
                rootNode.addContent(fullTextField.toElement());
                for (String source : fullTextFields) {
                    rootNode.addContent(createCopyField(source, "full_text"));
                }
            }
            XMLOutputter xmlOutput = new XMLOutputter();
            // display nice
            xmlOutput.setFormat(org.jdom2.output.Format.getPrettyFormat());
            xmlOutput.output(doc, new FileOutputStream(outputPath.toString() + File.separator + "schema.xml"));
        } catch (IOException exception) {
            LOGGER.error("Failed to merge schema", exception);
            return;
        }
        System.out.println("Solr schema is created successfully.");
    }

    private static Element createCopyField(String source, String destination) {
        Element copyElement = new Element("copyField");
        copyElement.setAttribute("source", source);
        copyElement.setAttribute("dest", destination);
        return copyElement;
    }

}
