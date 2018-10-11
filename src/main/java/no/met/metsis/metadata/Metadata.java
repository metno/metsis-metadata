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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public abstract class Metadata {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Metadata.class);
    private HttpSolrClient solrClient;
    private Config config;
    private boolean dryRun;

    public HttpSolrClient solrClient() {
        return solrClient;
    }

    public void solrClient(HttpSolrClient solrClient) {
        this.solrClient = solrClient;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public final Multimap<String, String> toMap(String metadataXML) {
        Multimap<String, String> keyValuePairMultimap = LinkedListMultimap.create();
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            SAXParser sp = spf.newSAXParser();
            XMLReader xr = sp.getXMLReader();
            Config jsonConfig = this.config.getConfig("no.met.metsis.solr.jsonize");
            MmdContentHandler mmdContentHandler = new MmdContentHandler(jsonConfig, xr, keyValuePairMultimap);
            xr.setContentHandler(mmdContentHandler);
            xr.parse(new InputSource(IOUtils.toInputStream(metadataXML)));
        } catch (ParserConfigurationException | SAXException | IOException ex) {
            LOGGER.error("Failed to parse the metadata", ex);
        }
        return keyValuePairMultimap;
    }

    public boolean hasRequiredFields(List<String> reqFields, Multimap<String, String> metadataMap) {
        for (String reqField : reqFields) {
            if (!metadataMap.containsKey(reqField)) {
                return false;
            }

        }
        return true;
    }

    public String getMetadataIdentifier(Multimap<String, String> metadataMap, String id) {
        String mmdId = id;
        Collection<String> idCollection = metadataMap.asMap().get("mmd_metadata_identifier");
        if (idCollection != null && !idCollection.isEmpty()) {
            mmdId = idCollection.iterator().next();
        }
        return mmdId;
    }

    public void doIndex(List<SolrInputDocument> documents) throws SolrServerException, IOException {
        if (!isDryRun()) {
            List<List<SolrInputDocument>> partition = Lists.partition(documents, 500);
            for (List<SolrInputDocument> documentList : partition) {
                //System.out.println("" + documentList.size());
                solrClient().add(documentList);
                solrClient().commit();
            }
        }
    }

    public abstract void index(Path sourceDirectory, List<String> requiredFields);

    public abstract SolrInputDocument toSolrInputDocument(String id, Multimap<String, String> metadataMap);

}
