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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MMDIndexer extends Metadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(MMDIndexer.class.getName());
    private final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat SOLR_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

    private boolean includeRelatedDataset;
    private String level;
    private Multimap<String, String> relatedDatasetMap;

    public MMDIndexer(HttpSolrClient solrClient, Config config, boolean dryRun) {
        super.setConfig(config);
        super.solrClient(solrClient);
        super.setDryRun(dryRun);
    }

    public MMDIndexer(HttpSolrClient solrClient, Config config, boolean includeRelatedDataset, boolean dryRun) {
        super.setConfig(config);
        super.solrClient(solrClient);
        super.setDryRun(dryRun);
        this.includeRelatedDataset = includeRelatedDataset;
        this.relatedDatasetMap = HashMultimap.create();
    }

    public void index(String id, String metadataXML, List<String> requiredFields) throws SolrServerException, IOException {
        SolrInputDocument solrInputDocument = createSolrInputDocument(id, metadataXML, requiredFields);
        if (solrInputDocument != null) {
            List<SolrInputDocument> documents = new ArrayList<>();
            documents.add(solrInputDocument);
            doIndex(documents);
        }

    }

    private void indexRelatedDataset() throws SolrServerException, IOException {
        Set<String> keySet = relatedDatasetMap.keySet();
        List<SolrInputDocument> documents = new ArrayList<>();
        for (String key : keySet) {
            Collection<String> relatedDatasets = relatedDatasetMap.get(key);
            documents.add(createRelatedDatasetDoc(key, relatedDatasets));
        }
        //chnage base url to level 1
        String baseURL = solrClient().getBaseURL();
        if (StringUtils.endsWith(baseURL, "l2")) {
            baseURL = StringUtils.replace(baseURL, "l2", "l1");
            solrClient().setBaseURL(baseURL);
            doIndex(documents);
        }
    }

    private SolrInputDocument createRelatedDatasetDoc(String id, Collection<String> relatedDatasets) {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", id);
        Map<String, Object> fieldModifier = new HashMap<>();
        fieldModifier.put("set", relatedDatasets);
        document.addField("mmd_related_dataset", fieldModifier);
        return document;
    }

    private void addDoument(List<SolrInputDocument> documents, Path sourceDirectory, String level, List<String> requiredFields) {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(sourceDirectory)) {
            for (Path path : directoryStream) {
                if (Files.isRegularFile(path)) {
                    String id = FilenameUtils.removeExtension(FilenameUtils.getName(path.toString()));
                    SolrInputDocument asSolrInputDocument = createSolrInputDocument(id, new String(Files.readAllBytes(path)), requiredFields);
                    if (asSolrInputDocument != null) {
                        documents.add(asSolrInputDocument);
                    } else {
                        LOGGER.warn("Skipping indexing of metadata ["
                                + path.toString() + "] since it is missing required attributes ["
                                + requiredFields.toString() + "]");
                    }
                }
            }
            doIndex(documents);
        } catch (IOException | SolrServerException ex) {
            LOGGER.error("Failed to index MMD documents", ex);
        }
    }

    private void addRelatedDataset(boolean hasRelatedDataset, String relatedDatasetDirecotry,
            SolrInputDocument asSolrInputDocument) throws IOException {
        if (includeRelatedDataset && hasRelatedDataset) {
            try (DirectoryStream<Path> relatedDatadirectoryStream = Files.newDirectoryStream(Paths.get(relatedDatasetDirecotry))) {
                for (Path relatedDatasetPath : relatedDatadirectoryStream) {
                    String relatedDatasetId = FilenameUtils.removeExtension(FilenameUtils.getName(relatedDatasetPath.toString()));
                    if (Files.isRegularFile(relatedDatasetPath)) {
                        Multimap<String, String> relatedDataMultimap = toMap(new String(Files.readAllBytes(relatedDatasetPath)));
                        String metadataIdentifier = getMetadataIdentifier(relatedDataMultimap, relatedDatasetId);
                        asSolrInputDocument.addField("mmd_related_dataset", metadataIdentifier);
                    }
                }
            }
        }
    }

    private void addRelatedDataset(String id, Multimap<String, String> metadataMap) {
        if (includeRelatedDataset) {
            String relatedDataset = getRelatedDataset(metadataMap);
            if (!relatedDataset.isEmpty()) {
                relatedDatasetMap.put(relatedDataset, id);
            }
        }
    }

    private SolrInputDocument createSolrInputDocument(String metadataId, String metadata, List<String> requiredFields) {
        Multimap<String, String> metadataMap = toMap(metadata);
        String id = getMetadataIdentifier(metadataMap, metadataId);
        SolrInputDocument solrDocument = null;
        if (hasRequiredFields(requiredFields, metadataMap)) {
            solrDocument = toSolrInputDocument(id, metadataMap);
            addRelatedDataset(id, metadataMap);
        }
        return solrDocument;
    }

    public String getRelatedDataset(Multimap<String, String> metadataMap) {
        String relatedDataSetId = "";
        Collection<String> idCollection = metadataMap.asMap().get("mmd_related_dataset");
        if (idCollection != null && !idCollection.isEmpty()) {
            relatedDataSetId = idCollection.iterator().next();
        }
        return relatedDataSetId;
    }

    @Override
    public SolrInputDocument toSolrInputDocument(String id, Multimap<String, String> metadataMap) {
        SolrInputDocument doc = new SolrInputDocument();
        //add compulasary id field
        doc.addField("id", id);

        Collection<Map.Entry<String, String>> entries = metadataMap.entries();
        Map<String, String> extensMap = new HashMap<>();
        for (Map.Entry<String, String> entry : entries) {
            if (entry.getKey().equals("mmd_geographic_extent_rectangle_west")) {
                extensMap.put("west", entry.getValue());
            }

            if (entry.getKey().equals("mmd_geographic_extent_rectangle_east")) {
                extensMap.put("east", entry.getValue());
            }

            if (entry.getKey().equals("mmd_geographic_extent_rectangle_south")) {
                extensMap.put("south", entry.getValue());
            }

            if (entry.getKey().equals("mmd_geographic_extent_rectangle_north")) {
                extensMap.put("north", entry.getValue());
            }

            if (entry.getValue().matches("\\d{4}-\\d{2}-\\d{2}")) {
                //some date 2011-09-31 unparseable dATE
                SIMPLE_DATE_FORMAT.setLenient(false);
                try {
                    Date parsedDate = SIMPLE_DATE_FORMAT.parse(entry.getValue());
                    doc.addField(entry.getKey(), SOLR_DATE_FORMAT.format(parsedDate));
                } catch (ParseException ex) {
                    //TODO
                    //metamod has wrong date
                    LOGGER.error("Error while parsing [" + id + "]");
                    //doc.addField(entry.getKey(), entry.getValue());
                }
            } else {
                doc.addField(entry.getKey(), entry.getValue());
            }
        }
        //doc.addField("geographic_extent", createMetadataPolygon(extensMap));
        doc.addField("bbox", createMetadataBBox(extensMap));

        return doc;
    }

    private String createMetadataBBox(Map<String, String> entries) {
        String west = entries.get("west");
        String east = entries.get("east");
        String noth = entries.get("north");
        String south = entries.get("south");

        StringBuilder sb = new StringBuilder();
        sb.append("ENVELOPE(");

        // minX, maxX, maxY, minY
        sb.append(west);
        sb.append(",");

        sb.append(east);
        sb.append(",");

        sb.append(noth);
        sb.append(",");

        sb.append(south);

        sb.append(")");
        return sb.toString();
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @Override
    public void index(Path sourceDirectory, List<String> requiredFields) {
        try {
            List<SolrInputDocument> documents = new ArrayList<>();
            addDoument(documents, sourceDirectory, level, requiredFields);
            doIndex(documents);
            indexRelatedDataset();
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Failed to index MMD documents");
        }
    }

    private String createMetadataPolygon(Map<String, String> entries) {
        String west = entries.get("west");
        String east = entries.get("east");
        String noth = entries.get("north");
        String south = entries.get("south");

        StringBuilder sb = new StringBuilder();
        sb.append("POLYGON((");

        // anti-clockwise W N, W S, E S, E N, W N
        sb.append(west);
        sb.append(" ");
        sb.append(noth);
        sb.append(",");

        sb.append(west);
        sb.append(" ");
        sb.append(south);
        sb.append(",");

        sb.append(east);
        sb.append(" ");
        sb.append(south);
        sb.append(",");

        sb.append(east);
        sb.append(" ");
        sb.append(noth);
        sb.append(",");

        sb.append(west);
        sb.append(" ");
        sb.append(noth);

        sb.append("))");
        return sb.toString();
    }

}
