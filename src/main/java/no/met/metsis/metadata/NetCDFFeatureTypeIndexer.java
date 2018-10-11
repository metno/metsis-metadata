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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;

public class NetCDFFeatureTypeIndexer extends Metadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetCDFFeatureTypeIndexer.class.getName());

    public NetCDFFeatureTypeIndexer(boolean dryRun) {
        super.setDryRun(dryRun);
    }

    @Override
    public void index(Path sourceDirectory, List<String> requiredFields) {
        List<SolrInputDocument> docs = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(sourceDirectory.toUri()))) {
            for (Path path : directoryStream) {
                if (Files.isRegularFile(path)) {
                    SolrInputDocument solrDocument = createFeatureTypeDocument(new String(Files.readAllBytes(path)), requiredFields,
                            FilenameUtils.removeExtension(FilenameUtils.getName(path.toString())));
                    if (solrDocument != null) {
                        docs.add(solrDocument);
                    } else {
                        LOGGER.warn("Skipping indexing of metadata ["
                                + path.toString() + "] since it is missing required attributes ["
                                + requiredFields.toString() + "]");
                    }

                }
            }
            doIndex(docs);
        } catch (IOException | SolrServerException ex) {
            LOGGER.error("Fail to index!", ex);
        }
    }

    @Override
    public SolrInputDocument toSolrInputDocument(String id, Multimap<String, String> metadataMap) {
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        solrInputDocument.addField("id", id);
        for (Map.Entry<String, String> entry : metadataMap.entries()) {
            solrInputDocument.addField(entry.getKey(), entry.getValue());

        }
        return solrInputDocument;
    }

    public void index(String metadataXML, List<String> requiredFields) throws SolrServerException, IOException {
        SolrInputDocument thumbnailDocument = createFeatureTypeDocument(metadataXML, requiredFields, "");
        if (thumbnailDocument != null) {
            List<SolrInputDocument> documents = new ArrayList<>();
            documents.add(thumbnailDocument);
            doIndex(documents);
        } else {
            LOGGER.warn("Skipping indexing of feature type since it is missing required attributes [mmd_data_access_resource] of type OPeNDAP!");
        }
    }

    private SolrInputDocument createFeatureTypeDocument(String metadataXML, List<String> requiredFields, String userProvidedId) {
        Multimap<String, String> metadataMap = toMap(metadataXML);
        SolrInputDocument solrDocument = null;
        if (hasRequiredFields(requiredFields, metadataMap)) {
            String mmdId = getMetadataIdentifier(metadataMap, userProvidedId);
            String featureType = getFeatureType(metadataMap);
            if (!featureType.isEmpty()) {
                solrDocument = toSolrInputDocument(mmdId, createMetadataKeyValuePair(mmdId, featureType));
            } else {
                LOGGER.info("Could not find featureType so skipping indexing.");
            }

        }
        return solrDocument;
    }

    private Multimap<String, String> createMetadataKeyValuePair(String metadataIdentifier, String featureType) {
        Multimap<String, String> keyValuePairMultimap = HashMultimap.create();
        keyValuePairMultimap.put("mmd_metadata_identifier", metadataIdentifier);
        keyValuePairMultimap.put("feature_type", featureType);
        return keyValuePairMultimap;
    }

    private String getFeatureType(Multimap<String, String> metadataMap) {
        Collection<String> accResCollection = metadataMap.get("mmd_data_access_resource");
        for (String resource : accResCollection) {
            String[] split = StringUtils.split(resource, ",");
            for (String token : split) {
                token = token.trim();
                token = token.replaceAll("\"", "");
                String isOpenDap = StringUtils.substringBefore(token, ":");
                isOpenDap = isOpenDap.toUpperCase();
                if (isOpenDap.startsWith("OPENDAP")) {
                    try {
                        token = StringUtils.substringAfter(token, ":");
                        NetcdfDataset netcdfDataset = NetcdfDataset.openDataset(token);
                        List<Attribute> globalAttributes = netcdfDataset.getGlobalAttributes();

                        for (Attribute globalAttribute : globalAttributes) {
                            if ("featureType".equalsIgnoreCase(globalAttribute.getFullName())) {
                                return globalAttribute.getStringValue();
                            }
                        }
                    } catch (IOException ex) {
                        LOGGER.error("Failed to parse OPenDAP dataset stream [" + token + "]. Now trying to open as netcdf file.");
                        try {
                            NetcdfFile netcdfFile = NetcdfFile.open(token);
                            List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();
                            return getFeatureType(globalAttributes);
                        } catch (IOException ex1) {
                            LOGGER.error("Failed to parse OPenDAP dateset stream as netcdf file [" + token + "]");
                        }
                    }
                }
            }
        }
        return "";
    }

    private String getFeatureType(List<Attribute> globalAttributes) {
        for (Attribute globalAttribute : globalAttributes) {
            if ("featureType".equalsIgnoreCase(globalAttribute.getFullName())) {
                return globalAttribute.getStringValue();
            }
        }
        return "";
    }

}
