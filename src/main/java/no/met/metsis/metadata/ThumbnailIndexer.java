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
import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.imageio.ImageIO;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.geotools.data.ows.Layer;
import org.geotools.data.ows.WMSCapabilities;
import org.geotools.data.wms.WMSUtils;
import org.geotools.data.wms.WebMapServer;
import org.geotools.ows.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThumbnailIndexer extends Metadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThumbnailIndexer.class.getName());
    private final String wmsVersion;

    public ThumbnailIndexer(String wmsVersion, boolean dryRun) {
        super.setDryRun(dryRun);
        this.wmsVersion = wmsVersion;
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
        SolrInputDocument thumbnailDocument = createThumbnailDocument(metadataXML, requiredFields, "");
        if (thumbnailDocument != null) {
            List<SolrInputDocument> documents = new ArrayList<>();
            documents.add(thumbnailDocument);
            doIndex(documents);
        } else {
            LOGGER.warn("Skipping indexing of thumbnail since it is missing required attributes ["
                    + requiredFields.toString() + "] of type WMS!");
        }
    }

    private SolrInputDocument createThumbnailDocument(String metadataXML, List<String> requiredFields, String userProvidedId) {
        Multimap<String, String> metadataMap = toMap(metadataXML);
        SolrInputDocument solrDocument = null;
        if (hasRequiredFields(requiredFields, metadataMap)) {
            String mmdId = getMetadataIdentifier(metadataMap, userProvidedId);
            String wmsUrl = getWmsUrl(requiredFields, metadataMap);
            System.out.println("wms: " + wmsUrl);
            if (!wmsUrl.isEmpty()) {
                String firstLayer = null;
                Collection<String> layersCollection = metadataMap.asMap().get("mmd_data_access_wms_layers_wms_layer");
                if (layersCollection != null) {
                    firstLayer = layersCollection.iterator().next();
                }
                String thumbnailUrl = generateThumbnailUrl(wmsUrl, firstLayer);
                System.out.println("" + thumbnailUrl);
                if (!thumbnailUrl.isEmpty()) {
                    solrDocument = toSolrInputDocument(mmdId, createMetadataKeyValuePair(mmdId, thumbnailUrl));
                } else {
                    LOGGER.error("Unable to generate thumbnail for the metadata [" + mmdId + "]");
                }
            }
        }
        return solrDocument;
    }

    @Override
    public void index(Path sourceDirectory, List<String> requiredFields) {
        List<SolrInputDocument> docs = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(sourceDirectory.toUri()))) {
            for (Path path : directoryStream) {
                if (Files.isRegularFile(path)) {
                    SolrInputDocument solrDocument = createThumbnailDocument(new String(Files.readAllBytes(path)),
                            requiredFields,
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
            LOGGER.error("Failed to index", ex);
        }
    }

    public Multimap<String, String> createMetadataKeyValuePair(String metadataIdentifier, String thumbnail) {
        Multimap<String, String> keyValuePairMultimap = HashMultimap.create();
        keyValuePairMultimap.put("thumbnail", thumbnail);
        String baseMapUrl = createBaseMapUrl(thumbnail);
        keyValuePairMultimap.put("base_map", baseMapUrl);
        //System.out.println("" + baseMapUrl);
        keyValuePairMultimap.put("mmd_metadata_identifier", metadataIdentifier);
        //System.out.println("" + getThumbnailData(baseMapUrl, thumbnail));
        keyValuePairMultimap.put("thumbnail_data", getThumbnailData(baseMapUrl, thumbnail));
        return keyValuePairMultimap;
    }

    public String createBaseMapUrl(String thumbnail) {
        try {
            String baseMapUrl = this.getConfig().getString("no.met.metsis.solr.thumbnail.baseMap.url");
            String baseMapLayer = this.getConfig().getString("no.met.metsis.solr.thumbnail.baseMap.layer");
            URIBuilder thumbnailURIBuilder = new URIBuilder(thumbnail);
            thumbnailURIBuilder.setParameter("LAYERS", baseMapLayer);
            //Do not use thumbnail WMS style. The base map server might not support it
            thumbnailURIBuilder.setParameter("STYLES", "");
            String sdl = this.getConfig().getString("no.met.metsis.solr.thumbnail.baseMap.sdl");
            URIBuilder basMapURIBuilder = new URIBuilder(baseMapUrl);

            basMapURIBuilder.addParameters(thumbnailURIBuilder.getQueryParams()).addParameter("sdl", sdl);
            return basMapURIBuilder.build().toString();//baseMapUrl + beforeLayers + "&LAYERS=" + baseMapLayer + withoutLayerName + "&sdl=" + sdl;
        } catch (URISyntaxException ex) {
            LOGGER.error("Invalid url");
        }
        return "";
    }

    public String generateThumbnailUrl(String wmsUrl, String providedFirstLayer) {
        try {
            String wh = this.getConfig().getString("no.met.metsis.solr.thumbnail.baseMap.widthHeight");
            List<NameValuePair> wmsNameValuePairs = new ArrayList<>();
            wmsNameValuePairs.add(new BasicNameValuePair("SERVICE", "WMS"));
            wmsNameValuePairs.add(new BasicNameValuePair("REQUEST", "GetMap"));
            wmsNameValuePairs.add(new BasicNameValuePair("VERSION", wmsVersion));
            wmsNameValuePairs.add(new BasicNameValuePair("FORMAT", "image/png"));
            wmsNameValuePairs.add(new BasicNameValuePair("WIDTH", wh));
            wmsNameValuePairs.add(new BasicNameValuePair("HEIGHT", wh));
            wmsNameValuePairs.add(new BasicNameValuePair("TRANSPARENT", "true"));

            URIBuilder thumbnailURIBuilder = new URIBuilder(wmsUrl);
            URIBuilder wmscURIBuilder = new URIBuilder(wmsUrl);
            wmscURIBuilder.addParameter("VERSION", "1.3.0").
                    addParameter("Request", "GetCapabilities").
                    addParameter("Service", "WMS").build();
            System.out.println("getCapabilitiesUrl: " + wmscURIBuilder.toString());
            WMSCapabilities wmsc = getWMSCapabilities(wmscURIBuilder.toString());
            if (wmsc == null) {
                return "";
            }
            Layer[] namedLayers = WMSUtils.getNamedLayers(wmsc);

            Layer thumbnailLayer = null;
            for (Layer namedLayer : namedLayers) {
                if (providedFirstLayer != null && namedLayer.getTitle().equalsIgnoreCase(providedFirstLayer)) {
                    thumbnailLayer = namedLayer;
                    break;
                }
            }
            //get first named layer if no layer is provided
            if (thumbnailLayer == null && namedLayers.length > 0) {
                thumbnailLayer = namedLayers[0];
            }

            wmsNameValuePairs.add(new BasicNameValuePair("LAYERS", thumbnailLayer.getName()));
            //
            String defaultDimension = getDefaultTimeDimension(thumbnailLayer);
            if (defaultDimension != null) {
                thumbnailURIBuilder.addParameter("TIME", defaultDimension).build();
                wmsNameValuePairs.add(new BasicNameValuePair("TIME", defaultDimension));
            }
            //}

            if (!thumbnailLayer.getBoundingBoxes().isEmpty()) {
                String crs = thumbnailLayer.getLayerBoundingBoxes().get(0).getSRSName();
                double maxX = thumbnailLayer.getLayerBoundingBoxes().get(0).getMaxX();
                double maxY = thumbnailLayer.getLayerBoundingBoxes().get(0).getMaxY();
                double minX = thumbnailLayer.getLayerBoundingBoxes().get(0).getMinX();
                double minY = thumbnailLayer.getLayerBoundingBoxes().get(0).getMinY();

                if (crs != null) {
                    wmsNameValuePairs.add(new BasicNameValuePair((wmsVersion.equalsIgnoreCase("1.3.0")) ? "CRS" : "SRS",
                            crs));
                    wmsNameValuePairs.add(new BasicNameValuePair("bbox", contriveEstimatedBounds(minX, minY, maxX, maxY)));
                }

            }
            //add styles anyway
            if (!thumbnailLayer.getStyles().isEmpty()) {
                String styleName = thumbnailLayer.getStyles().get(0).getName();
                wmsNameValuePairs.add(new BasicNameValuePair("STYLES", styleName));
            }
            return thumbnailURIBuilder.addParameters(wmsNameValuePairs).build().toString();
        } catch (URISyntaxException ex) {
            LOGGER.error("Invalid WMS url [" + wmsUrl + "]", ex);
        }
        return "";
    }

    private String getDefaultTimeDimension(Layer thumbnalLayer) {
        return thumbnalLayer.getDimension("time").getExtent().getDefaultValue();
    }

    private String contriveEstimatedBounds(double minX, double minY, double maxX, double maxY) {
        double x2 = maxX;
        double y2 = minY;
        double x4 = minX, y4 = maxY;

        double w = Math.sqrt(Math.pow((x2 - minX), 2) + Math.pow((y2 - minY), 2));
        double l = Math.sqrt(Math.pow((x4 - minX), 2) + Math.pow((y4 - minY), 2));

        double estMinX = minX - 2 * w;
        estMinX = (estMinX < -180) ? -180 : estMinX;
        double estMinY = minY - 2 * l;
        estMinY = (estMinY < -90) ? -90 : estMinY;
        double estMaxX = maxX + 2 * w;
        estMaxX = (estMaxX > 180) ? 180 : estMaxX;
        double estMaxY = maxY + 2 * l;
        estMaxY = (estMaxY > 90) ? 90 : estMaxY;
        return estMinX + "," + estMinY + "," + estMaxX + "," + estMaxY;
    }

    private String getThumbnailData(String baseMapUrl, String thumbnalUrl) {
        try {
            BufferedImage base = null;
            BufferedImage overlay = null;
            try {
                base = ImageIO.read(new URL(baseMapUrl));
            } catch (IOException ioe) {
                throw new IOException("Failed to get base map [" + baseMapUrl + "]");
            }
            try {
                overlay = ImageIO.read(new URL(thumbnalUrl));
            } catch (IOException ioe) {
                throw new IOException("Failed to get thumbnail [" + thumbnalUrl + "]");
            }

            if (base != null && overlay != null) {
                Graphics2D g2d = base.createGraphics();
                g2d.setComposite(AlphaComposite.SrcOver.derive(0.7f));
                int x = (base.getWidth() - overlay.getWidth()) / 2;
                int y = (base.getHeight() - overlay.getHeight()) / 2;
                g2d.drawImage(overlay, x, y, null);
                g2d.dispose();

                ByteArrayOutputStream output = new ByteArrayOutputStream();
                ImageIO.write(base, "png", output);
                String thumbnailData = DatatypeConverter.printBase64Binary(output.toByteArray());
                return "data:image/png;base64," + thumbnailData;
            }
        } catch (MalformedURLException ex) {
            LOGGER.error("Invalid url", ex);
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
        return "";
    }

    private WMSCapabilities getWMSCapabilities(String url) {
        WMSCapabilities wmsCapabilities = null;
        try {
            WebMapServer wms = new WebMapServer(new URL(url), 90);
            wmsCapabilities = wms.getCapabilities();
        } catch (IOException ex) {
            LOGGER.error("Failed to get WMS capabilities [" + url + "]", ex);
        } catch (ServiceException ex) {
            java.util.logging.Logger.getLogger(ThumbnailIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }

        return wmsCapabilities;
    }

    private String getWmsUrl(List<String> requiredFields, Multimap<String, String> metadataKeyValePair) {
        for (String requiredField : requiredFields) {
            if (metadataKeyValePair.containsKey(requiredField)) {
                Collection<String> get = metadataKeyValePair.get(requiredField);
                for (String resource : get) {
                    if (StringUtils.startsWith(resource, "\"OGC WMS\"")) {
                        return StringUtils.substringBetween(resource, ":", ",").replaceAll("\"", "");
                    }
                }
            }
        }
        return "";
    }

}
