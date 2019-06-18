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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.xmlbeans.XmlException;
import org.jdom2.JDOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class MetadataManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class.getName());
    private static final String LOCAL_APP_CONFIG_ENV = "METADATA_CONFIG";

    @Parameters(commandDescription = "Index metadata")
    private static class IndexCommand {

        @Parameter(names = "--sourceDirectory", required = true,
                description = "Source directory that contains mmd files",
                validateWith = FilePathValidator.class)
        private String sourceDirectory;

        @Parameter(names = "--server",
                description = "URL to the Solr server, including the core name [http://SERVER:PORT/solr/CORE_NAME]",
                required = true)
        private String server;

        @Parameter(names = "--level",
                description = "Dataset level [supported values: l1, l2]",
                required = true,
                validateWith = DatasetLevelValidator.class)
        private String level;

        @Parameter(names = "--includeRelatedDataset",
                description = "A boolean flag to include related dataset while indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String includeRelatedDataset;

        @Parameter(names = "--dryRun",
                description = "A boolean flag to run app without performing actaul indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String dryRun;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();

    }

    @Parameters(commandDescription = "Index metadata")
    private static class IndexSingleMetadataCommand {

        @Parameter(names = "--metadataFile", required = true,
                description = "Absolute path to metadata file in MMD format",
                validateWith = FilePathValidator.class)
        private String metadataFile;

        @Parameter(names = "--server",
                description = "URL to the Solr server, including the core name [http://SERVER:PORT/solr/CORE_NAME]",
                required = true)
        private String server;

        @Parameter(names = "--level",
                description = "Dataset level [supported values: l1, l2]",
                required = true,
                validateWith = DatasetLevelValidator.class)
        private String level;

        @Parameter(names = "--includeRelatedDataset",
                description = "A boolean flag to include related dataset while indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String includeRelatedDataset;

        @Parameter(names = "--dryRun",
                description = "A boolean flag to run app without performing actaul indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String dryRun;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();

    }

    @Parameters(commandDescription = "Index thumbnail")
    private static class IndexThumbnailCommand {

        @Parameter(names = "--sourceDirectory", required = true,
                description = "Source directory that contains mmd files (Typically l1 datasets if there are levels)",
                validateWith = FilePathValidator.class)
        private String sourceDirectory;

        @Parameter(names = "--server",
                description = "URL to the Solr server, including the core name [http://SERVER:PORT/solr/CORE_NAME]",
                required = true)
        private String server;

        @Parameter(names = "--wmsVersion",
                description = "Verion of the target WMS server. Use 1.3.0 or 1.1.0",
                validateWith = WMSVersionValidator.class,
                required = true)
        private String wmsVersion;

        @Parameter(names = "--dryRun",
                description = "A boolean flag to run app without performing actaul indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String dryRun;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();
    }

    @Parameters(commandDescription = "Index a single thumbnail from a single dateset")
    private static class IndexSingleThumbnailCommand {

        @Parameter(names = "--metadataFile", required = true,
                description = "Absolute path to metadata file in MMD format",
                validateWith = FilePathValidator.class)
        private String metadataFile;

        @Parameter(names = "--server",
                description = "URL to the Solr server, including the core name [http://SERVER:PORT/solr/CORE_NAME]",
                required = true)
        private String server;

        @Parameter(names = "--wmsVersion",
                description = "Verion of the target WMS server. Use 1.3.0 or 1.1.0",
                validateWith = WMSVersionValidator.class,
                required = true)
        private String wmsVersion;

        @Parameter(names = "--dryRun",
                description = "A boolean flag to run app without performing actaul indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String dryRun;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();
    }

    @Parameters(commandDescription = "Index a single netcdf feature from a single dateset")
    private static class IndexSingleFeatureCommand {

        @Parameter(names = "--metadataFile", required = true,
                description = "Absolute path to metadata file in MMD format",
                validateWith = FilePathValidator.class)
        private String metadataFile;

        @Parameter(names = "--server",
                description = "URL to the Solr server, including the core name [http://SERVER:PORT/solr/CORE_NAME]",
                required = true)
        private String server;

        @Parameter(names = "--dryRun",
                description = "A boolean flag to run app without performing actaul indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String dryRun;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();

    }

    @Parameters(commandDescription = "Index netcdf feature")
    private static class IndexFeatureCommand {

        @Parameter(names = "--sourceDirectory", required = true,
                description = "Source directory that contains mmd files (Typically l1 datasets if there are levels)",
                validateWith = FilePathValidator.class)
        private String sourceDirectory;

        @Parameter(names = "--server",
                description = "URL to the Solr server, including the core name [http://SERVER:PORT/solr/CORE_NAME]",
                required = true)
        private String server;

        @Parameter(names = "--dryRun",
                description = "A boolean flag to run app without performing actaul indexing",
                required = false,
                validateWith = BooleanFlagValidator.class)
        private String dryRun;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();
    }

    @Parameters(commandDescription = "Create solr schema,")
    private static class CreateSchemaCommand {

        @Parameter(names = "--solrVersion",
                description = "Version of target solr server. Supported values are [5, 7]",
                required = true,
                validateWith = SolrVersionValidator.class)
        private int solrVersion;

        @Parameter(names = "--destinationDirectory", required = true,
                description = "Destination directory where solr schema will be created",
                validateWith = FilePathValidator.class)
        private String destinationDirectory;

        @ParametersDelegate
        private final CommandLogAppender appender = new CommandLogAppender();

    }

    @Parameters(commandDescription = "Clear the entire index of a particular core")
    private static class CommandClear {

        @Parameter(names = "--server", description = "URL to the Solr server, including the core name", required = true)
        private String server;

    }

    @Parameters(commandDescription = "Clear the entire index of a particular core")
    private static class CommandLogAppender {

        @Parameter(names = "--appender", description = "Log appender. Suppoted values are [file, console]", required = false)
        private String logAppender;

    }

    public static void main(String[] args) throws XPathExpressionException, ParserConfigurationException,
            SAXException, SolrServerException, IOException, JDOMException, XmlException {

        JCommander jc = new JCommander();
        IndexCommand indexCommand = new IndexCommand();
        jc.addCommand("index-metadata", indexCommand);
        IndexThumbnailCommand indexThumbnailCommand = new IndexThumbnailCommand();
        jc.addCommand("index-thumbnail", indexThumbnailCommand);
        IndexSingleMetadataCommand ismc = new IndexSingleMetadataCommand();
        jc.addCommand("index-single-metadata", ismc);
        IndexSingleThumbnailCommand istc = new IndexSingleThumbnailCommand();
        jc.addCommand("index-single-thumbnail", istc);
        IndexSingleFeatureCommand isfc = new IndexSingleFeatureCommand();
        jc.addCommand("index-single-feature", isfc);
        IndexFeatureCommand ifc = new IndexFeatureCommand();
        jc.addCommand("index-feature", ifc);
        CreateSchemaCommand schemaCommand = new CreateSchemaCommand();
        jc.addCommand("create-schema", schemaCommand);
        CommandClear clearCommand = new CommandClear();
        jc.addCommand("clear", clearCommand);
        //CommandLogAppender appenderCommand = new CommandLogAppender();
        //jc.addCommand("appender", appenderCommand);
        jc.parse(args);

        Config envConfig = ConfigFactory.systemEnvironment();
        String localAppConfigPath = "";
        try {
            localAppConfigPath = envConfig.getString(LOCAL_APP_CONFIG_ENV);
        } catch (ConfigException.Missing mex) {
            LOGGER.info("Running with default application configurations!");
            //use default
        }
        Config defaultAppConfig = ConfigFactory.load("application");
        Config customAppConfig = ConfigFactory.parseFile(new File(localAppConfigPath))
                .withFallback(defaultAppConfig);
        //Config config = customAppConfig.getConfig("no.met.metsis.solr.jsonize");

        //enable file based logging
        if ("file".equalsIgnoreCase(schemaCommand.appender.logAppender)
                || "file".equalsIgnoreCase(indexCommand.appender.logAppender)
                || "file".equalsIgnoreCase(indexThumbnailCommand.appender.logAppender)
                || "file".equalsIgnoreCase(ismc.appender.logAppender)
                || "file".equalsIgnoreCase(istc.appender.logAppender)
                || "file".equalsIgnoreCase(ifc.appender.logAppender)
                || "file".equalsIgnoreCase(isfc.appender.logAppender)) {
            try {
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset();
                configurator.doConfigure(MetadataManager.class.getResourceAsStream("/secondary-logback.xml"));
                StatusPrinter.printInCaseOfErrorsOrWarnings(context);
            } catch (JoranException ex) {
                LOGGER.warn(ex.getMessage());
            }
        }

        if ("index-metadata".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Indexing metadata to [ " + indexCommand.server + " ]");
            List<String> requiredFields = customAppConfig.getStringList("no.met.metsis.solr.requiredFields");
            HttpSolrClient httpSolrClient = new HttpSolrClient(indexCommand.server);
            httpSolrClient.setParser(new XMLResponseParser());
 /*
            MetMetadata metMetadata = new MetMetadata(indexCommand.level);
            metMetadata.solrClient(httpSolrClient);
            metMetadata.setConfig(customAppConfig);
            metMetadata.index(Paths.get(indexCommand.sourceDirectory), requiredFields);
             */
            MMDIndexer mmdi = new MMDIndexer(httpSolrClient, customAppConfig,
                    Boolean.valueOf(indexCommand.includeRelatedDataset), Boolean.valueOf(indexCommand.dryRun));
            mmdi.setLevel(indexCommand.level);
            mmdi.index(Paths.get(indexCommand.sourceDirectory), requiredFields);
        } else if ("index-single-metadata".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Indexing single metadata to [ " + ismc.server + " ]");
            List<String> requiredFields = customAppConfig.getStringList("no.met.metsis.solr.requiredFields");
            HttpSolrClient httpSolrClient = new HttpSolrClient(ismc.server);
            httpSolrClient.setParser(new XMLResponseParser());

//            MetMetadata metMetadata = new MetMetadata(ismc.level);
//            metMetadata.solrClient(httpSolrClient);
//            metMetadata.setConfig(customAppConfig);
            MMDIndexer mmdi = new MMDIndexer(httpSolrClient, customAppConfig,
                    Boolean.valueOf(ismc.includeRelatedDataset), Boolean.valueOf(ismc.dryRun));
            mmdi.setLevel(ismc.level);
            String metadataXML = new String(Files.readAllBytes(Paths.get(ismc.metadataFile)));
            mmdi.index(FilenameUtils.removeExtension(ismc.metadataFile), metadataXML, requiredFields);
        } else if ("index-thumbnail".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Indexing thumnails to [ " + indexThumbnailCommand.server + " ]");
            List<String> requiredFields = customAppConfig.getStringList("no.met.metsis.solr.thumbnail.requiredFields");
            HttpSolrClient httpSolrClient = new HttpSolrClient(indexThumbnailCommand.server);
            httpSolrClient.setParser(new XMLResponseParser());

            Metadata metMetadata = new ThumbnailIndexer(indexThumbnailCommand.wmsVersion, Boolean.valueOf(indexThumbnailCommand.dryRun));
            metMetadata.solrClient(httpSolrClient);
            metMetadata.setConfig(customAppConfig);
            metMetadata.index(Paths.get(indexThumbnailCommand.sourceDirectory), requiredFields);
        } else if ("index-single-thumbnail".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Indexing a single thumnail to [ " + istc.server + " ]");
            List<String> requiredFields = customAppConfig.getStringList("no.met.metsis.solr.thumbnail.requiredFields");
            HttpSolrClient httpSolrClient = new HttpSolrClient(istc.server);
            httpSolrClient.setParser(new XMLResponseParser());

            ThumbnailIndexer thumbnailIndexer = new ThumbnailIndexer(istc.wmsVersion, Boolean.valueOf(indexThumbnailCommand.dryRun));
            thumbnailIndexer.solrClient(httpSolrClient);
            thumbnailIndexer.setConfig(customAppConfig);
            String metadataXML = new String(Files.readAllBytes(Paths.get(istc.metadataFile)));
            thumbnailIndexer.index(metadataXML, requiredFields);
        } else if ("index-feature".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Indexing a single netcdf feature to [ " + ifc.server + " ]");
            List<String> requiredFields = new ArrayList<>(); //customAppConfig.getStringList("no.met.metsis.solr.thumbnail.requiredFields");
            HttpSolrClient httpSolrClient = new HttpSolrClient(ifc.server);
            httpSolrClient.setParser(new XMLResponseParser());

            NetCDFFeatureTypeIndexer typeIndexer = new NetCDFFeatureTypeIndexer(Boolean.valueOf(ifc.dryRun));
            typeIndexer.solrClient(httpSolrClient);
            typeIndexer.setConfig(customAppConfig);
            typeIndexer.index(Paths.get(ifc.sourceDirectory), requiredFields);
        } else if ("index-single-feature".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Indexing a single netcdf feature to [ " + isfc.server + " ]");
            List<String> requiredFields = new ArrayList<>(); //customAppConfig.getStringList("no.met.metsis.solr.thumbnail.requiredFields");
            HttpSolrClient httpSolrClient = new HttpSolrClient(isfc.server);
            httpSolrClient.setParser(new XMLResponseParser());

            NetCDFFeatureTypeIndexer typeIndexer = new NetCDFFeatureTypeIndexer(Boolean.valueOf(isfc.dryRun));
            typeIndexer.solrClient(httpSolrClient);
            typeIndexer.setConfig(customAppConfig);
            String metadataXML = new String(Files.readAllBytes(Paths.get(isfc.metadataFile)));
            typeIndexer.index(metadataXML, requiredFields);
        } else if ("clear".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Clearing the solr index at " + clearCommand.server);
            HttpSolrClient httpSolrClient = new HttpSolrClient(clearCommand.server);
            httpSolrClient.setParser(new XMLResponseParser());
            httpSolrClient.deleteByQuery("*:*");
            httpSolrClient.commit();
        } else if ("create-schema".equalsIgnoreCase(jc.getParsedCommand())) {
            LOGGER.info("Creating solr schema at " + schemaCommand.destinationDirectory);
            List<String> fullTextFields = customAppConfig.getStringList("no.met.metsis.solr.fullTextFields");
            List<String> facetFields = customAppConfig.getStringList("no.met.metsis.solr.facetFields");
            SchemaOutput.outputSolrSchema(schemaCommand.solrVersion, Paths.get(schemaCommand.destinationDirectory), fullTextFields, facetFields);
        } else {
            jc.usage();
        }
        System.out.println("Done!");
    }
}
