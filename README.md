# 
# ARCHIVED DUE TO VULNERABILITIES IN THE DEPENDENCIES
#

# METSIS-metadata-ingestion

## Background
METSIS is the MET Norway Scientific Information System. METSIS is a collection of software packages offering data management functionality. In particular METSIS is addressing the following areas:
* Data documentation
* Data publication
* Data interoperability
* Data consumption

A focal issue in this context is to comply with existing and emerging standards within distributed data management. Typical technologies currently utilised include:
* OAI-PMH
* SolR
* XSLT
* SKOS
* OGC WMS
* OGC WPS
* OPeNDAP

The design principle of METSIS are based on the following:
* Modular
* Service Oriented

## Environment
METSIS metadata is a generic utility to index MMD dataset, thumbnails to solr. It is often used together with https://github.com/steingod/solrindexing. 

## Context
See https://pm.met.no/arctic-data-centre.

## Requirements
1. java >= 8

## How to run
```
$java -jar metsis-metadata-jar-with-dependencies.jar <COMMAND> <Parameters>
```
To list all commands and options
```
$java -jar metsis-metadata-jar-with-dependencies.jar
```
To index MMD dataset
```
$java -jar metsis-metadata-jar-with-dependencies.jar index-metadata --level l1 --server http://server:port/solr/coreName --sourceDirectory /path/to/MMD-datasets
```
To index a single MMD dataset
```
$java -jar metsis-metadata-jar-with-dependencies.jar index-single-metadata --level l1 --server http://server:port/solr/coreName ----metadataFile /path/to/MMD-dataset/file
```
To index thumbnails
```
$java -jar metsis-metadata-jar-with-dependencies.jar index-thumbnail --server http://server:port/solr/coreName --sourceDirectory /path/to/MMD-datasets
```
To clear an index
```
$java -jar metsis-metadata-jar-with-dependencies.jar clear --server http://server:port/solr/coreName
```
