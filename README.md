# metsis-metadata
METSIS metadata is a generic utility to index MMD dataset, thumbnails to solr.

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
