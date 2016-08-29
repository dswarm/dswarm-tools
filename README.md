# D:SWARM Tools #

...for backupping Projects and the content of their (input) Data Models and replaying them on running d:swarm instances (i.e. no full replacement of Metadata Repository and Data Hub is necessary).

## Build JAR from Sources

    mvn clean package


## (Meta) Data Management

### Export

#### Projects Export

execute projects export:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.exporter.ProjectsExportExecuter -dswarm-backend-api=[BASE_URI_OF_YOUR_DSWARM_BACKEND_API] -export-directory-name=[DIRECTORY_WHERE_THE_EXPORTED_PROJECTS_SHOULD_BE_STORED]

display help of projects export tool:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.exporter.ProjectsExportExecuter --help

#### Data Models (Content) Export

execute data models content export:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.exporter.DataModelsContentExportExecuter -dswarm-backend-api=[BASE_URI_OF_YOUR_DSWARM_BACKEND_API] -dswarm-graph-extension-api=[BASE_URI_OF_YOUR_DSWARM_GRAPH_EXTENSION_API] -export-directory-name=[DIRECTORY_WHERE_THE_EXPORTED_DATA_MODELS_CONTENT_SHOULD_BE_STORED]

display help of data models content export tool:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.exporter.DataModelsContentExportExecuter --help

### Import

#### Projects Import

execute projects import:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.importer.ProjectsImportExecuter -dswarm-backend-api=[BASE_URI_OF_YOUR_DSWARM_BACKEND_API] -import-directory-name=[DIRECTORY_WHERE_THE_PROJECTS_THAT_SHOULD_BE_IMPORTED_ARE_STORED]

display help of projects import tool:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.importer.ProjectsImportExecuter --help 

#### Data Models (Content) Import

execute data models content import:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.importer.DataModelsContentImportExecuter -dswarm-backend-api=[BASE_URI_OF_YOUR_DSWARM_BACKEND_API] -dswarm-graph-extension-api=[BASE_URI_OF_YOUR_DSWARM_GRAPH_EXTENSION_API] -import-directory-name=[DIRECTORY_WHERE_THE_DATA_MODELS_CONTENT_THAT_SHOULD_BE_IMPORTED_IS_STORED]

display help of data models content import tool:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.importer.DataModelsContentImportExecuter --help 

The data models content import requires the metadata of the data models (i.e. the data model description) upfront in the instance, where the data models content should be imported (since this task makes use of this information). You can get this metadata into your d:swarm instance via projects import, i.e., first execute projects import and then data models (content) import.

**note**: please don't forget to replace the variable parts (i.e. that one in '[]') with concrete things