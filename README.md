# D:SWARM Tools #

...for backupping Projects and the content of their (input) Data Models and replaying them on running d:swarm instances (i.e. no full replacement of Metadata Repository and Data Hub is necessary).

## Build JAR from Sources

    mvn clean package


## Projects Management

### Projects Export

execute projects export:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.exporter.ProjectsExportExecuter -dswarm-backend-api=[BASE_URI_OF_YOUR_DSWARM_BACKEND_API] -export-directory-name=[DIRECTORY_WHERE_THE_EXPORTED_PROJECTS_SHOULD_BE_STORED]

display help of projects export tool:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.exporter.ProjectsExportExecuter --help

### Projects Import

execute projects import:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.importer.ProjectsImportExecuter -dswarm-backend-api=[BASE_URI_OF_YOUR_DSWARM_BACKEND_API] -import-directory-name=[DIRECTORY_WHERE_THE_PROJECTS_THAT_SHOULD_BE_IMPORTED_ARE_STORED]

display help of projects import tool:

    java -cp dswarm-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.dswarm.tools.importer.ProjectsImportExecuter --help 

**note**: please don't forget ro replace the variable parts (i.e. that one in '[]') with concrete things