/**
 * Copyright © 2016 – 2017 SLUB Dresden (<code@dswarm.org>)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dswarm.tools.exporter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.tools.AbstractExecuter;
import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.apiclients.DswarmProjectsAPIClient;
import org.dswarm.tools.utils.DswarmToolUtils;

/**
 * To be able to execute the Data Models content backup via commandline.
 *
 * (incl. printable help)
 *
 * @author tgaengler
 */
public class DataModelsContentExportExecuter extends AbstractExecuter {

	private static final Logger LOG = LoggerFactory.getLogger(DataModelsContentExportExecuter.class);

	private static final StringBuilder HELP_SB = new StringBuilder();

	static {

		HELP_SB.append("\n")
				.append("this is the d:swarm data models content exporter").append("\n\n")
				.append("\t").append("this tool is intended for exporting content from Data Models from a running d:swarm instance (that can be imported to this or another d:swarm instance)").append("\n\n")
				.append("following parameters are available for configuration at the moment:").append("\n\n")
				.append("\t").append(DswarmToolsStatics.DSWARM_BACKEND_API_BASE_URI_PARAMETER).append(" : the d:swarm backend API base URI").append("\n")
				.append("\t").append(DswarmToolsStatics.DSWARM_GRAPH_EXTENSION_API_BASE_URI_PARAMETER).append(" : the d:swarm graph extension API base URI").append("\n")
				.append("\t").append(DswarmToolsStatics.EXPORT_DIRECTORY_NAME_PARAMETER).append(" : the name of the export directory (absolute path)").append("\n\n")
				.append("\t").append(DswarmToolsStatics.HELP_PARAMETER).append(" : prints this help").append("\n\n")
				.append("have fun with this tool!").append("\n\n")
				.append("if you observe any problems with this tool or have questions about handling this tool etc. don't hesitate to contact us").append("\n")
				.append("(you can find our contact details at http://dswarm.org)").append("\n");

		HELP = HELP_SB.toString();
	}

	private static void executeExport(final String dswarmBackendAPIBaseURI, final String dswarmGraphExtensionAPIBaseURI, final String exportDirectoryName) {

		final DswarmProjectsAPIClient dswarmProjectsAPIClient = new DswarmProjectsAPIClient(dswarmBackendAPIBaseURI);
		final DataModelsContentExporter dataModelsContentExporter = new DataModelsContentExporter(dswarmGraphExtensionAPIBaseURI);

		// fetch input data model identifiers + record class URIs of input schemata
		final Observable<Tuple2<String, String>> readDataModelRequestInputTupleObservable = dswarmProjectsAPIClient.fetchObjects()
				.map(projectTuple -> {

					final String projectIdentifier = projectTuple._1;
					final String projectJSONString = projectTuple._2;

					final String errorMessage = String.format("something went wrong, while deserializing project '%s'", projectIdentifier);

					return DswarmToolUtils.deserializeAsObjectNode(projectJSONString, errorMessage);
				})
				.map(projectJSON -> {

					final JsonNode inputDataModel = projectJSON.get(DswarmToolsStatics.INPUT_DATA_MODEL_IDENTIFIER);

					final String inputDataModelID = inputDataModel.get(DswarmToolsStatics.UUID_IDENTIFIER).asText();

					final String inputSchemaRecordClassURI = DswarmToolUtils.getRecordClassURI(inputDataModel);

					return Tuple.of(inputDataModelID, inputSchemaRecordClassURI);
				})
				.distinct();

		final Observable<String> dataModelContentJSONStringObservable = dataModelsContentExporter.exportObjectsContent(exportDirectoryName, readDataModelRequestInputTupleObservable);

		final AtomicInteger counter = new AtomicInteger(0);

		Iterable<String> dataModelDescriptions = dataModelContentJSONStringObservable
				.doOnNext(dataModelDescriptionJSONString -> counter.incrementAndGet())
				.doOnCompleted(() -> LOG.info("exported content from '{}' data models from '{}' to '{}'", counter.get(), dswarmGraphExtensionAPIBaseURI, exportDirectoryName))
				.doOnCompleted(() -> System.exit(0))
				.toBlocking().toIterable();

		dataModelDescriptions.forEach(dataModelDescription -> LOG.trace("exported data model description '{}'", dataModelDescription));
	}

	public static void main(final String[] args) {

		// 0. read path from arguments
		if (args == null || args.length <= 0) {

			LOG.error("cannot execute export - no d:swarm backend API base URI and d:swarm graph extension API base URI and export directory name are given as commandline parameter");

			return;
		}

		if (args.length == 1 && DswarmToolsStatics.HELP_PARAMETER.equals(args[0])) {

			printHelp();

			return;
		}

		final Map<String, String> argMap = parseArgs(args);

		final String dswarmBackendAPIBaseURI = argMap.get(DswarmToolsStatics.DSWARM_BACKEND_API_BASE_URI_PARAMETER);
		final String dswarmGraphExtensionAPIBaseURI = argMap.get(DswarmToolsStatics.DSWARM_GRAPH_EXTENSION_API_BASE_URI_PARAMETER);
		final String exportDirectoryName = argMap.get(DswarmToolsStatics.EXPORT_DIRECTORY_NAME_PARAMETER);

		LOG.info("d:swarm backend API base URI = '{}'", dswarmBackendAPIBaseURI);
		LOG.info("d:swarm graph extension API base URI = '{}'", dswarmGraphExtensionAPIBaseURI);
		LOG.info("export directory name = '{}'", exportDirectoryName);

		try {

			executeExport(dswarmBackendAPIBaseURI, dswarmGraphExtensionAPIBaseURI, exportDirectoryName);
		} catch (final Exception e) {

			LOG.error("something went wrong at export execution.", e);

			System.out.println("\n" + HELP);
		}
	}
}
