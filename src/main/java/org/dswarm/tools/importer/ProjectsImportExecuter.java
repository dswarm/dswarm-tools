/**
 * Copyright (C) 2016 SLUB Dresden (<code@dswarm.org>)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dswarm.tools.importer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.common.types.Tuple;
import org.dswarm.tools.AbstractExecuter;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.DswarmToolsStatics;

/**
 * @author tgaengler
 */
public class ProjectsImportExecuter extends AbstractExecuter {

	private static final Logger LOG = LoggerFactory.getLogger(ProjectsImportExecuter.class);

	private static final StringBuilder HELP_SB = new StringBuilder();

	static {

		HELP_SB.append("\n")
				.append("this is the d:swarm projects importer").append("\n\n")
				.append("\t").append("this tool is intended for importing Projects to a running d:swarm instance (that have been exported from this or another d:swarm instance)").append("\n\n")
				.append("following parameters are available for configuration at the moment:").append("\n\n")
				.append("\t").append(DswarmToolsStatics.DSWARM_BACKEND_API_BASE_URI_PARAMETER).append(" : the d:swarm backend API base URI").append("\n")
				.append("\t").append(DswarmToolsStatics.IMPORT_DIRECTORY_NAME_PARAMETER).append(" : the name of the import directory (absolute path), i.e., where the files are located that should be imported").append("\n\n")
				.append("\t").append(DswarmToolsStatics.HELP_PARAMETER).append(" : prints this help").append("\n\n")
				.append("have fun with this tool!").append("\n\n")
				.append("if you observe any problems with this tool or have questions about handling this tool etc. don't hesitate to contact us").append("\n")
				.append("(you can find our contact details at http://dswarm.org)").append("\n");

		HELP = HELP_SB.toString();
	}

	private static void executeImport(final String dswarmBackendAPIBaseURI, final String importDirectoryName) throws DswarmToolsException {

		final ProjectImporter projectImporter = new ProjectImporter(dswarmBackendAPIBaseURI);

		final Observable<Tuple<String, String>> projectDescriptionTupleObservable = projectImporter.importProjects(importDirectoryName);

		final AtomicInteger counter = new AtomicInteger(0);

		Iterable<Tuple<String, String>> projectDescriptionTuples = projectDescriptionTupleObservable
				.doOnNext(projectDescriptionTuple -> counter.incrementAndGet())
				.doOnNext(projectDescriptionTuple1 -> LOG.debug("imported project '{}' to '{}'", projectDescriptionTuple1.v1(), dswarmBackendAPIBaseURI))
				.doOnCompleted(() -> LOG.info("imported '{}' projects from '{}' to '{}'", counter.get(), importDirectoryName, dswarmBackendAPIBaseURI))
				.toBlocking().toIterable();

		projectDescriptionTuples.forEach(projectDescriptionTuple2 -> System.out.println(projectDescriptionTuple2.v2()));
	}

	public static void main(final String[] args) {

		// 0. read path from arguments
		if (args == null || args.length <= 0) {

			LOG.error("cannot execute import - no d:swarm backend API base URI and import directory name are given as commandline parameter");

			return;
		}

		if (args.length == 1 && DswarmToolsStatics.HELP_PARAMETER.equals(args[0])) {

			printHelp();

			return;
		}

		final Map<String, String> argMap = parseArgs(args);

		final String dswarmBackendAPIBaseURI = argMap.get(DswarmToolsStatics.DSWARM_BACKEND_API_BASE_URI_PARAMETER);
		final String importDirectoryName = argMap.get(DswarmToolsStatics.IMPORT_DIRECTORY_NAME_PARAMETER);

		LOG.info("d:swarm backend API base URI = '{}'", dswarmBackendAPIBaseURI);
		LOG.info("import directory name = '{}'", importDirectoryName);

		try {

			executeImport(dswarmBackendAPIBaseURI, importDirectoryName);
		} catch (final Exception e) {

			LOG.error("something went wrong at import execution.", e);

			System.out.println("\n" + HELP);
		}
	}
}
