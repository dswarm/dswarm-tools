/**
 * Copyright © 2016 SLUB Dresden (<code@dswarm.org>)
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.tools.AbstractExecuter;
import org.dswarm.tools.DswarmToolsStatics;

/**
 * To be able to execute the Projects backup via commandline.
 *
 * (incl. printable help)
 *
 * @author tgaengler
 */
public class ProjectsExportExecuter extends AbstractExecuter {

	private static final Logger LOG = LoggerFactory.getLogger(ProjectsExportExecuter.class);

	private static final StringBuilder HELP_SB = new StringBuilder();

	static {

		HELP_SB.append("\n")
				.append("this is the d:swarm projects exporter").append("\n\n")
				.append("\t").append("this tool is intended for exporting Projects from a running d:swarm instance (that can be imported to this or another d:swarm instance)").append("\n\n")
				.append("following parameters are available for configuration at the moment:").append("\n\n")
				.append("\t").append(DswarmToolsStatics.DSWARM_BACKEND_API_BASE_URI_PARAMETER).append(" : the d:swarm backend API base URI").append("\n")
				.append("\t").append(DswarmToolsStatics.EXPORT_DIRECTORY_NAME_PARAMETER).append(" : the name of the export directory (absolute path)").append("\n\n")
				.append("\t").append(DswarmToolsStatics.HELP_PARAMETER).append(" : prints this help").append("\n\n")
				.append("have fun with this tool!").append("\n\n")
				.append("if you observe any problems with this tool or have questions about handling this tool etc. don't hesitate to contact us").append("\n")
				.append("(you can find our contact details at http://dswarm.org)").append("\n");

		HELP = HELP_SB.toString();
	}

	private static void executeExport(final String dswarmBackendAPIBaseURI, final String exportDirectoryName) {

		final ProjectsExporter projectsExporter = new ProjectsExporter(dswarmBackendAPIBaseURI);

		final Observable<String> projectDescriptionJSONStringObservable = projectsExporter.exportObjects(exportDirectoryName);

		final AtomicInteger counter = new AtomicInteger(0);

		Iterable<String> projectDescriptions = projectDescriptionJSONStringObservable
				.doOnNext(projectDescriptionJSONString -> counter.incrementAndGet())
				.doOnCompleted(() -> LOG.info("exported '{}' projects from '{}' to '{}'", counter.get(), dswarmBackendAPIBaseURI, exportDirectoryName))
				.doOnCompleted(() -> System.exit(0))
				.toBlocking().toIterable();

		projectDescriptions.forEach(projectDescription -> LOG.trace("exported project description '{}'", projectDescription));
	}

	public static void main(final String[] args) {

		// 0. read path from arguments
		if (args == null || args.length <= 0) {

			LOG.error("cannot execute export - no d:swarm backend API base URI and export directory name are given as commandline parameter");

			return;
		}

		if (args.length == 1 && DswarmToolsStatics.HELP_PARAMETER.equals(args[0])) {

			printHelp();

			return;
		}

		final Map<String, String> argMap = parseArgs(args);

		final String dswarmBackendAPIBaseURI = argMap.get(DswarmToolsStatics.DSWARM_BACKEND_API_BASE_URI_PARAMETER);
		final String exportDirectoryName = argMap.get(DswarmToolsStatics.EXPORT_DIRECTORY_NAME_PARAMETER);

		LOG.info("d:swarm backend API base URI = '{}'", dswarmBackendAPIBaseURI);
		LOG.info("export directory name = '{}'", exportDirectoryName);

		try {

			executeExport(dswarmBackendAPIBaseURI, exportDirectoryName);
		} catch (final Exception e) {

			LOG.error("something went wrong at export execution.", e);

			System.out.println("\n" + HELP);
		}
	}
}
