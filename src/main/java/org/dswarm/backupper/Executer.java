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
package org.dswarm.backupper;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.common.types.Tuple;

/**
 * @author tgaengler
 */
public class Executer {

	private static final Logger LOG = LoggerFactory.getLogger(Executer.class);

	private static final String DSWARM_BACKEND_API_BASE_URI_PARAMETER = "-dswarm-backend-api";
	private static final String EXPORT_DIRECTORY_NAME_PARAMETER = "-export-directory-name";
	private static final String HELP_PARAMETER = "--help";

	private static final String EQUALS = "=";

	private static final StringBuilder HELP_SB = new StringBuilder();

	private static final String HELP;

	static {

		HELP_SB.append("\n")
				.append("this is the d:swarm backupper").append("\n\n")
				.append("\t").append("this tool is intended for backupping Projects and the content of their (input) Data Models and replaying them on running d:swarm instances").append("\n\n")
				.append("following parameters are available for configuration at the moment:").append("\n\n")
				.append("\t").append(DSWARM_BACKEND_API_BASE_URI_PARAMETER).append(" : the d:swarm backend API base URI").append("\n")
				.append("\t").append(EXPORT_DIRECTORY_NAME_PARAMETER).append(" : the name of the export directory (absolute path)").append("\n\n")
				.append("\t").append(HELP_PARAMETER).append(" : prints this help").append("\n\n")
				.append("have fun with this tool!").append("\n\n")
				.append("if you observe any problems with this tool or have questions about handling this tool etc. don't hesitate to contact us").append("\n")
				.append("(you can find our contact details at http://dswarm.org)").append("\n");

		HELP = HELP_SB.toString();
	}

	private static void executeExport(final String dswarmBackendAPIBaseURI, final String exportDirectoryName) {

		final ProjectExporter projectExporter = new ProjectExporter(dswarmBackendAPIBaseURI);

		final Observable<String> projectDescriptionJSONStringObservable = projectExporter.exportProjects(exportDirectoryName);

		final AtomicInteger counter = new AtomicInteger(0);

		Iterable<String> projectIds = projectDescriptionJSONStringObservable
				.doOnNext(projectDescriptionJSONString -> counter.incrementAndGet())
				.doOnCompleted(() -> LOG.info("exported '{}' projects from '{}' to '{}'", counter.get(), dswarmBackendAPIBaseURI, exportDirectoryName))
				.toBlocking().toIterable();

		projectIds.forEach(System.out::println);
	}

	public static void main(final String[] args) {

		// 0. read path from arguments
		if (args == null || args.length <= 0) {

			LOG.error("cannot execute export - no d:swarm backend API base URI and export directory name are given as commandline parameter");

			return;
		}

		if (args.length == 1 && HELP_PARAMETER.equals(args[0])) {

			printHelp();

			return;
		}

		final Map<String, String> argMap = parseArgs(args);

		final String dswarmBackendAPIBaseURI = argMap.get(DSWARM_BACKEND_API_BASE_URI_PARAMETER);
		final String exportDirectoryName = argMap.get(EXPORT_DIRECTORY_NAME_PARAMETER);

		LOG.info("d:swarm backend API base URI = '{}'", dswarmBackendAPIBaseURI);
		LOG.info("export directory name = '{}'", exportDirectoryName);

		try {

			executeExport(dswarmBackendAPIBaseURI, exportDirectoryName);
		} catch (final Exception e) {

			LOG.error("something went wrong at import execution.", e);

			System.out.println("\n" + HELP);
		}
	}

	private static void printHelp() {

		System.out.println(HELP);
	}

	private static Map<String, String> parseArgs(final String[] args) {

		return Arrays.asList(args)
				.stream()
				.map(arg -> {
					try {

						return parseArg(arg);
					} catch (final DswarmBackupperException e) {

						throw DswarmBackupperError.wrap(e);
					}
				})
				.collect(Collectors.toMap(Tuple::v1, Tuple::v2));
	}

	private static Tuple<String, String> parseArg(final String arg) throws DswarmBackupperException {

		if (!arg.contains(EQUALS)) {

			final String message = String.format("argument '%s' is in a wrong format; argument format should be -[KEY]=[VALUE]", arg);

			LOG.error(message);

			throw new DswarmBackupperException(message);
		}

		final String[] split = arg.split(EQUALS);

		return Tuple.tuple(split[0], split[1]);
	}
}
