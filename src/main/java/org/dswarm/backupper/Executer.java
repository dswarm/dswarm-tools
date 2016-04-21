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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * @author tgaengler
 */
public class Executer {

	private static final Logger LOG = LoggerFactory.getLogger(Executer.class);

	private static void executeExport(final String dswarmBackendAPIBaseURI, final String exportDirectoryName) {

		final ProjectExporter projectExporter = new ProjectExporter(dswarmBackendAPIBaseURI);

		final Observable<String> projectDescriptionJSONStringObservable = projectExporter.exportProjects(exportDirectoryName);

		Iterable<String> projectIds = projectDescriptionJSONStringObservable.toBlocking().toIterable();

		projectIds.forEach(System.out::println);
	}

	public static void main(final String[] args) {

		// 0. read path from arguments
		if (args == null || args.length <= 0) {

			LOG.error("cannot execute export - no d:swarm backend API base URI and export directory name are given as commandline parameter");

			return;
		}

		final String dswarmBackendAPIBaseURI = args[0];
		final String exportDirectoryName = args[1];

		try {

			executeExport(dswarmBackendAPIBaseURI, exportDirectoryName);
		} catch (final Exception e) {

			LOG.error("something went wrong at import execution.", e);
		}
	}
}
