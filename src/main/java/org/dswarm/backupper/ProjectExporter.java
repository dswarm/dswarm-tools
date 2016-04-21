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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Created by tgaengler on 21.04.16.
 */
public final class ProjectExporter {

	private static final Logger LOG = LoggerFactory.getLogger(ProjectExporter.class);

	private static final String DSWARM_PROJECT_WRITER_THREAD_NAMING_PATTERN = "dswarm-project-writer-%d";
	private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool(
			new BasicThreadFactory.Builder().daemon(false).namingPattern(DSWARM_PROJECT_WRITER_THREAD_NAMING_PATTERN).build());
	private static final Scheduler SCHEDULER = Schedulers.from(EXECUTOR_SERVICE);

	private final DswarmBackendAPIClient dswarmBackendAPIClient;

	public ProjectExporter(final String dswarmBackendAPIBaseURI) {

		dswarmBackendAPIClient = new DswarmBackendAPIClient(dswarmBackendAPIBaseURI);
	}

	public Observable<String> exportProjects(final String exportDirectoryName) {

		return dswarmBackendAPIClient.fetchProjects()
				.observeOn(SCHEDULER)
				// 3. store each project in a separate file
				.map(projectDescription -> {

					final String projectIdentifier = projectDescription.v1();
					final String projectDescriptionJSONString = projectDescription.v2();

					final String fileName = buildFileName(projectIdentifier);

					LOG.debug("trying to export (write) full project description for project '{}' to file '{}/{}'", projectIdentifier, exportDirectoryName, fileName);

					try {

						FileUtils.writeToFile(projectDescriptionJSONString, exportDirectoryName, fileName);

						LOG.debug("exported (wrote) full project description for project '{}' to file '{}/{}'", projectIdentifier, exportDirectoryName, fileName);

						return projectDescriptionJSONString;
					} catch (final IOException e) {

						final String message = String.format("something went wrong, while trying to write project '%s' as file '%s' in folder '%s'", projectIdentifier, fileName, exportDirectoryName);

						LOG.error(message, e);

						throw DswarmBackupperError.wrap(new DswarmBackupperException(message, e));
					}
				});
	}

	private static String buildFileName(String projectIdentifier) {

		return String.format("project.%s.json", projectIdentifier);
	}
}
