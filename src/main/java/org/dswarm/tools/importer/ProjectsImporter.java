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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import org.dswarm.common.types.Tuple;
import org.dswarm.tools.apiclients.DswarmProjectsAPIClient;
import org.dswarm.tools.DswarmToolsError;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.utils.DswarmToolUtils;

/**
 * @author tgaengler
 */
public final class ProjectsImporter {

	private static final Logger LOG = LoggerFactory.getLogger(ProjectsImporter.class);

	private static final String DSWARM_PROJECT_READER_THREAD_NAMING_PATTERN = "dswarm-project-reader-%d";
	private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool(
			new BasicThreadFactory.Builder().daemon(false).namingPattern(DSWARM_PROJECT_READER_THREAD_NAMING_PATTERN).build());
	private static final Scheduler SCHEDULER = Schedulers.from(EXECUTOR_SERVICE);

	private static final String UUID_IDENTIFIER = "uuid";

	private final DswarmProjectsAPIClient dswarmBackendAPIClient;

	public ProjectsImporter(final String dswarmBackendAPIBaseURI) {

		dswarmBackendAPIClient = new DswarmProjectsAPIClient(dswarmBackendAPIBaseURI);
	}

	public Observable<Tuple<String, String>> importProjects(final String importDirectoryName) throws DswarmToolsException {

		final File importDirectory = new File(importDirectoryName);

		if (!importDirectory.isDirectory()) {

			final String message = String.format("'%s' is no directory - please specify a folder as import directory", importDirectoryName);

			LOG.error(message);

			throw new DswarmToolsException(message);
		}

		final String[] importProjectFileNames = importDirectory.list();

		// read projects descriptions from files and prepare content
		final Observable<Tuple<String, String>> importProjectDescriptionTupleObservable = Observable.from(importProjectFileNames)
				.observeOn(SCHEDULER)
				.map(importProjectFileName -> readProjectFile(importDirectoryName, importProjectFileName))
				.map(ProjectsImporter::deserializeProjectFile)
				.map(ProjectsImporter::extractProjectIdentifier);

		return dswarmBackendAPIClient.importProjects(importProjectDescriptionTupleObservable);
	}

	private static Tuple<String, String> readProjectFile(final String importDirectoryName, final String importProjectFileName) {

		try {

			return Tuple.tuple(importDirectoryName + File.separator + importProjectFileName, DswarmToolUtils.readFromFile(importDirectoryName, importProjectFileName));
		} catch (final IOException e) {

			final String message = String.format("something went wrong, while trying to read file '%s' in folder '%s'", importProjectFileName, importDirectoryName);

			LOG.error(message, e);

			throw DswarmToolsError.wrap(new DswarmToolsException(message, e));
		}
	}

	private static Triple<String, ObjectNode, String> deserializeProjectFile(final Tuple<String, String> importProjectTuple) {

		final String absoluteImportProjectFileName = importProjectTuple.v1();
		final String importProjectJSONString = importProjectTuple.v2();

		final String errorMessage = String.format("something went wrong, while trying to deserialize file '%s'", absoluteImportProjectFileName);

		final ObjectNode importProjectJSON = DswarmToolUtils.deserializeAsObjectNode(importProjectJSONString, errorMessage);

		return Triple.of(absoluteImportProjectFileName, importProjectJSON, importProjectJSONString);
	}

	private static Tuple<String, String> extractProjectIdentifier(final Triple<String, ObjectNode, String> importProjectTriple) {

		final String absoluteImportProjectFileName = importProjectTriple.getLeft();
		final ObjectNode importProjectJSON = importProjectTriple.getMiddle();
		final String importProjectJSONString = importProjectTriple.getRight();

		final JsonNode importProjectUUIDJsonNode = importProjectJSON.get(UUID_IDENTIFIER);

		if (importProjectUUIDJsonNode == null) {

			final String message = String.format("something went wrong, while trying to extract project identifier from content of file '%s'", absoluteImportProjectFileName);

			LOG.error(message);

			throw DswarmToolsError.wrap(new DswarmToolsException(message));
		}

		final String importProjectUUID = importProjectUUIDJsonNode.asText();

		return Tuple.tuple(importProjectUUID, importProjectJSONString);
	}
}
