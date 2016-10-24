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
package org.dswarm.tools.importer;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

import org.dswarm.tools.DswarmToolsError;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.utils.DswarmToolUtils;
import org.dswarm.tools.utils.RxUtils;

/**
 * @author tgaengler
 */
public abstract class AbstractImporter<APICLIENT> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractImporter.class);

	protected final String objectName;
	protected final Scheduler scheduler;


	protected final APICLIENT apiClient;

	public AbstractImporter(final APICLIENT apiClientArg, final String objectNameArg) {

		apiClient = apiClientArg;
		objectName = objectNameArg;
		scheduler = RxUtils.getObjectReaderScheduler(objectName);
	}

	public Observable<Tuple2<String, String>> importObjects(final String importDirectoryName) throws DswarmToolsException {

		final Observable<Tuple2<String, String>> importObjectTupleObservable = prepareImport(importDirectoryName);

		return executeImport(importObjectTupleObservable);
	}

	protected Observable<Tuple2<String, String>> prepareImport(final String importDirectoryName) throws DswarmToolsException {

		final String[] importObjectFileNames = DswarmToolUtils.readFileNames(importDirectoryName);

		// read objects from files and prepare content
		return Observable.from(importObjectFileNames)
				.observeOn(scheduler)
				.map(importObjectFileName -> readObjectFile(importDirectoryName, importObjectFileName))
				.map(this::deserializeObjectFile)
				.map(this::extractObjectIdentifier);
	}

	protected abstract Observable<Tuple2<String, String>> executeImport(final Observable<Tuple2<String, String>> importObjectTupleObservable);

	protected abstract JsonNode deserializeObject(final String importObjectJSONString, final String errorMessage);

	private static Tuple2<String, String> readObjectFile(final String importDirectoryName, final String importObjectFileName) {

		try {

			return Tuple.of(importDirectoryName + File.separator + importObjectFileName, DswarmToolUtils.readFromFile(importDirectoryName, importObjectFileName));
		} catch (final IOException e) {

			final String message = String.format("something went wrong, while trying to read file '%s' in folder '%s'", importObjectFileName, importDirectoryName);

			LOG.error(message, e);

			throw DswarmToolsError.wrap(new DswarmToolsException(message, e));
		}
	}

	private Tuple3<String, JsonNode, String> deserializeObjectFile(final Tuple2<String, String> importObjectTuple) {

		final String absoluteImportObjectFileName = importObjectTuple._1;
		final String importObjectJSONString = importObjectTuple._2;

		final String errorMessage = String.format("something went wrong, while trying to deserialize file '%s'", absoluteImportObjectFileName);

		final JsonNode importObjectJSON = deserializeObject(importObjectJSONString, errorMessage);

		return Tuple.of(absoluteImportObjectFileName, importObjectJSON, importObjectJSONString);
	}

	protected Tuple2<String, String> extractObjectIdentifier(final Tuple3<String, JsonNode, String> importObjectTriple) {

		final String absoluteImportObjectFileName = importObjectTriple._1;
		final JsonNode importObjectJSON = importObjectTriple._2;
		final String importObjectJSONString = importObjectTriple._3;

		final JsonNode importObjectIdentifierJsonNode = importObjectJSON.get(DswarmToolsStatics.UUID_IDENTIFIER);

		if (importObjectIdentifierJsonNode == null) {

			final String message = String.format("something went wrong, while trying to extract %s identifier from content of file '%s'", objectName, absoluteImportObjectFileName);

			LOG.error(message);

			throw DswarmToolsError.wrap(new DswarmToolsException(message));
		}

		final String importObjectIdentifier = importObjectIdentifierJsonNode.asText();

		return Tuple.of(importObjectIdentifier, importObjectJSONString);
	}
}
