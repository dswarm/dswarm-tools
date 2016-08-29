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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.common.types.Tuple;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.apiclients.DswarmDataModelsAPIClient;
import org.dswarm.tools.apiclients.DswarmGraphExtensionAPIClient;
import org.dswarm.tools.utils.DswarmToolUtils;

/**
 * @author tgaengler
 */
public final class DataModelsContentImporter extends AbstractImporter<DswarmGraphExtensionAPIClient> {

	private static final Logger LOG = LoggerFactory.getLogger(DataModelsContentImporter.class);

	private final DswarmDataModelsAPIClient dswarmDataModelsAPIClient;

	public DataModelsContentImporter(final String dswarmGraphExtensionAPIBaseURI, final String dswarmBackendAPIBaseURI) {

		super(new DswarmGraphExtensionAPIClient(dswarmGraphExtensionAPIBaseURI), DswarmToolsStatics.DATA_MODEL);

		dswarmDataModelsAPIClient = new DswarmDataModelsAPIClient(dswarmBackendAPIBaseURI);
	}

	/**
	 * @param importDirectoryName
	 * @return v1 = data model identifier; v2 = data model metadata (JSON)
	 */
	public Observable<Tuple<String, String>> importObjectsContent(final String importDirectoryName) throws DswarmToolsException {

		final Observable<Triple<String, String, String>> dataModelWriteRequestTripleObservable = prepareImport2(importDirectoryName);

		return apiClient.importDataModelsContent(dataModelWriteRequestTripleObservable);
	}

	private Observable<Triple<String, String, String>> prepareImport2(final String importDirectoryName) throws DswarmToolsException {

		// read objects from files and prepare content/generate data model write request metadata
		final Observable<Tuple<String, String>> importObjectTupleObservable = prepareImport(importDirectoryName);

		return importObjectTupleObservable.flatMap(importObjectTuple -> {

			final String dataModelIdentifier = importObjectTuple.v1();
			final String dataModelContentJSONString = importObjectTuple.v2();

			final Observable<Tuple<String, String>> dataModelMetadataTupleObservable = dswarmDataModelsAPIClient.retrieveObject(dataModelIdentifier);

			return dataModelMetadataTupleObservable.map(dataModelMetadataTuple -> {

				final String dataModelMetadataJSONString = dataModelMetadataTuple.v2();

				final String errorMessage = String.format("something went wrong, while deserializing data model '%s'", dataModelIdentifier);

				final ObjectNode dataModelMetadataJSON = DswarmToolUtils.deserializeAsObjectNode(dataModelMetadataJSONString, errorMessage);

				// generate data model write request metadata (JSON) with help of data model metadata (JSON)
				final String dataModelWriteRequestMetadata = generateDataModelWriteRequestMetadata(dataModelIdentifier, dataModelMetadataJSON);

				return Triple.of(dataModelIdentifier, dataModelWriteRequestMetadata, dataModelContentJSONString);
			});
		});
	}

	@Override
	protected Observable<Tuple<String, String>> executeImport(final Observable<Tuple<String, String>> importObjectTupleObservable) {

		// TODO

		return null;
	}

	@Override
	protected JsonNode deserializeObject(final String importObjectJSONString, final String errorMessage) {

		return DswarmToolUtils.deserializeAsArrayNode(importObjectJSONString, errorMessage);
	}

	@Override
	protected Tuple<String, String> extractObjectIdentifier(final Triple<String, JsonNode, String> importObjectTriple) {

		final String absoluteImportObjectFileName = importObjectTriple.getLeft();
		final String importObjectJSONString = importObjectTriple.getRight();

		final String[] split = absoluteImportObjectFileName.split("\\.");

		final String importObjectIdentifier = split[split.length - 2];

		return Tuple.tuple(importObjectIdentifier, importObjectJSONString);
	}


	private String generateDataModelWriteRequestMetadata(final String dataModelIdentifier, final ObjectNode dataModelMetadataJSON) {

		final ObjectNode dataModelWriteRequestMetadataJSON = DswarmToolsStatics.MAPPER.createObjectNode();

		final String dataModelURI = String.format(DswarmToolsStatics.DATA_MODEL_URI_TEMPLATE, dataModelIdentifier);
		final String recorcClassURI = DswarmToolUtils.getRecordClassURI(dataModelMetadataJSON);

		dataModelWriteRequestMetadataJSON.put(DswarmToolsStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI)
				.put(DswarmToolsStatics.RECORD_CLASS_URI_IDENTIFIER, recorcClassURI)
				.put(DswarmToolsStatics.DEPRECATE_MISSING_RECORDS, Boolean.FALSE.toString())
				.put(DswarmToolsStatics.ENABLE_VERSIONING, Boolean.FALSE.toString());

		// TODO: add content schema, if necessary

		final String errorMessage = String.format("something went wrong, while trying to serialize request metadata for data model '%s'", dataModelIdentifier);

		return DswarmToolUtils.serialize(dataModelWriteRequestMetadataJSON, errorMessage);
	}
}
