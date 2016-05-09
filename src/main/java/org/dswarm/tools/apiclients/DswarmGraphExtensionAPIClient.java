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
package org.dswarm.tools.apiclients;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import org.glassfish.jersey.client.rx.RxWebTarget;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.common.types.Tuple;
import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.utils.DswarmToolUtils;

/**
 * @author tgaengler
 */
public final class DswarmGraphExtensionAPIClient extends AbstractAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmGraphExtensionAPIClient.class);

	private static final String DATA_MODEL_URI_TEMPLATE = "http://data.slub-dresden.de/datamodel/%s/data";
	private static final String DATA_MODEL_URI_IDENTIFIER = "data_model_uri";
	private static final String RECORD_CLASS_URI_IDENTIFIER = "record_class_uri";
	private static final String GDM_ENDPOINT_IDENTIFIER = "gdm";
	private static final String GET_ENDPOINT_IDENTIFIER = "get";
	private static final String READ_DATA_MODEL_CONTENT_ENDPOINT = String.format("%s%s%s", GDM_ENDPOINT_IDENTIFIER, SLASH, GET_ENDPOINT_IDENTIFIER);

	public DswarmGraphExtensionAPIClient(final String dswarmGraphExtensionAPIBaseURI) {

		super(dswarmGraphExtensionAPIBaseURI, DswarmToolsStatics.DATA_MODEL);
	}

	public Observable<Tuple<String, String>> fetchDataModelsContent(final Observable<Tuple<String, String>> dataModelRequestInputObservable) {

		return dataModelRequestInputObservable
				.flatMap(dataModelRequestInput -> generateReadDataModelRequest(dataModelRequestInput)
						.flatMap(this::retrieveDataModelContent));
	}

	private static Observable<Tuple<String, String>> generateReadDataModelRequest(final Tuple<String, String> dataModelRequestInputTuple) {

		final String dataModelId = dataModelRequestInputTuple.v1();
		final String recordClassURI = dataModelRequestInputTuple.v2();

		final String dataModelURI = String.format(DATA_MODEL_URI_TEMPLATE, dataModelId);

		final ObjectNode requestJSON = DswarmToolsStatics.MAPPER.createObjectNode();

		requestJSON.put(DATA_MODEL_URI_IDENTIFIER, dataModelURI)
				.put(RECORD_CLASS_URI_IDENTIFIER, recordClassURI);

		return Observable.from(Futures.immediateFuture(DswarmToolUtils.serialize(requestJSON, "something went wrong while serializing the request JSON for the read-data-model-content-request")))
				.map(requestJSONString -> Tuple.tuple(dataModelId, requestJSONString));
	}

	private Observable<Tuple<String, String>> retrieveDataModelContent(final Tuple<String, String> readDataModelContentRequestTuple) {

		final String dataModelId = readDataModelContentRequestTuple.v1();
		final String readDataModelContentRequestJSONString = readDataModelContentRequestTuple.v2();

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(READ_DATA_MODEL_CONTENT_ENDPOINT);

		final RxObservableInvoker rx = rxWebTarget.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.post(Entity.entity(readDataModelContentRequestJSONString, MediaType.APPLICATION_JSON), String.class)
				.observeOn(exportScheduler)
				.map(dataModelGDMJSONString -> {

					LOG.debug("exported content of data model '{}'", dataModelId);

					return getObjectsJSON(dataModelId, dataModelGDMJSONString);
				})
				.map(dataModelContentJSON -> serializeObjectJSON(dataModelId, dataModelContentJSON));
	}
}
