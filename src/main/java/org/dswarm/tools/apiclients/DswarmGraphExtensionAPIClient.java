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
package org.dswarm.tools.apiclients;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.glassfish.jersey.client.rx.RxWebTarget;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;

import org.dswarm.common.types.Tuple;
import org.dswarm.tools.DswarmToolsError;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.utils.DswarmToolUtils;

/**
 * @author tgaengler
 */
public final class DswarmGraphExtensionAPIClient extends AbstractAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmGraphExtensionAPIClient.class);

	private static final String GDM_ENDPOINT_IDENTIFIER = "gdm";
	private static final String GET_ENDPOINT_IDENTIFIER = "get";
	private static final String PUT_ENDPOINT_IDENTIFIER = "put";
	private static final String READ_DATA_MODEL_CONTENT_ENDPOINT = String.format("%s%s%s", GDM_ENDPOINT_IDENTIFIER, SLASH, GET_ENDPOINT_IDENTIFIER);
	private static final String WRITE_DATA_MODEL_CONTENT_ENDPOINT = String.format("%s%s%s", GDM_ENDPOINT_IDENTIFIER, SLASH, PUT_ENDPOINT_IDENTIFIER);
	private static final String EXPORT_TYPE = "exported";

	private static final String MULTIPART_MIXED = "multipart/mixed";
	public static final String CHUNKED_TRANSFER_ENCODING = "chunked";

	private static final String WRITE_GDM = "write to graph database";

	static {

		System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
	}

	public DswarmGraphExtensionAPIClient(final String dswarmGraphExtensionAPIBaseURI) {

		super(dswarmGraphExtensionAPIBaseURI, DswarmToolsStatics.DATA_MODEL);
	}

	public Observable<Tuple<String, String>> fetchDataModelsContent(final Observable<Tuple<String, String>> dataModelRequestInputObservable) {

		return dataModelRequestInputObservable
				.flatMap(dataModelRequestInput -> generateReadDataModelRequest(dataModelRequestInput)
						.flatMap(this::retrieveDataModelContent));
	}

	public Observable<Tuple<String, String>> importDataModelsContent(final Observable<Triple<String, String, String>> dataModelWriteRequestTripleObservable) {

		// TODO: this is just a workaround to process the request serially (i.e. one after another) until the processing at the endpoint is fixed (i.e. also prepared for parallel requests)
		return dataModelWriteRequestTripleObservable.flatMap(dataModelWriteRequestTriple -> {

			try {

				return importDataModelContent(dataModelWriteRequestTriple);
			} catch (final DswarmToolsException e) {

				throw DswarmToolsError.wrap(e);
			}
		}, 1);
	}

	private static Observable<Tuple<String, String>> generateReadDataModelRequest(final Tuple<String, String> dataModelRequestInputTuple) {

		final String dataModelId = dataModelRequestInputTuple.v1();
		final String recordClassURI = dataModelRequestInputTuple.v2();

		final String dataModelURI = String.format(DswarmToolsStatics.DATA_MODEL_URI_TEMPLATE, dataModelId);

		final ObjectNode requestJSON = DswarmToolsStatics.MAPPER.createObjectNode();

		requestJSON.put(DswarmToolsStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI)
				.put(DswarmToolsStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);

		return Observable.from(Futures.immediateFuture(DswarmToolUtils.serialize(requestJSON, "something went wrong while serializing the request JSON for the read-data-model-content-request")))
				.map(requestJSONString -> Tuple.tuple(dataModelId, requestJSONString));
	}

	private Observable<Tuple<String, String>> retrieveDataModelContent(final Tuple<String, String> readDataModelContentRequestTuple) {

		return executePOSTRequest(readDataModelContentRequestTuple, READ_DATA_MODEL_CONTENT_ENDPOINT, EXPORT_TYPE, exportScheduler);
	}

	private Observable<Tuple<String, String>> importDataModelContent(final Triple<String, String, String> writeDataModelContentRequestTriple) throws DswarmToolsException {

		final String dataModelId = writeDataModelContentRequestTriple.getLeft();
		final String writeDataModelContentRequestJSONString = writeDataModelContentRequestTriple.getMiddle();
		final String dataModelContentJSONString = writeDataModelContentRequestTriple.getRight();

		LOG.debug("metadata for write data model content request = '{}'", writeDataModelContentRequestJSONString);

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(WRITE_DATA_MODEL_CONTENT_ENDPOINT);

		final RxObservableInvoker rx = rxWebTarget.request(MULTIPART_MIXED).header(HttpHeaders.TRANSFER_ENCODING, CHUNKED_TRANSFER_ENCODING).rx();

		final InputStream input = IOUtils.toInputStream(dataModelContentJSONString, StandardCharsets.UTF_8);

		final MultiPart multiPart = new MultiPart();
		final BufferedInputStream entity1 = new BufferedInputStream(input, CHUNK_SIZE);

		multiPart
				.bodyPart(writeDataModelContentRequestJSONString, MediaType.APPLICATION_JSON_TYPE)
				.bodyPart(entity1, MediaType.APPLICATION_OCTET_STREAM_TYPE);

		// POST the request
		final Entity<MultiPart> entity = Entity.entity(multiPart, MULTIPART_MIXED);

		final Observable<Response> post = rx.post(entity).observeOn(importScheduler);

		final PublishSubject<Response> asyncPost = PublishSubject.create();
		asyncPost.subscribe(response -> {

			try {

				closeResource(multiPart, WRITE_GDM);
				closeResource(input, WRITE_GDM);

				//TODO maybe check status code here, i.e., should be 200

				LOG.debug("wrote GDM data for data model '{}' into data hub", dataModelId);
			} catch (final DswarmToolsException e) {

				throw DswarmToolsError.wrap(e);
			}
		}, throwable -> {

			throw DswarmToolsError.wrap(new DswarmToolsException(
					String.format("Couldn't store GDM data into database. Received status code '%s' from database endpoint.",
							throwable.getMessage())));
		}, () -> LOG.debug("completely wrote GDM data for data model '{}' into data hub", dataModelId));

		post.subscribe(asyncPost);

		return asyncPost.map(response -> {

			int status = response.getStatus();

			return Tuple.tuple(dataModelId, String.valueOf(status));
		});
	}

	private Observable<Tuple<String, String>> executePOSTRequest(final Tuple<String, String> requestTuple,
	                                                             final String requestURI,
	                                                             final String type,
	                                                             final Scheduler scheduler) {

		final String dataModelId = requestTuple.v1();
		final String requestJSONString = requestTuple.v2();

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(requestURI);

		final RxObservableInvoker rx = rxWebTarget.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.post(Entity.entity(requestJSONString, MediaType.APPLICATION_JSON), String.class)
				.observeOn(scheduler)
				.map(dataModelGDMJSONString -> {

					LOG.debug("{} content of data model '{}'", type, dataModelId);

					return getObjectsJSON(dataModelId, dataModelGDMJSONString);
				})
				.map(dataModelContentJSON -> serializeObjectJSON(dataModelId, dataModelContentJSON));
	}

	private static void closeResource(final Closeable closeable, final String type) throws DswarmToolsException {

		if (closeable != null) {

			try {

				closeable.close();
			} catch (final IOException e) {

				throw new DswarmToolsException(String.format("couldn't finish %s processing", type), e);
			}
		}
	}
}
