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
import java.util.concurrent.Executors;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.Futures;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.Tuple3;
import org.glassfish.jersey.client.rx.RxWebTarget;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

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

	private static final Scheduler IMPORT_SCHEDULER = Schedulers.from(Executors.newSingleThreadExecutor());

	static {

		System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
	}

	public DswarmGraphExtensionAPIClient(final String dswarmGraphExtensionAPIBaseURI) {

		super(dswarmGraphExtensionAPIBaseURI, DswarmToolsStatics.DATA_MODEL);
	}

	public Observable<Tuple2<String, String>> fetchDataModelsContent(final Observable<Tuple2<String, String>> dataModelRequestInputObservable) {

		return dataModelRequestInputObservable
				.flatMap(dataModelRequestInput -> generateReadDataModelRequest(dataModelRequestInput)
						.flatMap(this::retrieveDataModelContent));
	}

	public Observable<Tuple2<String, String>> importDataModelsContent(final Observable<Tuple3<String, String, InputStream>> dataModelWriteRequestTripleObservable) {

		return dataModelWriteRequestTripleObservable.flatMap(this::importDataModelContent, 1);
	}

	private static Observable<Tuple2<String, String>> generateReadDataModelRequest(final Tuple2<String, String> dataModelRequestInputTuple) {

		final String dataModelId = dataModelRequestInputTuple._1;
		final String recordClassURI = dataModelRequestInputTuple._2;

		final String dataModelURI = String.format(DswarmToolsStatics.DATA_MODEL_URI_TEMPLATE, dataModelId);

		final ObjectNode requestJSON = DswarmToolsStatics.MAPPER.createObjectNode();

		requestJSON.put(DswarmToolsStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI)
				.put(DswarmToolsStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);

		return Observable.from(Futures.immediateFuture(DswarmToolUtils.serialize(requestJSON, "something went wrong while serializing the request JSON for the read-data-model-content-request")))
				.map(requestJSONString -> Tuple.of(dataModelId, requestJSONString));
	}

	private Observable<Tuple2<String, String>> retrieveDataModelContent(final Tuple2<String, String> readDataModelContentRequestTuple) {

		return executePOSTRequest(readDataModelContentRequestTuple, READ_DATA_MODEL_CONTENT_ENDPOINT, EXPORT_TYPE, exportScheduler);
	}

	private Observable<Tuple2<String, String>> importDataModelContent(final Tuple3<String, String, InputStream> writeDataModelContentRequestTriple) {

		final String dataModelId = writeDataModelContentRequestTriple._1;
		final String writeDataModelContentRequestJSONString = writeDataModelContentRequestTriple._2;
		final InputStream dataModelContentJSONIS = writeDataModelContentRequestTriple._3;

		LOG.debug("metadata for write data model content request = '{}'", writeDataModelContentRequestJSONString);

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(WRITE_DATA_MODEL_CONTENT_ENDPOINT);

		final RxObservableInvoker rx = rxWebTarget.request(MULTIPART_MIXED).header(HttpHeaders.TRANSFER_ENCODING, CHUNKED_TRANSFER_ENCODING).rx();

		final MultiPart multiPart = new MultiPart();
		final BufferedInputStream entity1 = new BufferedInputStream(dataModelContentJSONIS, CHUNK_SIZE);

		multiPart
				.bodyPart(writeDataModelContentRequestJSONString, MediaType.APPLICATION_JSON_TYPE)
				.bodyPart(entity1, MediaType.APPLICATION_OCTET_STREAM_TYPE);

		// POST the request
		final Entity<MultiPart> entity = Entity.entity(multiPart, MULTIPART_MIXED);

		final Observable<Response> post = rx.post(entity).observeOn(IMPORT_SCHEDULER);

		return post.map(response -> {

			int status = response.getStatus();

			try {

				closeResource(multiPart, WRITE_GDM);
				closeResource(entity1, WRITE_GDM);
				closeResource(dataModelContentJSONIS, WRITE_GDM);

			} catch (final DswarmToolsException e) {

				throw DswarmToolsError.wrap(e);
			}

			if (status == 200) {

				LOG.debug("wrote GDM data for data model '{}' into data hub", dataModelId);
			} else {

				throw DswarmToolsError.wrap(new DswarmToolsException(
						String.format("Couldn't store GDM data into database. Received status code '%s' from database endpoint (response body = '%s').", status, response.readEntity(String.class))));
			}

			return Tuple.of(dataModelId, String.valueOf(status));
		})
				.doOnError(throwable -> {

					throw DswarmToolsError.wrap(new DswarmToolsException(String.format("Couldn't store GDM data into database (err0r = '%s')", throwable.getMessage())));
				})
				.doOnCompleted(() -> LOG.debug("completely wrote GDM data for data model '{}' into data hub", dataModelId));
	}

	private Observable<Tuple2<String, String>> executePOSTRequest(final Tuple2<String, String> requestTuple,
	                                                             final String requestURI,
	                                                             final String type,
	                                                             final Scheduler scheduler) {

		final String dataModelId = requestTuple._1;
		final String requestJSONString = requestTuple._2;

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(requestURI);

		final RxObservableInvoker rx = rxWebTarget.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.post(Entity.entity(requestJSONString, MediaType.APPLICATION_JSON))
				.observeOn(scheduler)
				.filter(response -> {

					final int responseStatus = response.getStatus();

					if(responseStatus != 200) {

						LOG.error("could not retrieve content of data model '{}' (got response status = '{}')", dataModelId, responseStatus);

						return false;
					}

					LOG.info("could retrieve content of data model '{}'", dataModelId);

					return true;
				})
				.map(response -> response.readEntity(String.class))
				.map(dataModelGDMJSONString -> getObjectsJSON(dataModelId, dataModelGDMJSONString))
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
