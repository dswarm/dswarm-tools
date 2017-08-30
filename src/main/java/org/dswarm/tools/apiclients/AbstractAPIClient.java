/**
 * Copyright © 2016 – 2017 SLUB Dresden (<code@dswarm.org>)
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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.rx.RxWebTarget;
import org.glassfish.jersey.client.rx.rxjava.RxObservable;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import rx.Scheduler;

import org.dswarm.tools.utils.DswarmToolUtils;
import org.dswarm.tools.utils.RxUtils;

/**
 * @author tgaengler
 */
public abstract class AbstractAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractAPIClient.class);

	private static final String CHUNKED = "CHUNKED";

	protected static final int CHUNK_SIZE = 1024;
	private static final int REQUEST_TIMEOUT = 20000000;

	private static final ClientBuilder BUILDER = ClientBuilder.newBuilder().register(MultiPartFeature.class)
			.property(ClientProperties.CHUNKED_ENCODING_SIZE, CHUNK_SIZE)
			.property(ClientProperties.REQUEST_ENTITY_PROCESSING, CHUNKED)
			.property(ClientProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, CHUNK_SIZE)
			.property(ClientProperties.CONNECT_TIMEOUT, REQUEST_TIMEOUT)
			.property(ClientProperties.READ_TIMEOUT, REQUEST_TIMEOUT)
			.property(LoggingFeature.LOGGING_FEATURE_VERBOSITY_CLIENT, LoggingFeature.Verbosity.HEADERS_ONLY)
			.property(LoggingFeature.LOGGING_FEATURE_LOGGER_LEVEL_CLIENT, Level.INFO.toString());

	private static final Client CLIENT = BUILDER.register(LoggingFeature.class).build();

	protected static final String SLASH = "/";

	protected final Scheduler exportScheduler;
	protected final Scheduler importScheduler;
	protected final String objectName;
	protected final String apiBaseURI;

	public AbstractAPIClient(final String apiBaseURIArg, final String objectNameArg) {

		apiBaseURI = apiBaseURIArg;
		objectName = objectNameArg;

		exportScheduler = RxUtils.getObjectExporterScheduler(objectName);
		importScheduler = RxUtils.getObjectImporterScheduler(objectName);
	}

	protected ObjectNode getObjectJSON(final String objectIdentifier, final String objectJSONString) {

		final String errorMessage = String.format("something went wrong, while trying to transform %s %s", objectName, objectIdentifier);

		return DswarmToolUtils.deserializeAsObjectNode(objectJSONString, errorMessage);
	}

	protected ArrayNode getObjectsJSON(final String objectIdentifier, final String objectJSONString) {

		final String errorMessage = String.format("something went wrong, while trying to transform %s %s", objectName, objectIdentifier);

		return DswarmToolUtils.deserializeAsArrayNode(objectJSONString, errorMessage);
	}

	protected Tuple2<String, String> serializeObjectJSON(final String objectIdentifier, final Object objectJSON) {

		final String errorMessage = String.format("something went wrong, while trying to serialize %s %s", objectName, objectIdentifier);

		final String objectJSONString = DswarmToolUtils.serialize(objectJSON, errorMessage);

		return Tuple.of(objectIdentifier, objectJSONString);
	}

	private static Client client() {

		return CLIENT;
	}

	protected WebTarget target() {

		return client().target(apiBaseURI);
	}

	protected WebTarget target(final String... path) {

		WebTarget target = target();

		for (final String p : path) {

			target = target.path(p);
		}

		return target;
	}

	protected RxWebTarget<RxObservableInvoker> rxWebTarget() {

		final WebTarget target = target();

		return RxObservable.from(target);
	}

	protected RxWebTarget<RxObservableInvoker> rxWebTarget(final String... path) {

		final WebTarget target = target(path);

		return RxObservable.from(target);
	}
}
