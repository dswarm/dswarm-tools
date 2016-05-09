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
package org.dswarm.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.rx.RxWebTarget;
import org.glassfish.jersey.client.rx.rxjava.RxObservable;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import org.dswarm.common.types.Tuple;
import org.dswarm.tools.utils.FileUtils;

/**
 * @author tgaengler
 */
public class DswarmBackendAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmBackendAPIClient.class);

	private static final String CHUNKED = "CHUNKED";

	private static final int CHUNK_SIZE = 1024;
	private static final int REQUEST_TIMEOUT = 20000000;

	private static final String DSWARM_PROJECT_EXPORTER_THREAD_NAMING_PATTERN = "dswarm-project-exporter-%d";
	private static final ExecutorService EXPORT_EXECUTOR_SERVICE = Executors.newCachedThreadPool(
			new BasicThreadFactory.Builder().daemon(false).namingPattern(DSWARM_PROJECT_EXPORTER_THREAD_NAMING_PATTERN).build());
	private static final Scheduler EXPORT_SCHEDULER = Schedulers.from(EXPORT_EXECUTOR_SERVICE);

	private static final String DSWARM_PROJECT_IMPORTER_THREAD_NAMING_PATTERN = "dswarm-project-importer-%d";
	private static final ExecutorService IMPORT_EXECUTOR_SERVICE = Executors.newCachedThreadPool(
			new BasicThreadFactory.Builder().daemon(false).namingPattern(DSWARM_PROJECT_IMPORTER_THREAD_NAMING_PATTERN).build());
	private static final Scheduler IMPORT_SCHEDULER = Schedulers.from(IMPORT_EXECUTOR_SERVICE);

	private static final ClientBuilder BUILDER = ClientBuilder.newBuilder().register(MultiPartFeature.class)
			.property(ClientProperties.CHUNKED_ENCODING_SIZE, CHUNK_SIZE)
			.property(ClientProperties.REQUEST_ENTITY_PROCESSING, CHUNKED)
			.property(ClientProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, CHUNK_SIZE)
			.property(ClientProperties.CONNECT_TIMEOUT, REQUEST_TIMEOUT)
			.property(ClientProperties.READ_TIMEOUT, REQUEST_TIMEOUT);

	private static final Client CLIENT = BUILDER.register(new LoggingFilter()).build();

	private static final String PROJECTS_IDENTIFIER = "/projects";
	private static final String FORMAT_IDENTIFIER = "format";
	private static final String SHORT_FORMAT_IDENTIFIER = "short";
	private static final String UUID_IDENTIFIER = "uuid";
	private static final String SLASH = "/";
	private static final String ROBUST_IDENTIFIER = "robust";
	private static final String ROBUST_IMPORT_PROJECT_ENDPOINT = PROJECTS_IDENTIFIER + SLASH + ROBUST_IDENTIFIER;

	private final String dswarmBackendAPIBaseURI;

	public DswarmBackendAPIClient(final String dswarmBackendAPIBaseURI) {

		this.dswarmBackendAPIBaseURI = dswarmBackendAPIBaseURI;
	}

	public Observable<Tuple<String, String>> fetchProjects() {

		// 1. retrieve all projects (in short form)
		return retrieveAllProjectIds()
				// 2. for each project: retrieve complete project
				.flatMap(this::retrieveProject);
	}

	public Observable<Tuple<String, String>> importProjects(final Observable<Tuple<String, String>> projectDescriptionTupleObservable) {

		// TODO: this is just a workaround to process the request serially (i.e. one after another) until the processing at the endpoint is fixed (i.e. also prepared for parallel requests)
		return projectDescriptionTupleObservable.flatMap(this::importProject, 1);
	}

	private Observable<String> retrieveAllProjectIds() {

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(PROJECTS_IDENTIFIER);

		final RxObservableInvoker rx = rxWebTarget.queryParam(FORMAT_IDENTIFIER, SHORT_FORMAT_IDENTIFIER)
				.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.get(String.class)
				.observeOn(EXPORT_SCHEDULER)
				.map(projectDescriptionsJSON -> {

					final String errorMessage = "something went wrong, while trying to retrieve short descriptions of all projects";

					return FileUtils.deserializeAsArrayNode(projectDescriptionsJSON, errorMessage);
				})
				.flatMap(projectDescriptionsJSON -> Observable.from(projectDescriptionsJSON)
						.map(projectDescriptionJSON -> projectDescriptionJSON.get(UUID_IDENTIFIER).asText()));
	}

	private Observable<Tuple<String, String>> retrieveProject(final String projectIdentifier) {

		LOG.debug("trying to retrieve full project description for project '{}'", projectIdentifier);

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(PROJECTS_IDENTIFIER + SLASH + projectIdentifier);

		final RxObservableInvoker rx = rxWebTarget.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.get(String.class)
				.observeOn(EXPORT_SCHEDULER)
				.map(projectDescriptionJSONString -> {

					LOG.debug("retrieved full project description for project '{}'", projectIdentifier);

					return getProjectJSON(projectIdentifier, projectDescriptionJSONString);
				})
				.map(projectDescriptionJSON -> serializeProjectJSON(projectIdentifier, projectDescriptionJSON));
	}

	private Observable<Tuple<String, String>> importProject(Tuple<String, String> projectDescriptionTuple) {

		final String projectIdentifier = projectDescriptionTuple.v1();
		final String projectDescriptionJSONString = projectDescriptionTuple.v2();

		LOG.debug("trying to import full project description of project '{}'", projectIdentifier);

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(ROBUST_IMPORT_PROJECT_ENDPOINT);

		final RxObservableInvoker rx = rxWebTarget.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.post(Entity.entity(projectDescriptionJSONString, MediaType.APPLICATION_JSON), String.class)
				.observeOn(IMPORT_SCHEDULER)
				.map(responseProjectDescriptionJSONString -> {

					LOG.debug("imported full project description for project '{}'", projectIdentifier);

					return getProjectJSON(projectIdentifier, responseProjectDescriptionJSONString);
				})
				.map(projectDescriptionJSON -> serializeProjectJSON(projectIdentifier, projectDescriptionJSON));
	}

	private static ObjectNode getProjectJSON(final String projectIdentifier, final String projectDescriptionJSONString) {

		final String errorMessage = String.format("something went wrong, while trying to transform full description of project %s", projectIdentifier);

		return FileUtils.deserializeAsObjectNode(projectDescriptionJSONString, errorMessage);
	}

	private static Tuple<String, String> serializeProjectJSON(final String projectIdentifier, final ObjectNode projectDescriptionJSON) {

		final String errorMessage = String.format("something went wrong, while trying to serialize full description of project %s", projectIdentifier);

		final String projectDescriptionJSONString = FileUtils.serialize(projectDescriptionJSON, errorMessage);

		return Tuple.tuple(projectIdentifier, projectDescriptionJSONString);
	}

	private static Client client() {

		return CLIENT;
	}

	private WebTarget target() {

		return client().target(dswarmBackendAPIBaseURI);
	}

	private WebTarget target(final String... path) {

		WebTarget target = target();

		for (final String p : path) {

			target = target.path(p);
		}

		return target;
	}

	private RxWebTarget<RxObservableInvoker> rxWebTarget() {

		final WebTarget target = target();

		return RxObservable.from(target);
	}

	private RxWebTarget<RxObservableInvoker> rxWebTarget(final String... path) {

		final WebTarget target = target(path);

		return RxObservable.from(target);
	}
}
