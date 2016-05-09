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
public final class DswarmProjectsAPIClient extends AbstractAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmProjectsAPIClient.class);

	private final String PROJECTS_IDENTIFIER = String.format("%s%ss", SLASH, objectName);
	private static final String FORMAT_IDENTIFIER = "format";
	private static final String SHORT_FORMAT_IDENTIFIER = "short";
	private static final String UUID_IDENTIFIER = "uuid";
	private static final String ROBUST_IDENTIFIER = "robust";
	private final String ROBUST_IMPORT_PROJECT_ENDPOINT = String.format("%s%s%s", PROJECTS_IDENTIFIER, SLASH, ROBUST_IDENTIFIER);

	public DswarmProjectsAPIClient(final String dswarmBackendAPIBaseURI) {

		super(dswarmBackendAPIBaseURI, DswarmToolsStatics.PROJECT);
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
				.observeOn(exportScheduler)
				.map(projectDescriptionsJSON -> {

					final String errorMessage = "something went wrong, while trying to retrieve short descriptions of all projects";

					return DswarmToolUtils.deserializeAsArrayNode(projectDescriptionsJSON, errorMessage);
				})
				.flatMap(projectDescriptionsJSON -> Observable.from(projectDescriptionsJSON)
						.map(projectDescriptionJSON -> projectDescriptionJSON.get(UUID_IDENTIFIER).asText()));
	}

	private Observable<Tuple<String, String>> retrieveProject(final String projectIdentifier) {

		LOG.debug("trying to retrieve full project description for project '{}'", projectIdentifier);

		final String requestURI = String.format("%s%s%s", PROJECTS_IDENTIFIER, SLASH, projectIdentifier);

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget(requestURI);

		final RxObservableInvoker rx = rxWebTarget.request()
				.accept(MediaType.APPLICATION_JSON_TYPE)
				.rx();

		return rx.get(String.class)
				.observeOn(exportScheduler)
				.map(projectDescriptionJSONString -> {

					LOG.debug("retrieved full project description for project '{}'", projectIdentifier);

					return getObjectJSON(projectIdentifier, projectDescriptionJSONString);
				})
				.map(projectDescriptionJSON -> serializeObjectJSON(projectIdentifier, projectDescriptionJSON));
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
				.observeOn(importScheduler)
				.map(responseProjectDescriptionJSONString -> {

					LOG.debug("imported full project description for project '{}'", projectIdentifier);

					return getObjectJSON(projectIdentifier, responseProjectDescriptionJSONString);
				})
				.map(projectDescriptionJSON -> serializeObjectJSON(projectIdentifier, projectDescriptionJSON));
	}
}
