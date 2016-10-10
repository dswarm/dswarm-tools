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

import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


import org.dswarm.tools.DswarmToolsStatics;

/**
 * @author tgaengler
 */
public final class DswarmProjectsAPIClient extends AbstractDswarmBackendAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmProjectsAPIClient.class);

	private static final String ROBUST_IDENTIFIER = "robust";
	private final String ROBUST_IMPORT_PROJECT_ENDPOINT = String.format("%s%s%s", OBJECTS_IDENTIFIER, SLASH, ROBUST_IDENTIFIER);

	public DswarmProjectsAPIClient(final String dswarmBackendAPIBaseURI) {

		super(dswarmBackendAPIBaseURI, DswarmToolsStatics.PROJECT);
	}

	@Override
	public Observable<Tuple2<String, String>> importObjects(final Observable<Tuple2<String, String>> objectDescriptionTupleObservable) {

		// TODO: this is just a workaround to process the request serially (i.e. one after another) until the processing at the endpoint is fixed (i.e. also prepared for parallel requests)
		return objectDescriptionTupleObservable.flatMap(this::importObject, 1);
	}

	@Override
	protected String getObjectsImportEndpoint() {

		return ROBUST_IMPORT_PROJECT_ENDPOINT;
	}
}
