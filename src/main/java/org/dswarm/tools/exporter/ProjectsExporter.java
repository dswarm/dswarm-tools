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
package org.dswarm.tools.exporter;

import javaslang.Tuple2;
import rx.Observable;

import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.apiclients.DswarmProjectsAPIClient;

/**
 * @author tgaengler
 */
public final class ProjectsExporter extends AbstractExporter<DswarmProjectsAPIClient> {

	public ProjectsExporter(final String dswarmBackendAPIBaseURI) {

		super(new DswarmProjectsAPIClient(dswarmBackendAPIBaseURI), DswarmToolsStatics.PROJECT);
	}

	@Override
	protected Observable<Tuple2<String, String>> fetchObjects() {

		return apiClient.fetchObjects();
	}
}