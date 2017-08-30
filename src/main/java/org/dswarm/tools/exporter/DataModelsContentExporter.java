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
package org.dswarm.tools.exporter;

import javaslang.Tuple2;
import rx.Observable;

import org.dswarm.tools.DswarmToolsStatics;
import org.dswarm.tools.apiclients.DswarmGraphExtensionAPIClient;

/**
 * @author tgaengler
 */
public final class DataModelsContentExporter extends AbstractExporter<DswarmGraphExtensionAPIClient> {

	public DataModelsContentExporter(final String dswarmGraphExtensionAPIBaseURI) {

		super(new DswarmGraphExtensionAPIClient(dswarmGraphExtensionAPIBaseURI), DswarmToolsStatics.DATA_MODEL);
	}

	public Observable<String> exportObjectsContent(final String exportDirectoryName, final Observable<Tuple2<String, String>> requestInputObservable) {

		return fetchObjectsContent(requestInputObservable)
				.observeOn(scheduler)
				// 3. store each object in a separate file
				.map(projectDescription -> writeExportObjectToFile(exportDirectoryName, projectDescription));
	}

	@Override
	protected Observable<Tuple2<String, String>> fetchObjects() {

		// TODO (if needed)
		return null;
	}

	private Observable<Tuple2<String, String>> fetchObjectsContent(final Observable<Tuple2<String, String>> dataModelRequestInputObservable) {

		return apiClient.fetchDataModelsContent(dataModelRequestInputObservable);
	}
}
