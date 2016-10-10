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

import java.io.IOException;

import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

import org.dswarm.tools.DswarmToolsError;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.utils.DswarmToolUtils;
import org.dswarm.tools.utils.RxUtils;

/**
 * @author tgaengler
 */
public abstract class AbstractExporter<APICLIENT> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractExporter.class);

	protected final String objectName;
	protected final Scheduler scheduler;


	protected final APICLIENT apiClient;

	public AbstractExporter(final APICLIENT apiClientArg, final String objectNameArg) {

		apiClient = apiClientArg;
		objectName = objectNameArg;
		scheduler = RxUtils.getObjectWriterScheduler(objectName);
	}

	public Observable<String> exportObjects(final String exportDirectoryName) {

		return fetchObjects()
				.observeOn(scheduler)
				// 3. store each object in a separate file
				.map(projectDescription -> writeExportObjectToFile(exportDirectoryName, projectDescription));
	}

	protected abstract Observable<Tuple2<String, String>> fetchObjects();

	protected String writeExportObjectToFile(final String exportDirectoryName, final Tuple2<String, String> objectDescription) {

		final String objectIdentifier = objectDescription._1;
		final String objectDescriptionJSONString = objectDescription._2;

		final String fileName = buildFileName(objectIdentifier);

		LOG.debug("trying to export (write) full {} description for {} '{}' to file '{}/{}'", objectName, objectName, objectIdentifier, exportDirectoryName, fileName);

		try {

			DswarmToolUtils.writeToFile(objectDescriptionJSONString, exportDirectoryName, fileName);

			LOG.debug("exported (wrote) full {} description for {} '{}' to file '{}/{}'", objectName, objectName, objectIdentifier, exportDirectoryName, fileName);

			return objectDescriptionJSONString;
		} catch (final IOException e) {

			final String message = String.format("something went wrong, while trying to write %s '%s' as file '%s' in folder '%s'", objectName, objectIdentifier, fileName, exportDirectoryName);

			LOG.error(message, e);

			throw DswarmToolsError.wrap(new DswarmToolsException(message, e));
		}
	}

	private String buildFileName(final String objectIdentifier) {

		return String.format("%s.%s.json", objectName, objectIdentifier);
	}
}
