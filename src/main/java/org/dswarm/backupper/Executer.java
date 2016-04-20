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
package org.dswarm.backupper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Executer {

	private static final Logger LOG = LoggerFactory.getLogger(Executer.class);

	private static void executeExport(final String dswarmBackendAPIBaseURI) {

		final DswarmBackendAPIClient dswarmBackendAPIClient = new DswarmBackendAPIClient();

		dswarmBackendAPIClient.fetchProjects(dswarmBackendAPIBaseURI, "");
	}

	public static void main(final String[] args) {

		// 0. read path from arguments
		if (args == null || args.length <= 0) {

			LOG.error("cannot execute export - no d:swarm backend API base URI given as commandline parameter");

			return;
		}

		final String dswarmBackendAPIBaseURI = args[0];

		try {

			executeExport(dswarmBackendAPIBaseURI);
		} catch (final Exception e) {

			LOG.error("something went wrong at import execution.", e);
		}
	}
}
