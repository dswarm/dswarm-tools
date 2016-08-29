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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.tools.DswarmToolsStatics;

/**
 * @author tgaengler
 */
public final class DswarmDataModelsAPIClient extends AbstractDswarmBackendAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmDataModelsAPIClient.class);

	public DswarmDataModelsAPIClient(final String dswarmBackendAPIBaseURI) {

		super(dswarmBackendAPIBaseURI, DswarmToolsStatics.DATA_MODEL);
	}
}
