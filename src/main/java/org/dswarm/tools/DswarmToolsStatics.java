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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author tgaengler
 */
public final class DswarmToolsStatics {

	public static final String DSWARM_BACKEND_API_BASE_URI_PARAMETER = "-dswarm-backend-api";
	public static final String DSWARM_GRAPH_EXTENSION_API_BASE_URI_PARAMETER = "-dswarm-graph-extension-api";
	public static final String EXPORT_DIRECTORY_NAME_PARAMETER = "-export-directory-name";
	public static final String IMPORT_DIRECTORY_NAME_PARAMETER = "-import-directory-name";
	public static final String HELP_PARAMETER = "--help";

	public static final String EQUALS = "=";

	public static final String PROJECT = "project";
	public static final String DATA_MODEL = "datamodel";

	public static final String INPUT_DATA_MODEL_IDENTIFIER = "input_data_model";
	public static final String UUID_IDENTIFIER = "uuid";
	public static final String SCHEMA_IDENTIFIER = "schema";
	public static final String RECORD_CLASS_IDENTIFIER = "record_class";
	public static final String URI_IDENTIFIER = "uri";

	public static final ObjectMapper MAPPER = new ObjectMapper()
			.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
			.setSerializationInclusion(JsonInclude.Include.NON_NULL)
			.configure(SerializationFeature.INDENT_OUTPUT, true);
}
