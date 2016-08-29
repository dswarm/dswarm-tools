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
package org.dswarm.tools.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.tools.DswarmToolsError;
import org.dswarm.tools.DswarmToolsException;
import org.dswarm.tools.DswarmToolsStatics;

/**
 * @author tgaengler
 */
public final class DswarmToolUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DswarmToolUtils.class);

	public static void writeToFile(final String content, final String directory, final String fileName) throws IOException {

		checkDirExistenceOrCreateMissingParts(directory);

		final File file = org.apache.commons.io.FileUtils.getFile(directory, fileName);

		Files.write(content, file, Charsets.UTF_8);
	}

	public static String readFromFile(final String directory, final String fileName) throws IOException {

		final Path inputFilePath = Paths.get(directory + File.separator + fileName);
		final byte[] inputBytes = java.nio.file.Files.readAllBytes(inputFilePath);

		return new String(inputBytes, Charsets.UTF_8);
	}

	public static ObjectNode deserializeAsObjectNode(final String jsonString, final String errorMessage) {

		return deserialize(jsonString, errorMessage, ObjectNode.class);
	}

	public static ArrayNode deserializeAsArrayNode(final String jsonString, final String errorMessage) {

		return deserialize(jsonString, errorMessage, ArrayNode.class);
	}

	public static <TARGET_CLASS> TARGET_CLASS deserialize(final String jsonString, final String errorMessage, final Class<TARGET_CLASS> clasz) {

		try {

			return DswarmToolsStatics.MAPPER.readValue(jsonString, clasz);
		} catch (final IOException e) {

			LOG.error(errorMessage, e);

			throw DswarmToolsError.wrap(new DswarmToolsException(errorMessage, e));
		}
	}

	public static String serialize(final Object json, final String errorMessage) {

		try {

			return DswarmToolsStatics.MAPPER.writeValueAsString(json);
		} catch (final JsonProcessingException e) {

			LOG.error(errorMessage, e);

			throw DswarmToolsError.wrap(new DswarmToolsException(errorMessage, e));
		}
	}

	public static String getRecordClassURI(final JsonNode dataModelJSON) {

		final JsonNode schema = dataModelJSON.get(DswarmToolsStatics.SCHEMA_IDENTIFIER);

		final JsonNode schemaRecordClass = schema.get(DswarmToolsStatics.RECORD_CLASS_IDENTIFIER);

		return schemaRecordClass.get(DswarmToolsStatics.URI_IDENTIFIER).asText();
	}

	private static void checkDirExistenceOrCreateMissingParts(final String dirPath) {

		final File dirFile = new File(dirPath);

		if (!dirFile.exists()) {

			dirFile.mkdirs();
		}
	}
}
