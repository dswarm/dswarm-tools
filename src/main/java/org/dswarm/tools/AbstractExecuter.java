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
package org.dswarm.tools;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import javaslang.Tuple;
import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract executer - includes methods for parsing the arguments and printing the help.
 *
 * @author tgaengler
 */
public abstract class AbstractExecuter {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractExecuter.class);

	protected static String HELP;

	protected static void printHelp() {

		System.out.println(HELP);
	}

	protected static Map<String, String> parseArgs(final String[] args) {

		return Arrays.asList(args)
				.stream()
				.map(arg -> {
					try {

						return parseArg(arg);
					} catch (final DswarmToolsException e) {

						throw DswarmToolsError.wrap(e);
					}
				})
				.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
	}

	protected static Tuple2<String, String> parseArg(final String arg) throws DswarmToolsException {

		if (!arg.contains(DswarmToolsStatics.EQUALS)) {

			final String message = String.format("argument '%s' is in a wrong format; argument format should be -[KEY]=[VALUE]", arg);

			LOG.error(message);

			throw new DswarmToolsException(message);
		}

		final String[] split = arg.split(DswarmToolsStatics.EQUALS);

		return Tuple.of(split[0], split[1]);
	}
}
