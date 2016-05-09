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
package org.dswarm.tools.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * @author tgaengler
 */
public final class RxUtils {
	
	private static final Map<String, Scheduler> schedulers = new HashMap<>();
	
	
	public static Scheduler getObjectWriterScheduler(final String name) {

		final String dswarmObjectWriterThreadNamingPattern = String.format("dswarm-%s-writer-", name);
		
		return getScheduler(dswarmObjectWriterThreadNamingPattern + "%d");
	}
	
	public static Scheduler getScheduler(final String name) {
		
		return schedulers.computeIfAbsent(name, name1 -> {

			final ExecutorService executorService = Executors.newCachedThreadPool(
					new BasicThreadFactory.Builder().daemon(false).namingPattern(name1).build());
			
			return Schedulers.from(executorService);
		});
		
		
	}
}
