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

import rx.functions.Action0;
import rx.functions.Func1;

/**
 * @author tgaengler
 */
public class DswarmBackupperError extends RuntimeException {

	public DswarmBackupperError(final Throwable cause) {
		super(cause);
	}

	public static DswarmBackupperError wrap(final DswarmBackupperException exception) {
		return new DswarmBackupperError(exception);
	}

	public static Action0 wrapped(final PersistenceAction action) {
		return () -> {
			try {
				action.run();
			} catch (DswarmBackupperException e) {
				throw wrap(e);
			}
		};
	}

	public static <T, R> Func1<T, R> wrapped(final PersistenceFunction1<T, R> func) {
		return t -> {
			try {
				return func.apply(t);
			} catch (DswarmBackupperException e) {
				throw wrap(e);
			}
		};
	}

	@FunctionalInterface
	public interface PersistenceAction {

		void run() throws DswarmBackupperException;
	}

	@FunctionalInterface
	public interface PersistenceFunction1<T, R> {

		R apply(final T t) throws DswarmBackupperException;
	}
}
