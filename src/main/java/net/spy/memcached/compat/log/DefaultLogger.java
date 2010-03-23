// Copyright (c) 2002  SPY internetworking <dustin@spy.net>

package net.spy.memcached.compat.log;


/**
 * Default logger implementation.
 *
 * Just extends SLF4JLogger, so SLF4JLogger is the real implementation.
 */
public class DefaultLogger extends SLF4JLogger {

	/**
	 * Get an instance of DefaultLogger.
	 */
	public DefaultLogger(String name) {
		super(name);
	}
}
