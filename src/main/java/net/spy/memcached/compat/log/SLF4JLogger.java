package net.spy.memcached.compat.log;

/**
 * Logging implementation using
 * <a href="http://www.slf4j.org/">slf4j</a>.
 */
public class SLF4JLogger extends AbstractLogger {

	// Can't really import this without confusion as there's another thing
	// by this name in here.
	private final org.slf4j.Logger slf4jLogger;

	/**
	 * Get an instance of Log4JLogger.
	 */
	public SLF4JLogger(String name) {
		super(name);

		// Get the log4j logger instance.
		slf4jLogger=org.slf4j.LoggerFactory.getLogger(name);
	}

	/**
	 * True if the underlying logger would allow debug messages through.
	 */
	@Override
	public boolean isDebugEnabled() {
		return(slf4jLogger.isDebugEnabled());
	}

	/**
	 * True if the underlying logger would allow info messages through.
	 */
	@Override
	public boolean isInfoEnabled() {
		return(slf4jLogger.isInfoEnabled());
	}

	/**
	 * Wrapper around log4j.
	 *
	 * @param level net.spy.compat.log.AbstractLogger level.
	 * @param message object message
	 * @param e optional throwable
	 */
	@Override
	public void log(Level level, Object message, Throwable e) {
		switch(level == null ? Level.FATAL : level) {
			case DEBUG:
				if (slf4jLogger.isDebugEnabled()) {
					slf4jLogger.debug(message.toString(), e);
				}
				break;
			case INFO:
				if (slf4jLogger.isInfoEnabled()) {
					slf4jLogger.info(message.toString(), e);
				}
				break;
			case WARN:
				if (slf4jLogger.isWarnEnabled()) {
					slf4jLogger.warn(message.toString(), e);
				}
				break;
			case ERROR:
				if (slf4jLogger.isErrorEnabled()) {
					slf4jLogger.error(message.toString(), e);
				}
				break;
			case FATAL:
				if (slf4jLogger.isErrorEnabled()) {
					slf4jLogger.error(message.toString(), e);
				}
				break;
			default:
				// I don't know what this is, so consider it fatal
				if (slf4jLogger.isErrorEnabled()) {
					slf4jLogger.error(message.toString(), e);
				}
		}
	}

}
