package runner.runner;

import java.util.Collection;

/**
 * 
 * Spawn a consumer
 * 
 * @author colte
 *
 */
public interface KafkaRunner<T> {
	Collection<T> pull();
}
