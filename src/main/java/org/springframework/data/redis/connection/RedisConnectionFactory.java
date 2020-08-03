/*
 * Copyright 2011-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.redis.connection;

import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * Thread-safe factory of Redis connections.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisConnectionFactory extends PersistenceExceptionTranslator {

	/**
	 * 创建一个RedisConnection
	 *
	 * @return connection for interacting with Redis.
	 */
	RedisConnection getConnection();

	/**
	 * 创建一个RedisClusterConnection
	 *
	 * @return
	 * @since 1.7
	 */
	RedisClusterConnection getClusterConnection();

	/**
	 * 指定是否将 pipelined result 转换为预期的数据类型。
	 * If false, results of {@link RedisConnection#closePipeline()} and {RedisConnection#exec()} will be of the type returned by the underlying
	 * driver This method is mostly for backwards compatibility with 1.0. It is generally always a good idea to allow
	 * results to be converted and deserialized. In fact, this is now the default behavior.
	 *
	 * @return Whether or not to convert pipeline and tx results
	 */
	boolean getConvertPipelineAndTxResults();

	/**
	 * Sentinel：哨兵，提供用于与Redis Sentinel交互的Connection
	 *
	 * @return connection for interacting with Redis Sentinel.
	 * @since 1.4
	 */
	RedisSentinelConnection getSentinelConnection();
}
