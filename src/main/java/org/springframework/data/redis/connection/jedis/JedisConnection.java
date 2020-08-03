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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Client;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisResultBuilder;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisStatusResult;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/xetorthio/jedis">Jedis</a> library.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Jungtaek Lim
 * @author Konstantin Shchepanovskyi
 * @author David Liu
 * @author Milan Agatonovic
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Guy Korland
 */
public class JedisConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(JedisConverters.exceptionConverter());

	private final Jedis jedis;
	private @Nullable Transaction transaction;
	private final @Nullable Pool<Jedis> pool;
	/**
	 * flag indicating whether the connection needs to be dropped or not
	 */
	private volatile @Nullable JedisSubscription subscription;
	private volatile @Nullable Pipeline pipeline;
	private final int dbIndex;
	private final String clientName;
	private boolean convertPipelineAndTxResults = true;
	private List<JedisResult> pipelinedResults = new ArrayList<>();
	private Queue<FutureResult<Response<?>>> txResults = new LinkedList<>();

	/**
	 * Constructs a new <code>JedisConnection</code> instance.
	 *
	 * @param jedis Jedis entity
	 */
	public JedisConnection(Jedis jedis) {
		this(jedis, null, 0);
	}

	/**
	 * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
	 *
	 * @param jedis
	 * @param pool can be null, if no pool is used
	 * @param dbIndex
	 */
	public JedisConnection(Jedis jedis, Pool<Jedis> pool, int dbIndex) {
		this(jedis, pool, dbIndex, null);
	}

	/**
	 * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
	 *
	 * @param jedis
	 * @param pool can be null, if no pool is used
	 * @param dbIndex
	 * @param clientName the client name, can be {@literal null}.
	 * @since 1.8
	 */
	protected JedisConnection(Jedis jedis, @Nullable Pool<Jedis> pool, int dbIndex, String clientName) {

		this.jedis = jedis;
		this.pool = pool;
		this.dbIndex = dbIndex;
		this.clientName = clientName;

		// select the db
		// if this fail, do manual clean-up before propagating the exception
		// as we're inside the constructor
		if (dbIndex != jedis.getDB()) {
			try {
				select(dbIndex);
			} catch (DataAccessException ex) {
				close();
				throw ex;
			}
		}
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		DataAccessException exception = EXCEPTION_TRANSLATION.translate(ex);
		return exception != null ? exception : new RedisSystemException(ex.getMessage(), ex);
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return new JedisKeyCommands(this);
	}

	@Override
	public RedisStreamCommands streamCommands() {
		throw new UnsupportedOperationException("Streams not supported using Jedis!");
	}

	@Override
	public RedisStringCommands stringCommands() {
		return new JedisStringCommands(this);
	}

	@Override
	public RedisListCommands listCommands() {
		return new JedisListCommands(this);
	}

	@Override
	public RedisSetCommands setCommands() {
		return new JedisSetCommands(this);
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return new JedisZSetCommands(this);
	}

	@Override
	public RedisHashCommands hashCommands() {
		return new JedisHashCommands(this);
	}

	@Override
	public RedisGeoCommands geoCommands() {
		return new JedisGeoCommands(this);
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		return new JedisScriptingCommands(this);
	}

	@Override
	public RedisServerCommands serverCommands() {
		return new JedisServerCommands(this);
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return new JedisHyperLogLogCommands(this);
	}

	@Override
	public Object execute(String command, byte[]... args) {
		return execute(command, args, Connection::getOne, JedisClientUtils::getResponse);
	}

	<T> T execute(String command, byte[][] args, Function<Client, T> resultMapper, Function<Object, Response<?>> pipelineResponseMapper) {

		Assert.hasText(command, "A valid command needs to be specified!");
		Assert.notNull(args, "Arguments must not be null!");

		try {

			Client client = JedisClientUtils.sendCommand(command, args, this.jedis);

			if (isQueueing() || isPipelined()) {

				Response<?> result = pipelineResponseMapper
						.apply(isPipelined() ? getRequiredPipeline() : getRequiredTransaction());
				if (isPipelined()) {
					pipeline(newJedisResult(result));
				} else {
					transaction(newJedisResult(result));
				}
				return null;
			}
			return resultMapper.apply(client);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void close() throws DataAccessException {

		super.close();

		// return the connection to the pool
		if (pool != null) {
			jedis.close();
			return;
		}
		// else close the connection normally (doing the try/catch dance)
		Exception exc = null;
		try {
			jedis.quit();
		} catch (Exception ex) {
			exc = ex;
		}
		try {
			jedis.disconnect();
		} catch (Exception ex) {
			exc = ex;
		}
		if (exc != null)
			throw convertJedisAccessException(exc);
	}

	@Override
	public Jedis getNativeConnection() {
		return jedis;
	}

	@Override
	public boolean isClosed() {
		try {
			return !jedis.isConnected();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean isQueueing() {
		return JedisClientUtils.isInMulti(jedis);
	}

	@Override
	public boolean isPipelined() {
		return (pipeline != null);
	}

	@Override
	public void openPipeline() {
		if (pipeline == null) {
			pipeline = jedis.pipelined();
		}
	}

	@Override
	public List<Object> closePipeline() {
		if (pipeline != null) {
			try {
				return convertPipelineResults();
			} finally {
				pipeline = null;
				pipelinedResults.clear();
			}
		}
		return Collections.emptyList();
	}

	private List<Object> convertPipelineResults() {
		List<Object> results = new ArrayList<>();
		getRequiredPipeline().sync();
		Exception cause = null;
		for (JedisResult result : pipelinedResults) {
			try {

				Object data = result.get();

				if (!result.isStatus()) {
					results.add(result.conversionRequired() ? result.convert(data) : data);
				}
			} catch (JedisDataException e) {
				DataAccessException dataAccessException = convertJedisAccessException(e);
				if (cause == null) {
					cause = dataAccessException;
				}
				results.add(dataAccessException);
			} catch (DataAccessException e) {
				if (cause == null) {
					cause = e;
				}
				results.add(e);
			}
		}
		if (cause != null) {
			throw new RedisPipelineException(cause, results);
		}
		return results;
	}

	void pipeline(JedisResult result) {
		if (isQueueing()) {
			transaction(result);
		} else {
			pipelinedResults.add(result);
		}
	}

	void transaction(FutureResult<Response<?>> result) {
		txResults.add(result);
	}

	@Override
	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(newJedisResult(getRequiredPipeline().echo(message)));
				return null;
			}
			if (isQueueing()) {
				transaction(newJedisResult(getRequiredTransaction().echo(message)));
				return null;
			}
			return jedis.echo(message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(newJedisResult(getRequiredPipeline().ping()));
				return null;
			}
			if (isQueueing()) {
				transaction(newJedisResult(getRequiredTransaction().ping()));
				return null;
			}
			return jedis.ping();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void discard() {
		try {
			if (isPipelined()) {
				pipeline(newStatusResult(getRequiredPipeline().discard()));
				return;
			}
			getRequiredTransaction().discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	@Override
	public List<Object> exec() {
		try {
			if (isPipelined()) {
				pipeline(newJedisResult(getRequiredPipeline().exec(),
						new TransactionResultConverter<>(new LinkedList<>(txResults), JedisConverters.exceptionConverter())));
				return null;
			}

			if (transaction == null) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction. Did you forget to call multi?");
			}

			List<Object> results = transaction.exec();

			return !CollectionUtils.isEmpty(results)
					? new TransactionResultConverter<>(txResults, JedisConverters.exceptionConverter()).convert(results)
					: results;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	@Nullable
	public Pipeline getPipeline() {
		return pipeline;
	}

	public Pipeline getRequiredPipeline() {

		Pipeline pipeline = getPipeline();

		if (pipeline == null) {
			throw new IllegalStateException("Connection has no active pipeline");
		}

		return pipeline;
	}

	@Nullable
	public Transaction getTransaction() {
		return transaction;
	}

	public Transaction getRequiredTransaction() {

		Transaction transaction = getTransaction();

		if (transaction == null) {
			throw new IllegalStateException("Connection has no active transaction");
		}

		return transaction;
	}

	public Jedis getJedis() {
		return jedis;
	}

	JedisResult newJedisResult(Response<?> response) {
		return JedisResultBuilder.forResponse(response).build();
	}

	<T, R> JedisResult newJedisResult(Response<T> response, Converter<T, R> converter) {

		return JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).build();
	}

	<T, R> JedisResult newJedisResult(Response<T> response, Converter<T, R> converter, Supplier<R> defaultValue) {

		return JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).mapNullTo(defaultValue).build();
	}

	JedisStatusResult newStatusResult(Response<?> response) {
		return JedisResultBuilder.forResponse(response).buildStatusResult();
	}

	@Override
	public void multi() {
		if (isQueueing()) {
			return;
		}
		try {
			if (isPipelined()) {
				getRequiredPipeline().multi();
				return;
			}
			this.transaction = jedis.multi();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(newStatusResult(getRequiredPipeline().select(dbIndex)));
				return;
			}
			if (isQueueing()) {
				transaction(newStatusResult(getRequiredTransaction().select(dbIndex)));
				return;
			}
			jedis.select(dbIndex);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void unwatch() {
		try {
			jedis.unwatch();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			for (byte[] key : keys) {
				if (isPipelined()) {
					pipeline(newStatusResult(getRequiredPipeline().watch(key)));
				} else {
					jedis.watch(key);
				}
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(newJedisResult(getRequiredPipeline().publish(channel, message)));
				return null;
			}
			if (isQueueing()) {
				transaction(newJedisResult(getRequiredTransaction().publish(channel, message)));
				return null;
			}
			return jedis.publish(channel, message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			jedis.psubscribe(jedisPubSub, patterns);

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			jedis.subscribe(jedisPubSub, channels);

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Jedis driver
	 *
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	@Override
	protected boolean isActive(RedisNode node) {

		Jedis temp = null;
		try {
			temp = getJedis(node);
			temp.connect();
			return temp.ping().equalsIgnoreCase("pong");
		} catch (Exception e) {
			return false;
		} finally {
			if (temp != null) {
				temp.disconnect();
				temp.close();
			}
		}
	}

	@Override
	protected JedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
		return new JedisSentinelConnection(getJedis(sentinel));
	}

	protected Jedis getJedis(RedisNode node) {

		Jedis jedis = new Jedis(node.getHost(), node.getPort());

		if (StringUtils.hasText(clientName)) {
			jedis.clientSetname(clientName);
		}

		return jedis;
	}

}
