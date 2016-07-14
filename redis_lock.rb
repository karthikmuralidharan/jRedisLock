module RedisLock
  LockError           = Class.new(StandardError)

  DEFAULT_TTL         = 5 ## secs
  DEFAULT_RETRY_COUNT = 1
  DEFAULT_RETRY_DELAY = 100
  CLOCK_DRIFT_FACTOR  = 0.01

  UNLOCK_SCRIPT       = <<-SCRIPT
    if redis.call("get",KEYS[1]) == ARGV[1] then
      return redis.call("del",KEYS[1])
    else
      return 0
    end
  SCRIPT

  def perform_with_lock(lock_key, &block)
    lock_info = acquire_lock!(lock_key)
    yield lock_info if lock_info
  ensure
    unlock!(lock_info) if lock_info
  end

  def acquire_lock!(resource, ttl=DEFAULT_TTL)
    unique_id = get_unique_lock_id

    DEFAULT_RETRY_COUNT.times do |current_retry|
      start_time = timed
      lock_info = lock!(resource, unique_id, ttl)
      validity_time = (start_time + ttl) - (timed - start_time) - drift(ttl)
      if lock_info && validity_time > 0
        return {
          validity: validity_time,
          resource: resource,
          val: unique_id
        }
      end
      sleep get_sleep_time
    end

    ApplicationLogger.log(:warn, :redis_lock_error, {
      resource: resource,
      ttl: ttl
    })
    raise LockError, "failed to acquire lock for resource #{resource}"
  end

  def unlock!(lock_info)
    connection_pool.with do |connection|
      !!connection.client.call([:eval, UNLOCK_SCRIPT, 1, lock_info[:resource], lock_info[:val]])
    end
  end

  private

  def drift(ttl)
    # Add 2 milliseconds to the drift to account for Redis expires
    # precision, which is 1 millisecond, plus 1 millisecond min drift
    # for small TTLs.
    (ttl * CLOCK_DRIFT_FACTOR).to_i + 2
  end

  def get_unique_lock_id
    bytes = SecureRandom.random_bytes
    bytes.each_byte.inject('') { |val, b| val << b.to_s(32) }
  end

  def lock!(resource, val, ttl)
    connection_pool.with do |connection|
      !!connection.client.call([:set,resource,val,:nx,:ex,ttl])
    end
  end

  def get_sleep_time
    rand(DEFAULT_RETRY_DELAY).to_f / 1000
  end

  def timed
    (Time.now.utc.to_f * 1000).to_i
  end
end

