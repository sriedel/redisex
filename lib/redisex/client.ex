defmodule RedisEx.Client do
  #TODO: Unit test
  alias RedisEx.Connection
  alias RedisEx.ConnectionSupervisor

  defrecord ConnectionHandle, handle: nil

  def connect( hostname, port ) when is_binary( hostname )
                                 and is_binary( port ) do
    connect( hostname, binary_to_integer( port ) )
  end

  def connect( hostname, port ) when is_binary( hostname )
                                 and is_integer( port ) do
     server_pid = ConnectionSupervisor.add_connection( [ hostname: hostname, port: port ] )
    ConnectionHandle.new( handle: server_pid )
  end

  def disconnect( client ) do
    ConnectionSupervisor.remove_connection( client.handle )
  end

  # Key Commands
  def del( _client, _key ), do: true
  def del( _client, _key_list ), do: true
  def dump( _client, _key ), do: true
  def exists( _client, _key ), do: true
  def expire( _client, _key, _seconds ), do: true
  def expireat( _client, _key, _timestamp ), do: true
  def keys( _client, _pattern ), do: true
  def migrate( _client, _host, _port, _key, _db, _timeout, _opts \\ [] ), do: true
  def move( _client, _key, _db ), do: true
  def object( _client, subcommand, _arguments ) when subcommand in [:REFCOUNT, :ENCODING, :IDLETIME], do: true
  def persist( _client, _key ), do: true
  def pexpire( _client, _key, _milliseconds ), do: true
  def pexpireat( _client, _key, _millisecond_timestamp ), do: true
  def pttl( _client, _key ), do: true
  def random_key( _client ), do: true
  def rename( _client, _key, _newkey ), do: true
  def renamenx( _client, _key, _newkey ), do: true
  def restore( _client, _key, _ttl, _serialized__value ), do: true
  def scan( _client, _cursor, _opts \\ [] ), do: true
  def sort( _client, _key, _opts \\ [] ), do: true
  def ttl( _client, _key ), do: true
  def type( _client, _key ), do: true

  # String Commands
  def append( _client, _key, _value ), do: true
  def bitcount( _client, _key ), do: true
  def bitcount( _client, _key, _start ), do: true
  def bitcount( _client, _key, _range__start, _range_end ), do: true
  def bitop( _client, op, _dest_key, key ) when op in [:AND, :OR, :XOR, :NOT] and is_binary( key ), do: true
  def bitop( _client, op, _dest_key, key_list ) when op in [:AND, :OR, :XOR, :NOT] and is_list( key_list) , do: true
  def bitpos( _client, _key, _bit ), do: true
  def bitpos( _client, _key, _bit, _start ), do: true
  def bitpos( _client, _key, _bit, _start, _range_end ), do: true
  def decr( _client, _key ), do: true
  def decrby( _client, _key, _amount ), do: true
  def get( _client, _key ), do: true
  def getbit( _client, _key, _offset ), do: true
  def getrange( _client, _key, _start, _range_end ), do: true
  def getset( _client, _key, _value ), do: true
  def incr( _client, _key ), do: true
  def incrby( _client, _key, _amount ), do: true
  def incrbyfloat( _client, _key, _increment ), do: true
  def mget( _client, _key_list ), do: true
  def mset( _client, _key__value_list ), do: true
  def msetnx( _client, _key__value_list ), do: true
  def psetex( _client, _key, _milliseconds, _value ), do: true
  def set( _client, _key, _value ), do: true
  def setbit( _client, _key, _offset, _value ), do: true
  def setex( _client, _key, _seconds, _value ), do: true
  def setnx( _client, _key, _value ), do: true
  def setrange( _client, _key, _offset, _value ), do: true
  def strlen( _client, _key ), do: true

  # Hash commands
  def hdel( _client, _key, _field ), do: true
  def hdel( _client, _key, _field_list ), do: true
  def hexists( _client, _key, _field ), do: true
  def hget( _client, _key, _field ), do: true
  def hgetall( _client, _key ), do: true
  def hincrby( _client, _key, _field, _increment ), do: true
  def hincrbyfloat( _client, _key, _field, _increment ), do: true
  def hkeys( _client, _key ), do: true
  def hlen( _client, _key ), do: true
  def hmget( _client, _key, _field_list ), do: true
  def hmset( _client, _key, _field__value_list ), do: true
  def hscan( _client, _key, _cursor, _opts \\ []), do: true
  def hset( _client, _key, _field, _value ), do: true
  def hsetnx( _client, _key, _field, _value ), do: true
  def hvals( _client, _key ), do: true

  # List commands
  def blpop( _client, _key, _timeout ), do: true
  def blpop( _client, _key_list, _timeout ), do: true
  def brpop( _client, _key, _timeout ), do: true
  def brpop( _client, _key_list, _timeout ), do: true
  def brpoplpush( _client, _source, _destination, _timeout ), do: true
  def lindex( _client, _key, _index ), do: true
  def linsert( _client, _key, :before, _pivot, _value ), do: true
  def linsert( _client, _key, :after, _pivot, _value ), do: true
  def llen( _client, _key ), do: true
  def lpop( _client, _key ), do: true
  def lpush( _client, _key, _value ), do: true
  def lpush( _client, _key, _value_list ), do: true
  def lpushx( _client, _key, _value ), do: true
  def lrange( _client, _key, _start, _stop ), do: true
  def lrem( _client, _key, _count, _value ), do: true
  def lset( _client, _key, _index, _value ), do: true
  def ltrim( _client, _key, _start, _stop ), do: true
  def rpop( _client, _key ), do: true
  def rpoplpush( _client, _source, _destination ), do: true
  def rpush( _client, _key, _value ), do: true
  def rpush( _client, _key, _value_list ), do: true
  def rpushx( _client, _key, _value ), do: true

  # Set commands
  def sadd( _client, _key, _member ), do: true
  def scard( _client, _key ), do: true
  def sdiff( _client, _key_list ), do: true
  def sdiffstore( _client, _destination, _key_list ), do: true
  def sinter( _client, _key_list ), do: true
  def sinterstore( _client, _destination, _key_list ), do: true
  def sis_member( _client, _key, _member ), do: true
  def s_members( _client, _key ), do: true
  def smove( _client, _source_key, _destinationkey, _member ), do: true
  def spop( _client, _key ), do: true
  def srand_member( _client, _key ), do: true
  def srand_member( _client, _key, _count ), do: true
  def srem( _client, _key, _member ), do: true
  def srem( _client, _key, _member_list ), do: true
  def sscan( _client, _key, _cursor, _opts \\ [] ), do: true
  def sunion( _client, _key_list ), do: true
  def sunionstore( _client, _destination, _key_list ), do: true

  # Sorted Set commands
  def zadd( _client, _key, _score, _member ), do: true
  def zadd( _client, _key, _score_member_list ), do: true
  def zcard( _client, _key ), do: true
  def zcount( _client, _key, _min, _max ), do: true
  def zincrby( _client, _key, _increment, _member ), do: true
  def zinterstore( _client, _destination, _num_keys, _key_list, _opts \\ [] ), do: true
  def zrange( _client, _key, _start, _stop, _opts \\ [] ), do: true
  def zrangebyscore( _client, _key, _min, _max, _opts \\ [] ), do: true
  def zrange( _client, _key, _member ), do: true
  def zrem( _client, _key, _member ), do: true
  def zrem( _client, _key, _member_list ), do: true
  def zremrangebyrank( _client, _key, _start, _stop), do: true
  def zremrangebyscore( _client, _key, _min, _max ), do: true
  def zrevrange( _client, _key, _start, _stop, _opts \\ [] ), do: true
  def zrevrangebyscore( _client, _key, _max, _min, _opts \\ [] ), do: true
  def zrevrank( _client, _key, _member ), do: true
  def zscan( _client, _key, _cursor, _opts \\ [] ), do: true
  def zscore( _client, _key, _member ), do: true
  def zunionstore( _client, _destination, _numkeys, _key_list, _opts \\ [] ), do: true

  # Pub/Sub commands
  def psubscribe( _client, _pattern ), do: true
  def psubscribe( _client, _pattern_list ), do: true
  def pubsub( _client, :channels ), do: true
  def pubsub( _client, :channels, _pattern ), do: true
  def pubsub( _client, :numsub, channel ) when is_binary( channel ), do: true
  def pubsub( _client, :numsub, channel_list ) when is_list( channel_list ), do: true
  def pubsub( _client, :numpat ), do: true
  def punsubscribe( _client, _pattern ), do: true
  def punsubscribe( _client, _pattern_list ), do: true
  def subscribe( _client, _channel ), do: true
  def subscribe( _client, _channel_list ), do: true
  def unsubscribe( _client, _channel ), do: true
  def unsubscribe( _client, _channel_list ), do: true

  # Transactions
  def discard( _client ), do: true
  def exec( _client ), do: true
  def multi( _client ), do: true
  def unwatch( _client ), do: true
  def watch( _client, _key ), do: true
  def watch( _client, _key_list ), do: true

  # Scripting
  def eval( _client, _script, _key_list, _arg_list ), do: true
  def evalsha( _client, _sha_digest, _numkeys, _key_list, _arg_list ), do: true
  def script_exists( _client, _script ), do: true
  def script_exists( _client, _script_list ), do: true
  def script_flush( _client ), do: true
  def script_kill( _client ), do: true
  def script_load( _client, _script ), do: true
  
  # Connection
  def auth( _client, _password ), do: true
  def echo( _client, _message ), do: true
  def ping( _client ), do: true
  def quit( _client ), do: true
  def select( _client, _index ), do: true

  # Server
  def bgrewriteaof( _client ), do: true
  def bgsave( _client ), do: true
  def client_getname( _client ), do: true
  def client_kill( _client, _ip, _port ), do: true
  def client_list( _client ), do: true
  def client_pause( _client, _timeout ), do: true
  def client_setname( _client, _connection_name ), do: true
  def config( _client, :get, _parameter ), do: true
  def config( _client, :resetstat ), do: true
  def config( _client, :rewrite ), do: true
  def config( _client, :set, _parameter, _value ), do: true
  def dbsize( _client ), do: true
  def flushall( _client ), do: true
  def flushdb( _client ), do: true
  def info( _client, section \\ :default ) when section in [ :server, :clients, :memory, :persistence, :stats, :replication, :cpu, :commandstats, :cluster, :keyspace, :all, :default ], do: true
  def lastsave( _client ), do: true
  def save( _client ), do: true
  def shutdown( _client ), do: true
  def shutdown( _client, :save ), do: true
  def shutdown( _client, :nosave ), do: true
  def slaveof( _client, _host, _port ), do: true
  def slaveof( _client, :noone ), do: true
  def slowlog( _client, :get ), do: true
  def slowlog( _client, :len ), do: true
  def slowlog( _client, :reset ), do: true
  def sync( _client ), do: true
  def time( _client ), do: true
end
