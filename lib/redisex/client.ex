defmodule RedisEx.Client do
  #TODO: Unit test
  alias RedisEx.Connection

  defrecord ConnectionHandle, handle: nil

  def connect( hostname, port ) when is_binary( hostname )
                                 and is_binary( port ) do
    connect( hostname, binary_to_integer( port ) )
  end

  def connect( hostname, port ) when is_binary( hostname )
                                 and is_integer( port ) do
    ConnectionHandle.new( handle: Connection.start_link( [ hostname, port ] ) )
  end

  def disconnect( client ) do
    Connection.disconnect( client.socket )
  end

  defp process_command( client, command_list ) do
    Connection.send_command( client.socket, command_list )
    Connection.get_response( client.socket ) 
  end

  # Key Commands
  def del( client, key ), do: true
  def del( client, key_list ), do: true
  def dump( client, key ), do: true
  def exists( client, key ), do: true
  def expire( client, key, seconds ), do: true
  def expireat( client, key, timestamp ), do: true
  def keys( client, pattern ), do: true
  def migrate( client, host, port, key, db, timeout, opts \\ [] ), do: true
  def move( client, key, db ), do: true
  def object( client, subcommand, arguments ) when subcommand in [:REFCOUNT, :ENCODING, :IDLETIME], do: true
  def persist( client, key ), do: true
  def pexpire( client, key, milliseconds ), do: true
  def pexpireat( client, key, millisecond_timestamp ), do: true
  def pttl( client, key ), do: true
  def randomkey( client ), do: true
  def rename( client, key, newkey ), do: true
  def renamenx( client, key, newkey ), do: true
  def restore( client, key, ttl, serialized_value ), do: true
  def scan( client, cursor, opts \\ [] ), do: true
  def sort( client, key, opts \\ [] ), do: true
  def ttl( client, key ), do: true
  def type( client, key ), do: true

  # String Commands
  def append( client, key, value ), do: true
  def bitcount( client, key ), do: true
  def bitcount( client, key, start ), do: true
  def bitcount( client, key, range_start, range_end ), do: true
  def bitop( client, op, destkey, key ) when op in [:AND, :OR, :XOR, :NOT] and is_binary( key ), do: true
  def bitop( client, op, destkey, key_list ) when op in [:AND, :OR, :XOR, :NOT] and is_list( key_list) , do: true
  def bitpos( client, key, bit ), do: true
  def bitpos( client, key, bit, start ), do: true
  def bitpos( client, key, bit, start, range_end ), do: true
  def decr( client, key ), do: true
  def decrby( client, key, amount ), do: true
  def get( client, key ), do: true
  def getbit( client, key, offset ), do: true
  def getrange( client, key, start, range_end ), do: true
  def getset( client, key, value ), do: true
  def incr( client, key ), do: true
  def incrby( client, key, amount ), do: true
  def incrbyfloat( client, key, increment ), do: true
  def mget( client, key_list ), do: true
  def mset( client, key_value_list ), do: true
  def msetnx( client, key_value_list ), do: true
  def psetex( client, key, milliseconds, value ), do: true
  def set( client, key, value ), do: true
  def setbit( client, key, offset, value ), do: true
  def setex( client, key, seconds, value ), do: true
  def setnx( client, key, value ), do: true
  def setrange( client, key, offset, value ), do: true
  def strlen( client, key ), do: true

  # Hash commands
  def hdel( client, key, field ), do: true
  def hdel( client, key, field_list ), do: true
  def hexists( client, key, field ), do: true
  def hget( client, key, field ), do: true
  def hgetall( client, key ), do: true
  def hincrby( client, key, field, increment ), do: true
  def hincrbyfloat( client, key, field, increment ), do: true
  def hkeys( client, key ), do: true
  def hlen( client, key ), do: true
  def hmget( client, key, field_list ), do: true
  def hmset( client, key, field_value_list ), do: true
  def hscan( client, key, cursor, opts \\ []), do: true
  def hset( client, key, field, value ), do: true
  def hsetnx( client, key, field, value ), do: true
  def hvals( client, key ), do: true

  # List commands
  def blpop( client, key, timeout ), do: true
  def blpop( client, key_list, timeout ), do: true
  def brpop( client, key, timeout ), do: true
  def brpop( client, key_list, timeout ), do: true
  def brpoplpush( client, source, destination, timeout ), do: true
  def lindex( client, key, index ), do: true
  def linsert( client, key, :before, pivot, value ), do: true
  def linsert( client, key, :after, pivot, value ), do: true
  def llen( client, key ), do: true
  def lpop( client, key ), do: true
  def lpush( client, key, value ), do: true
  def lpush( client, key, value_list ), do: true
  def lpushx( client, key, value ), do: true
  def lrange( client, key, start, stop ), do: true
  def lrem( client, key, count, value ), do: true
  def lset( client, key, index, value ), do: true
  def ltrim( client, key, start, stop ), do: true
  def rpop( client, key ), do: true
  def rpoplpush( client, source, destination ), do: true
  def rpush( client, key, value ), do: true
  def rpush( client, key, value_list ), do: true
  def rpushx( client, key, value ), do: true

  # Set commands
  def sadd( client, key, member ), do: true
  def scard( client, key ), do: true
  def sdiff( client, key_list ), do: true
  def sdiffstore( client, destination, key_list ), do: true
  def sinter( client, key_list ), do: true
  def sinterstore( client, destination, key_list ), do: true
  def sismember( client, key, member ), do: true
  def smembers( client, key ), do: true
  def smove( client, sourcekey, destinationkey, member ), do: true
  def spop( client, key ), do: true
  def srandmember( client, key ), do: true
  def srandmember( client, key, count ), do: true
  def srem( client, key, member ), do: true
  def srem( client, key, member_list ), do: true
  def sscan( client, key, cursor, opts \\ [] ), do: true
  def sunion( client, key_list ), do: true
  def sunionstore( client, destination, key_list ), do: true

  # Sorted Set commands
  def zadd( client, key, score, member ), do: true
  def zadd( client, key, score_member_list ), do: true
  def zcard( client, key ), do: true
  def zcount( client, key, min, max ), do: true
  def zincrby( client, key, increment, member ), do: true
  def zinterstore( client, destination, numkeys, key_list, opts \\ [] ), do: true
  def zrange( client, key, start, stop, opts \\ [] ), do: true
  def zrangebyscore( client, key, min, max, opts \\ [] ), do: true
  def zrange( client, key, member ), do: true
  def zrem( client, key, member ), do: true
  def zrem( client, key, member_list ), do: true
  def zremrangebyrank( client, key, start, stop), do: true
  def zremrangebyscore( client, key, min, max ), do: true
  def zrevrange( client, key, start, stop, opts \\ [] ), do: true
  def zrevrangebyscore( client, key, max, min, opts \\ [] ), do: true
  def zrevrank( client, key, member ), do: true
  def zscan( client, key, cursor, opts \\ [] ), do: true
  def zscore( client, key, member ), do: true
  def zunionstore( client, destination, numkeys, key_list, opts \\ [] ), do: true

  # Pub/Sub commands
  def psubscribe( client, pattern ), do: true
  def psubscribe( client, pattern_list ), do: true
  def pubsub( client, :channels ), do: true
  def pubsub( client, :channels, pattern ), do: true
  def pubsub( client, :numsub, channel ), do: true
  def pubsub( client, :numsub, channel_list ), do: true
  def pubsub( client, :numpat ), do: true
  def punsubscribe( client, pattern ), do: true
  def punsubscribe( client, pattern_list ), do: true
  def subscribe( client, channel ), do: true
  def subscribe( client, channel_list ), do: true
  def unsubscribe( client, channel ), do: true
  def unsubscribe( client, channel_list ), do: true

  # Transactions
  def discard( client ), do: true
  def exec( client ), do: true
  def multi( client ), do: true
  def unwatch( client ), do: true
  def watch( client, key ), do: true
  def watch( client, key_list ), do: true

  # Scripting
  def eval( client, script, key_list, arg_list ), do: true
  def evalsha( client, sha_digest, numkeys, key_list, arg_list ), do: true
  def script_exists( client, script ), do: true
  def script_exists( client, script_list ), do: true
  def script_flush( client ), do: true
  def script_kill( client ), do: true
  def script_load( client, script ), do: true
  
  # Connection
  def auth( client, password ), do: true
  def echo( client, message ), do: true
  def ping( client ), do: true
  def quit( client ), do: true
  def select( client, index ), do: true

  # Server
  def bgrewriteaof( client ), do: true
  def bgsave( client ), do: true
  def client_getname( client ), do: true
  def client_kill( client, ip, port ), do: true
  def client_list( client ), do: true
  def client_pause( client, timeout ), do: true
  def client_setname( client, connection_name ), do: true
  def config( client, :get, parameter ), do: true
  def config( client, :resetstat ), do: true
  def config( client, :rewrite ), do: true
  def config( client, :set, parameter, value ), do: true
  def dbsize( client ), do: true
  def flushall( client ), do: true
  def flushdb( client ), do: true
  def info( client, section \\ :default ) when section in [ :server, :clients, :memory, :persistence, :stats, :replication, :cpu, :commandstats, :cluster, :keyspace, :all, :default ], do: true
  def lastsave( client ), do: true
  def save( client ), do: true
  def shutdown( client ), do: true
  def shutdown( client, :save ), do: true
  def shutdown( client, :nosave ), do: true
  def slaveof( client, host, port ), do: true
  def slaveof( client, :noone ), do: true
  def slowlog( client, :get ), do: true
  def slowlog( client, :len ), do: true
  def slowlog( client, :reset ), do: true
  def sync( client ), do: true
  def time( client ), do: true
end
