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
  def append( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( value ) do
    command_list = [ "APPEND", key, value ]
    Connection.process( connection_handle.handle, command_list )
  end
                                          
  def bitcount( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) do
    command_list = [ "BITCOUNT", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitcount( connection_handle, key, range_start ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( range_start ) do

    command_list = [ "BITCOUNT", key, range_start ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitcount( connection_handle, key, range_start, range_end ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "BITCOUNT", key, range_start, range_end ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitop( connection_handle, op, dest_key, key ) 
      when op in [:AND, :OR, :XOR, :NOT] 
       and is_binary( key ) 
       and is_binary( dest_key )
       and is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "BITOP", atom_to_binary( op ), dest_key, key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitop( connection_handle, op, dest_key, key_list ) 
      when op in [:AND, :OR, :XOR, :NOT] 
       and is_list( key_list)  
       and is_binary( dest_key )
       and is_list( key_list )
       and is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "BITOP", atom_to_binary( op ), dest_key | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitpos( connection_handle, key, bit ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ] do
    command_list = [ "BITPOS", key, bit ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitpos( connection_handle, key, bit, range_start )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ]
       and is_integer( range_start ) do
    command_list = [ "BITPOS", key, bit, range_start ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitpos( connection_handle, key, bit, range_start, range_end ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ] 
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "BITPOS", key, bit, range_start, range_end ]
    Connection.process( connection_handle.handle, command_list )
  end

  def decr( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "DECR", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def decrby( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( increment ) do
    command_list = [ "DECRBY", key, increment ]
    Connection.process( connection_handle.handle, command_list )
  end

  def get( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "GET", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def getbit( connection_handle, key, offset ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( offset )
       and offset >= 0 do
    command_list = [ "GETBIT", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def getrange( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "GETRANGE", key, range_start, range_end ]
    Connection.process( connection_handle.handle, command_list )
  end

  def getset( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( value ) do
    command_list = [ "GETSET", key, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def incr( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) do
    command_list = [ "INCR", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def incrby( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( increment ) do
    command_list = [ "INCRBY", key, increment ]
    Connection.process( connection_handle.handle, command_list )
  end

  def incrbyfloat( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_float( increment ) do
    command_list = [ "INCRBYFLOAT", key, increment ]
    Connection.process( connection_handle.handle, command_list )
  end

  def mget( connection_handle, key_list ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "MGET" | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def mset( connection_handle, key_value_list ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_list( key_value_list )
       and length( key_value_list ) > 0
       and rem( length( key_value_list ), 2 ) == 0 do
    command_list = [ "MSET" | key_value_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def msetnx( connection_handle, key_value_list ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_list( key_value_list )
       and length( key_value_list ) > 0
       and rem( length( key_value_list ), 2 ) == 0 do
    command_list = [ "MSETNX" | key_value_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def psetex( connection_handle, key, milliseconds, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( milliseconds )
       and is_binary( value ) do
    command_list = [ "PSETEX", key, milliseconds, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def set( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( value ) do
    command_list = [ "SET", key, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def setbit( connection_handle, key, offset, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( offset )
       and offset >= 0
       and value in [ 0, 1 ] do
    command_list = [ "SETBIT", key, offset, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def setex( connection_handle, key, seconds, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( seconds )
       and seconds >= 0
       and is_binary( value ) do
    command_list = [ "SETEX", key, seconds, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def setnx( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( value ) do
    command_list = [ "SETNX", key, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def setrange( connection_handle, key, offset, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( offset )
       and offset >= 0
       and is_binary( value ) do
    command_list = [ "SETRANGE", key, offset, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def strlen( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) do
    command_list = [ "STRLEN", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  # Hash commands
  def hdel( connection_handle, key, field_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( field_list )
       and length( field_list ) > 0 do
    command_list = [ "HDEL", key | field_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hexists( connection_handle, key, field )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) do
    command_list = [ "HEXISTS", key, field ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hget( connection_handle, key, field )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) do
    command_list = [ "HGET", key, field ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hgetall( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "HGETALL", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hincrby( connection_handle, key, field, increment )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field )
       and is_integer( increment ) do
    command_list = [ "HINCRBY", key, field, increment ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hincrbyfloat( connection_handle, key, field, increment )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) 
       and is_float( increment ) do
    command_list = [ "HINCRBYFLOAT", key, field, increment ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hkeys( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "HKEYS", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hlen( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "HLEN", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hmget( connection_handle, key, field_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( field_list )
       and length( field_list ) > 0 do
    command_list = [ "HMGET", key | field_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hmset( connection_handle, key, field_value_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( field_value_list )
       and length( field_value_list ) > 0
       and rem( length( field_value_list ), 2 ) == 0 do
    command_list = [ "HMSET", key | field_value_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  # TODO: Implement this
  # def hscan( connection_handle, key, cursor, opts \\ [])
  #     when is_record( connection_handle, ConnectionHandle )
  #      and is_binary( key ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end

  def hset( connection_handle, key, field, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field )
       and is_binary( value ) do
    command_list = [ "HSET", key, field, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hsetnx( connection_handle, key, field, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) 
       and is_binary( value ) do
    command_list = [ "HSETNX", key, field, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def hvals( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "HVALS", key ]
    Connection.process( connection_handle.handle, command_list )
  end


  # List commands
  def blpop( _client, _key, _timeout ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def blpop( _client, _key_list, _timeout ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def brpop( _client, _key, _timeout ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def brpop( _client, _key_list, _timeout ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def brpoplpush( _client, _source, _destination, _timeout ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lindex( _client, _key, _index ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def linsert( _client, _key, :before, _pivot, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def linsert( _client, _key, :after, _pivot, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def llen( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lpop( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lpush( _client, _key, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lpush( _client, _key, _value_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lpushx( _client, _key, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lrange( _client, _key, _start, _stop ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lrem( _client, _key, _count, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lset( _client, _key, _index, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def ltrim( _client, _key, _start, _stop ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def rpop( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def rpoplpush( _client, _source, _destination ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def rpush( _client, _key, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def rpush( _client, _key, _value_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def rpushx( _client, _key, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end

  # Set commands
  def sadd( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def scard( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sdiff( _client, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sdiffstore( _client, _destination, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sinter( _client, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sinterstore( _client, _destination, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sis_member( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def s_members( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def smove( _client, _source_key, _destinationkey, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def spop( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def srand_member( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def srand_member( _client, _key, _count ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def srem( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def srem( _client, _key, _member_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sscan( _client, _key, _cursor, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sunion( _client, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sunionstore( _client, _destination, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end

  # Sorted Set commands
  def zadd( _client, _key, _score, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zadd( _client, _key, _score_member_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zcard( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zcount( _client, _key, _min, _max ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zincrby( _client, _key, _increment, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zinterstore( _client, _destination, _num_keys, _key_list, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrange( _client, _key, _start, _stop, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrangebyscore( _client, _key, _min, _max, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrange( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrem( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrem( _client, _key, _member_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zremrangebyrank( _client, _key, _start, _stop) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zremrangebyscore( _client, _key, _min, _max ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrevrange( _client, _key, _start, _stop, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrevrangebyscore( _client, _key, _max, _min, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zrevrank( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zscan( _client, _key, _cursor, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zscore( _client, _key, _member ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def zunionstore( _client, _destination, _numkeys, _key_list, _opts \\ [] ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end

  # Pub/Sub commands
  def psubscribe( _client, _pattern ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def psubscribe( _client, _pattern_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def pubsub( _client, :channels ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def pubsub( _client, :channels, _pattern ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def pubsub( _client, :numsub, channel ) when is_binary( channel ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def pubsub( _client, :numsub, channel_list ) when is_list( channel_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def pubsub( _client, :numpat ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def punsubscribe( _client, _pattern ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def punsubscribe( _client, _pattern_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def subscribe( _client, _channel ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def subscribe( _client, _channel_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def unsubscribe( _client, _channel ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def unsubscribe( _client, _channel_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end

  # Transactions
  def discard( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def exec( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def multi( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def unwatch( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def watch( _client, _key ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def watch( _client, _key_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end

  # Scripting
  def eval( _client, _script, _key_list, _arg_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def evalsha( _client, _sha_digest, _numkeys, _key_list, _arg_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def script_exists( _client, _script ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def script_exists( _client, _script_list ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def script_flush( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def script_kill( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def script_load( _client, _script ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  
  # Connection
  def auth( _client, _password ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def echo( _client, _message ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def ping( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def quit( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def select( _client, _index ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end

  # Server
  def bgrewriteaof( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def bgsave( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def client_getname( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def client_kill( _client, _ip, _port ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def client_list( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def client_pause( _client, _timeout ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def client_setname( _client, _connection_name ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def config( _client, :get, _parameter ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def config( _client, :resetstat ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def config( _client, :rewrite ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def config( _client, :set, _parameter, _value ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def dbsize( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def flushall( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def flushdb( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def info( _client, section \\ :default ) when section in [ :server, :clients, :memory, :persistence, :stats, :replication, :cpu, :commandstats, :cluster, :keyspace, :all, :default ] do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def lastsave( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def save( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def shutdown( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def shutdown( _client, :save ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def shutdown( _client, :nosave ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def slaveof( _client, _host, _port ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def slaveof( _client, :noone ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def slowlog( _client, :get ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def slowlog( _client, :len ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def slowlog( _client, :reset ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def sync( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
  def time( _client ) do
    command_list = []
    Connection.process( connection_handle.handle, command_list )
  end
end
