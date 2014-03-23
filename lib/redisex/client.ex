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
  def del( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "DEL", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def del( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "DEL" | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def dump( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "DUMP", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def exists( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "EXISTS", key ]
    result = Connection.process( connection_handle.handle, command_list )
    result == 1
  end

  def expire( connection_handle, key, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( seconds ) 
       and seconds >= 0 do
    command_list = [ "EXPIRE", key, integer_to_binary( seconds ) ]
    result = Connection.process( connection_handle.handle, command_list )
    result == 1
  end

  def expireat( connection_handle, key, timestamp )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( timestamp ) do
    command_list = [ "EXPIREAT", key, timestamp ]
    Connection.process( connection_handle.handle, command_list )
  end

  def keys( connection_handle, pattern )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( pattern ) do
    command_list = [ "KEYS", pattern ]
    Connection.process( connection_handle.handle, command_list )
  end

  def migrate( connection_handle, host, port, key, db, timeout, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( host )
       and is_binary( port )
       and is_binary( key )
       and is_integer( db )
       and db >= 0
       and is_integer( timeout )
       and timeout >= 0 do

    opt_list = []
    if :replace in opts do
      opt_list = [ "REPLACE" | opt_list ]
    end
    if :copy in opts do
      opt_list = [ "COPY" | opt_list ]
    end

    command_list = [ "MIGRATE", host, port, key, db, timeout | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def move( connection_handle, key, db )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( db ) 
       and db >= 0 do
    command_list = [ "MOVE", key, db ]
    Connection.process( connection_handle.handle, command_list )
  end

  def object( connection_handle, subcommand, arguments ) 
      when is_record( connection_handle, ConnectionHandle )
       and subcommand in [:REFCOUNT, :ENCODING, :IDLETIME]
       and is_list( arguments ) do
    command_list = [ "OBJECT", atom_to_binary( subcommand ) | arguments ]
    Connection.process( connection_handle.handle, command_list )
  end

  def persist( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "PERSIST", key ]
    result = Connection.process( connection_handle.handle, command_list )
    result == 1
  end

  def pexpire( connection_handle, key, milliseconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( milliseconds ) 
       and milliseconds > 0 do
    command_list = [ "PEXPIRE", key, integer_to_binary( milliseconds ) ]
    result = Connection.process( connection_handle.handle, command_list )
    result == 1
  end

  def pexpireat( connection_handle, key, millisecond_timestamp )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( millisecond_timestamp ) 
       and millisecond_timestamp > 0 do
    command_list = [ "PEXPIREAT", key, millisecond_timestamp ]
    Connection.process( connection_handle.handle, command_list )
  end

  def pttl( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "PTTL", key ]
    case Connection.process( connection_handle.handle, command_list ) do
      -2 -> nil
      -1 -> :no_ttl
      x when is_integer( x ) and x >= 0 -> x
    end
  end

  def randomkey( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "RANDOMKEY" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def rename( connection_handle, key, newkey )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( newkey ) do
    command_list = [ "RENAME", key, newkey ]
    Connection.process( connection_handle.handle, command_list )
  end

  def renamenx( connection_handle, key, newkey )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( newkey ) do
    command_list = [ "RENAMENX", key, newkey ]
    Connection.process( connection_handle.handle, command_list )
  end

  def restore( connection_handle, key, ttl, serialized_value )
      when is_record( connection_handle, ConnectionHandle )
       and is_integer( ttl )
       and ttl >= 0
       and is_binary( key ) 
       and is_binary( serialized_value ) do
    command_list = [ "RESTORE", key, integer_to_binary( ttl ), serialized_value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def scan( connection_handle, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    command_list = [ "SCAN", cursor | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sort( connection_handle, key, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do

    opt_list = []
    case opts[:store] do
      key when is_binary( key ) -> opt_list = [ "STORE", key | opt_list ]
      _ -> opt_list
    end

    case opts[:alpha] do
      true -> opt_list = [ "ALPHA" | opt_list ]
      _    -> opt_list 
    end

    case opts[:order] do
      :asc -> opt_list = [ "ASC" | opt_list ]
      :desc -> opt_list = [ "DESC" | opt_list ]
      _     -> opt_list
    end

    case opts[:limit] do
      a..b when is_integer( a ) and is_integer(b) -> opt_list = [ "LIMIT", integer_to_binary(a), integer_to_binary(b) | opt_list ]
      _ -> opt_list
    end

    case opts[:by] do
      keypattern when is_binary( keypattern ) -> opt_list = [ "BY", keypattern | opt_list ]
      _ -> opt_list
    end

    case opts[:get] do
      [ keypattern, "#" ] when is_binary( keypattern ) -> opt_list = [ "GET", keypattern, "GET", "#" | opt_list ]
      keypattern when is_binary( keypattern ) -> opt_list = [ "GET", keypattern | opt_list ]
      "#" -> opt_list = [ "GET", "#" | opt_list ]
      _ -> opt_list
    end

    command_list = [ "SORT", key | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def ttl( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "TTL", key ]
    case Connection.process( connection_handle.handle, command_list ) do
      -2 -> nil
      -1 -> :no_ttl
      x when is_integer( x ) and x >= 0 -> x
    end
  end

  def type( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "TYPE", key ]
    case Connection.process( connection_handle.handle, command_list ) do
      "string" -> :string
      "list" -> :list
      "hash" -> :hash
      "set" -> :set
      "zset" -> :zset
      _ -> :none
    end
  end

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

  def bitcount( connection_handle, key, range_start, range_end ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "BITCOUNT", key, integer_to_binary(range_start), integer_to_binary(range_end) ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitop( connection_handle, :NOT, dest_key, key ) 
      when is_binary( key ) 
       and is_binary( dest_key )
       and is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "BITOP", "NOT", dest_key, key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitop( connection_handle, op, dest_key, key_list ) 
      when op in [:AND, :OR, :XOR] 
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
    command_list = [ "BITPOS", key, integer_to_binary( bit ) ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitpos( connection_handle, key, bit, range_start )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ]
       and is_integer( range_start ) do
    command_list = [ "BITPOS", key, integer_to_binary( bit ), integer_to_binary( range_start ) ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bitpos( connection_handle, key, bit, range_start, range_end ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ] 
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "BITPOS", key, integer_to_binary( bit ), integer_to_binary( range_start ), integer_to_binary( range_end ) ]
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
    command_list = [ "DECRBY", key, integer_to_binary( increment ) ]
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
    command_list = [ "GETBIT", key, integer_to_binary( offset ) ]
    Connection.process( connection_handle.handle, command_list )
  end

  def getrange( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "GETRANGE", key, integer_to_binary(range_start), integer_to_binary(range_end) ]
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
    command_list = [ "INCRBY", key, integer_to_binary( increment ) ]
    Connection.process( connection_handle.handle, command_list )
  end

  def incrbyfloat( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( increment ) do
    command_list = [ "INCRBYFLOAT", key, increment ]
    result = Connection.process( connection_handle.handle, command_list )
    case result do
      { :redis_error, error_message } -> { :redis_error, error_message }
      float_as_binary -> if String.contains?( float_as_binary, "." ) do
                           binary_to_float( float_as_binary )
                         else
                           binary_to_integer( float_as_binary ) * 1.0
                         end
    end
  end
  def incrbyfloat( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_float( increment ) do
    incrbyfloat( connection_handle, key, float_to_binary(increment)  )
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
    case Connection.process( connection_handle.handle, command_list ) do
      1 -> true
      0 -> false
    end

  end

  def psetex( connection_handle, key, milliseconds, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( milliseconds )
       and is_binary( value ) do
    command_list = [ "PSETEX", key, integer_to_binary( milliseconds ), value ]
    Connection.process( connection_handle.handle, command_list )
  end

  #TODO: Support set options
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
    command_list = [ "SETBIT", key, integer_to_binary( offset ), integer_to_binary( value ) ]
    Connection.process( connection_handle.handle, command_list )
  end

  def setex( connection_handle, key, seconds, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( seconds )
       and seconds >= 0
       and is_binary( value ) do
    command_list = [ "SETEX", key, integer_to_binary( seconds ), value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def setnx( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( value ) do
    command_list = [ "SETNX", key, value ]
    case Connection.process( connection_handle.handle, command_list ) do
      1 -> true
      0 -> false
    end
  end

  def setrange( connection_handle, key, offset, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( offset )
       and offset >= 0
       and is_binary( value ) do
    command_list = [ "SETRANGE", key, integer_to_binary( offset ), value ]
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

  def hscan( connection_handle, key, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    command_list = [ "HSCAN", cursor | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

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
  def blpop( connection_handle, key_list, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0
       and is_integer( seconds )
       and seconds >= 0 do
    command_list = [ "BLPOP" | key_list ] ++ [ seconds ]
    Connection.process( connection_handle.handle, command_list )
  end

  def brpop( connection_handle, key_list, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list )
       and length( key_list ) > 0
       and is_integer( seconds )
       and seconds >= 0 do
    command_list = [ "BRPOP" | key_list ] ++ [ seconds ]
    Connection.process( connection_handle.handle, command_list )
  end

  def brpoplpush( connection_handle, source, destination, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( source )
       and is_binary( destination )
       and is_integer( seconds )
       and seconds >= 0 do
    command_list = [ "BRPOPLPUSH", source, destination, seconds ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lindex( connection_handle, key, index )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( index ) do
    command_list = [ "LINDEX", key, index ]
    Connection.process( connection_handle.handle, command_list )
  end

  def linsert( connection_handle, key, where, pivot, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and where in [ :before, :after ]
       and is_binary( pivot )
       and is_binary( value ) do
    command_list = [ "LINSERT", key, atom_to_binary( where ), pivot, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def llen( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "LLEN", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lpop( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "LPOP", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lpush( connection_handle, key, value_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( value_list )
       and length( value_list ) > 0 do
    command_list = [ "LPUSH", key | value_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lpushx( connection_handle, key, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( value ) do
    command_list = [ "LPUSHX", key, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lrange( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "LRANGE", key, range_start, range_end ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lrem( connection_handle, key, count, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( count ) 
       and is_binary( value ) do
    command_list = [ "LREM", key, count, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lset( connection_handle, key, index, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( index )
       and is_binary( value ) do
    command_list = [ "LSET", key, index, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def ltrim( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start )
       and is_integer( range_end ) do
    command_list = [ "LTRIM", key, range_start, range_end ]
    Connection.process( connection_handle.handle, command_list )
  end

  def rpop( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "RPOP", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def rpoplpush( connection_handle, source, destination )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( source )
       and is_binary( destination ) do
    command_list = [ "RPOPLPUSH", source, destination ]
    Connection.process( connection_handle.handle, command_list )
  end

  def rpush( connection_handle, key, value_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( value_list )
       and length( value_list ) > 0 do
    command_list = [ "RPUSH", key | value_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def rpushx( connection_handle, key, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( value ) do
    command_list = [ "RPUSHX", key, value ]
    Connection.process( connection_handle.handle, command_list )
  end


  # Set commands
  def sadd( connection_handle, key, member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( member_list )
       and length( member_list ) > 0 do
    command_list = [ "SADD", key | member_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def scard( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "SCARD", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sdiff( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "SDIFF" | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sdiffstore( connection_handle, destination, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "SDIFFSTORE", destination | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sinter( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "SINTER" | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sinterstore( connection_handle, destination, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "SINTERSTORE", destination | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sismember( connection_handle, key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( member ) do
    command_list = [ "SISMEMBER", key, member ]
    Connection.process( connection_handle.handle, command_list )
  end

  def smembers( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "SMEMBERS", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def smove( connection_handle, source_key, destination_key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( source_key ) 
       and is_binary( destination_key ) 
       and is_binary( member ) do
    command_list = [ "SMOVE", source_key, destination_key, member ]
    Connection.process( connection_handle.handle, command_list )
  end

  def spop( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "SPOP", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def srandmember( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "SRANDMEMBER", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def srandmember( connection_handle, key, count )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( count ) do
    command_list = [ "SRANDMEMBER", key, count ]
    Connection.process( connection_handle.handle, command_list )
  end

  def srem( connection_handle, key, member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( member_list ) 
       and length( member_list ) > 0 do
    command_list = [ "SREM", key | member_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sscan( connection_handle, key, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    command_list = [ "SSCAN", cursor | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sunion( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "SUNION" | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sunionstore( connection_handle, destination_key, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination_key ) 
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    command_list = [ "SUNIONSTORE", destination_key | key_list ]
    Connection.process( connection_handle.handle, command_list )
  end


  # Sorted Set commands
  def zadd( connection_handle, key, score_member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( score_member_list )
       and length( score_member_list ) > 0
       and rem( length( score_member_list ), 2 ) == 0 do
    command_list = [ "ZADD", key | score_member_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zcard( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "ZCARD", key ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zcount( connection_handle, key, min, max ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( min ) 
       and is_binary( max ) do
    command_list = [ "ZCOUNT", key, min, max ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zincrby( connection_handle, key, increment, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( increment )
       and is_binary( member ) do
    command_list = [ "ZINCRBY", key, increment, member ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zinterstore( connection_handle, destination, key_list, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination ) 
       and is_list( key_list )
       and length( key_list ) > 0 do

    opt_list = []
    if :aggregate in opts and opts[:aggregate] in [ :sum, :min, :max ] do
      opt_list = [ "AGGREGATE", opts[:aggregate] | opt_list ]
    end
    if :weights in opts do
      weight_list = [ "WEIGHTS" | opts[:weights] ]
      opt_list = :lists.append( weight_list, opt_list )
    end

    command_list = [ "ZINTERSTORE", destination, length(key_list) | key_list ]
    command_list = :lists.append( command_list, opt_list )
    Connection.process( connection_handle.handle, command_list )
  end

  def zrange( connection_handle, key, range_start, range_end, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start ) 
       and is_integer( range_end ) do

    opt_list = []
    if :withscores in opts do
      opt_list = [ "WITHSCORES" | opt_list ]
    end
    command_list = [ "ZRANGE", key, range_start, range_end | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zrangebyscore( connection_handle, key, min, max, opts )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( min )
       and is_binary( max ) do

    opt_list = []
    if :limit in opts do
      [ offset, count ] = opts[:limit]
      opt_list = [ "LIMIT", offset, count | opt_list ]
    end

    if :withscore in opts do
      opt_list = [ "WITHSCORE" | opt_list ]
    end

    command_list = [ "ZRANGEBYSCORE", key, min, max | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zrangebyscore( connection_handle, key, min, max, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_number( min )
       and is_number( max ) do
    bin_min = number_to_binary( min )
    bin_max = number_to_binary( max )
    zrangebyscore( connection_handle, key, bin_min, bin_max, opts )
  end


  def zrank( connection_handle, key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    command_list = [ "ZRANGE", key, member ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zrem( connection_handle, key, member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( member_list ) 
       and length( member_list ) > 0 do
    command_list = [ "ZREM", key | member_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zremrangebyrank( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start ) 
       and is_integer( range_end ) do
    command_list = [ "ZREMRANGEBYRANK", key, range_start, range_end ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zremrangebyscore( connection_handle, key, min, max )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( min ) 
       and is_binary( max ) do
    command_list = [ "ZREMRANGEBYSCORE", key, min, max ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zremrangebyscore( connection_handle, key, min, max )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_number( min ) 
       and is_number( max ) do
    zremrangebyscore( connection_handle, key, number_to_binary( min ), number_to_binary( max ) )
  end

  def zrevrange( connection_handle, key, range_start, range_end, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start )
       and is_integer( range_end ) do

    opt_list = []
    if :withscores in opts do
      opt_list = [ "WITHSCORES" | opt_list ] 
    end

    command_list = [ "ZREVRANGE", key, range_start, range_end | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zrevrangebyscore( connection_handle, key, max, min, opts ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( min )
       and is_binary( max ) do

    opt_list = []
    if :limit in opts do
      [ offset, count ] = opts[:limit]
      opt_list = [ "LIMIT", offset, count | opt_list ]
    end

    if :withscore in opts do
      opt_list = [ "WITHSCORE" | opt_list ]
    end

    command_list = [ "ZREVRANGEBYSCORE", key, max, min | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zrevrangebyscore( connection_handle, key, max, min, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_number( min )
       and is_number( max ) do
    zrevrangebyscore( connection_handle, key, number_to_binary( min ), number_to_binary( max ), opts )
  end

  def zrevrank( connection_handle, key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( member ) do
    command_list = [ "ZREVRANK", key, member ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zscan( connection_handle, key, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    command_list = [ "ZSCAN", cursor | opt_list ]
    Connection.process( connection_handle.handle, command_list )
  end


  def zscore( connection_handle, key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( member ) do
    command_list = [ "ZSCORE", key, member ]
    Connection.process( connection_handle.handle, command_list )
  end

  def zunionstore( connection_handle, destination, key_list, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination ) 
       and is_list( key_list )
       and length( key_list ) > 0 do

    opt_list = []
    if :aggregate in opts and opts[:aggregate] in [ :sum, :min, :max ] do
      opt_list = [ "AGGREGATE", opts[:aggregate] | opt_list ]
    end
    if :weights in opts do
      weight_list = [ "WEIGHTS" | opts[:weights] ]
      opt_list = :lists.append( weight_list, opt_list )
    end

    command_list = [ "ZUNIONSTORE", destination, length( key_list ) | key_list ]
    command_list = :lists.append( command_list, opt_list )
    Connection.process( connection_handle.handle, command_list )
  end


  # Pub/Sub commands
  #TODO: Implement these
  # def psubscribe( _client, _pattern ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def psubscribe( _client, _pattern_list ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def pubsub( _client, :channels ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def pubsub( _client, :channels, _pattern ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def pubsub( _client, :numsub, channel ) when is_binary( channel ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def pubsub( _client, :numsub, channel_list ) when is_list( channel_list ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def pubsub( _client, :numpat ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def punsubscribe( _client, _pattern ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def punsubscribe( _client, _pattern_list ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def subscribe( _client, _channel ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def subscribe( _client, _channel_list ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def unsubscribe( _client, _channel ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def unsubscribe( _client, _channel_list ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end

  #TODO: Implement these
  # Transactions
  # def discard( _client ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def exec( _client ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def multi( _client ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def unwatch( _client ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def watch( _client, _key ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end
  # def watch( _client, _key_list ) do
  #   command_list = []
  #   Connection.process( connection_handle.handle, command_list )
  # end

  # Scripting
  def eval( connection_handle, script, key_list, arg_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( script )
       and is_list( key_list ) 
       and is_list( arg_list ) do
    key_arg_list = :lists.append( key_list, arg_list )
    command_list = [ "EVAL", script, length( key_list ) | key_arg_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def evalsha( connection_handle, sha_digest, key_list, arg_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( sha_digest ) 
       and is_list( key_list )
       and is_list( arg_list ) do
    key_arg_list = :lists.append( key_list, arg_list )
    command_list = [ "EVALSHA", sha_digest, length( key_list ) | key_arg_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def script_exists( connection_handle, script_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( script_list )
       and length( script_list ) > 0 do
    command_list = [ "SCRIPT EXISTS" | script_list ]
    Connection.process( connection_handle.handle, command_list )
  end

  def script_flush( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SCRIPT FLUSH" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def script_kill( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SCRIPT KILL" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def script_load( connection_handle, script )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( script ) do
    command_list = [ "SCRIPT LOAD", script ]
    Connection.process( connection_handle.handle, command_list )
  end

  
  # Connection
  def auth( connection_handle, password )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( password ) do
    command_list = [ "AUTH", password ]
    Connection.process( connection_handle.handle, command_list )
  end

  def echo( connection_handle, message )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( message ) do
    command_list = [ "ECHO", message ]
    Connection.process( connection_handle.handle, command_list )
  end

  def ping( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "PING" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def quit( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "QUIT" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def select( connection_handle, index )
      when is_record( connection_handle, ConnectionHandle )
       and is_integer( index )
       and index >= 0 do
    command_list = [ "SELECT", index ]
    Connection.process( connection_handle.handle, command_list )
  end


  # Server
  def bgrewriteaof( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "BGREWRITEAOF" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def bgsave( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "BGSAVE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def client_getname( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "CLIENT GETNAME" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def client_kill( connection_handle, ip, port )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( ip )
       and is_binary( port ) do
    command_list = [ "CLIENT KILL", "#{ip}:#{port}" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def client_list( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "CLIENT LIST" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def client_pause( connection_handle, timeout )
      when is_record( connection_handle, ConnectionHandle )
       and is_integer( timeout )
       and timeout >= 0 do
    command_list = [ "CLIENT PAUSE", timeout ]
    Connection.process( connection_handle.handle, command_list )
  end

  def client_setname( connection_handle, name )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( name ) do
    command_list = [ "CLIENT SETNAME", name ]
    Connection.process( connection_handle.handle, command_list )
  end

  def config_get( connection_handle, parameter )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( parameter ) do
    command_list = [ "CONFIG GET", parameter ]
    Connection.process( connection_handle.handle, command_list )
  end

  def config_resetstat( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "CONFIG RESETSTAT" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def config_rewrite( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "CONFIG REWRITE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def config_set( connection_handle, parameter, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( parameter )
       and ( is_binary( value ) or is_integer( value ) ) do
    command_list = [ "CONFIG SET", parameter, value ]
    Connection.process( connection_handle.handle, command_list )
  end

  def dbsize( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "DBSIZE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def flushall( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "FLUSHALL" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def flushdb( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "FLUSHDB" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def info( connection_handle, section \\ :default )
      when is_record( connection_handle, ConnectionHandle )
       and section in [ :server, :clients, :memory, :persistence, :stats, :replication, :cpu, :commandstats, :cluster, :keyspace, :all, :default ] do
    command_list = [ "INFO", section ]
    Connection.process( connection_handle.handle, command_list )
  end

  def lastsave( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "LASTSAVE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def save( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SAVE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def shutdown( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SHUTDOWN" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def shutdown( connection_handle, :save )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SHUTDOWN SAVE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def shutdown( connection_handle, :nosave )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SHUTDOWN NOSAVE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def slaveof( connection_handle, host, port )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( host )
       and is_binary( port ) do
    command_list = [ "SLAVEOF", host, port ]
    Connection.process( connection_handle.handle, command_list )
  end

  def slaveof( connection_handle, :noone )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SLAVEOF", "NO", "ONE" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def slowlog( connection_handle, subcommand, arguments \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and subcommand in [ :get, :len, :reset ] 
       and is_list( arguments ) do
    command_list = [ "SLOWLOG", atom_to_binary( subcommand ) | arguments ]
    Connection.process( connection_handle.handle, command_list )
  end

  def sync( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "SYNC" ]
    Connection.process( connection_handle.handle, command_list )
  end

  def time( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    command_list = [ "TIME" ]
    Connection.process( connection_handle.handle, command_list )
  end

  defp number_to_binary( number ) when is_integer( number ), do: integer_to_binary( number )
  defp number_to_binary( number ) when is_float( number ), do: float_to_binary( number )
end
