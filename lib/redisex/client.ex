defmodule RedisEx.Client do
  #TODO: Unit test
  alias RedisEx.ConnectionSupervisor
  import RedisEx.Connection, only: [ process: 2 ]

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
    [ "DEL", key ] 
    |> process( connection_handle.handle )
  end

  def del( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "DEL" | key_list ] 
    |> process( connection_handle.handle )
  end

  def dump( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "DUMP", key ] 
    |> process( connection_handle.handle )
  end

  def exists( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "EXISTS", key ] 
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def expire( connection_handle, key, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( seconds ) 
       and seconds >= 0 do
    [ "EXPIRE", key, integer_to_binary( seconds ) ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def expireat( connection_handle, key, timestamp )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( timestamp ) do
    [ "EXPIREAT", key, integer_to_binary( timestamp ) ]
    |> process( connection_handle.handle )
  end

  def keys( connection_handle, pattern )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( pattern ) do
    [ "KEYS", pattern ]
    |> process( connection_handle.handle )
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
    if opts[:replace], do: opt_list = [ "REPLACE" | opt_list ]
    if opts[:copy], do: opt_list = [ "COPY" | opt_list ]

    [ "MIGRATE", host, port, key, integer_to_binary(db), integer_to_binary(timeout) | opt_list ]
    |> process( connection_handle.handle )
  end

  def move( connection_handle, key, db )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( db ) 
       and db >= 0 do
    [ "MOVE", key, db ]
    |> process( connection_handle.handle )
  end

  def object( connection_handle, subcommand, arguments ) 
      when is_record( connection_handle, ConnectionHandle )
       and subcommand in [:REFCOUNT, :ENCODING, :IDLETIME]
       and is_list( arguments ) do
    [ "OBJECT", atom_to_binary( subcommand ) | arguments ]
    |> process( connection_handle.handle )
  end

  def persist( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "PERSIST", key ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def pexpire( connection_handle, key, milliseconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( milliseconds ) 
       and milliseconds > 0 do
    [ "PEXPIRE", key, integer_to_binary( milliseconds ) ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def pexpireat( connection_handle, key, millisecond_timestamp )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( millisecond_timestamp ) 
       and millisecond_timestamp > 0 do
    [ "PEXPIREAT", key, integer_to_binary( millisecond_timestamp ) ]
    |> process( connection_handle.handle )
  end

  def pttl( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    result = [ "PTTL", key ]
             |> process( connection_handle.handle )

    case result do
      -2 -> nil
      -1 -> :no_ttl
      x  -> x
    end
  end

  def randomkey( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "RANDOMKEY" ]
    |> process( connection_handle.handle )
  end

  def rename( connection_handle, key, newkey )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( newkey ) do
    [ "RENAME", key, newkey ]
    |> process( connection_handle.handle )
  end

  def renamenx( connection_handle, key, newkey )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( newkey ) do
    [ "RENAMENX", key, newkey ]
    |> process( connection_handle.handle )
  end

  def restore( connection_handle, key, ttl, serialized_value )
      when is_record( connection_handle, ConnectionHandle )
       and is_integer( ttl )
       and ttl >= 0
       and is_binary( key ) 
       and is_binary( serialized_value ) do
    [ "RESTORE", key, integer_to_binary( ttl ), serialized_value ]
    |> process( connection_handle.handle )
  end

  def scan( connection_handle, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    [ "SCAN", cursor | opt_list ]
    |> process( connection_handle.handle )
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
      :asc  -> opt_list = [ "ASC" | opt_list ]
      :desc -> opt_list = [ "DESC" | opt_list ]
      _     -> opt_list
    end

    case opts[:limit] do
      a..b when is_integer( a ) and is_integer(b) -> 
        opt_list = [ "LIMIT", integer_to_binary(a), integer_to_binary(b) | opt_list ]
      _ -> opt_list
    end

    case opts[:by] do
      keypattern when is_binary( keypattern ) -> 
        opt_list = [ "BY", keypattern | opt_list ]
      _ -> opt_list
    end

    case opts[:get] do
      [ keypattern, "#" ] when is_binary( keypattern ) -> 
        opt_list = [ "GET", keypattern, "GET", "#" | opt_list ]

      keypattern when is_binary( keypattern ) -> 
        opt_list = [ "GET", keypattern | opt_list ]

      "#" -> opt_list = [ "GET", "#" | opt_list ]
      _   -> opt_list
    end

    [ "SORT", key | opt_list ]
    |> process( connection_handle.handle )
  end

  def ttl( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    result = [ "TTL", key ]
             |> process( connection_handle.handle )

    case result do
      -2 -> nil
      -1 -> :no_ttl
      x  -> x
    end
  end

  def type( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    result = [ "TYPE", key ]
             |> process( connection_handle.handle )
    case result do
      "string" -> :string
      "list"   -> :list
      "hash"   -> :hash
      "set"    -> :set
      "zset"   -> :zset
      _        -> :none
    end
  end

  # String Commands
  def append( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( value ) do
    [ "APPEND", key, value ]
    |> process( connection_handle.handle )
  end
                                          
  def bitcount( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) do
    [ "BITCOUNT", key ]
    |> process( connection_handle.handle )
  end

  def bitcount( connection_handle, key, range_start, range_end ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( range_start )
       and is_integer( range_end ) do
    [ "BITCOUNT", key, integer_to_binary(range_start), integer_to_binary(range_end) ]
    |> process( connection_handle.handle )
  end

  def bitop( connection_handle, :NOT, dest_key, key ) 
      when is_binary( key ) 
       and is_binary( dest_key )
       and is_record( connection_handle, ConnectionHandle ) do
    [ "BITOP", "NOT", dest_key, key ]
    |> process( connection_handle.handle )
  end

  def bitop( connection_handle, op, dest_key, key_list ) 
      when op in [:AND, :OR, :XOR] 
       and is_list( key_list)  
       and is_binary( dest_key )
       and is_list( key_list )
       and is_record( connection_handle, ConnectionHandle ) do
    [ "BITOP", atom_to_binary( op ), dest_key | key_list ]
    |> process( connection_handle.handle )
  end

  def bitpos( connection_handle, key, bit ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ] do
    [ "BITPOS", key, integer_to_binary( bit ) ]
    |> process( connection_handle.handle )
  end

  def bitpos( connection_handle, key, bit, range_start )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ]
       and is_integer( range_start ) do
    [ "BITPOS", key, integer_to_binary( bit ), integer_to_binary( range_start ) ]
    |> process( connection_handle.handle )
  end

  def bitpos( connection_handle, key, bit, range_start, range_end ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and bit in [ 0, 1 ] 
       and is_integer( range_start )
       and is_integer( range_end ) do
    [ "BITPOS", key, integer_to_binary( bit ), integer_to_binary( range_start ), integer_to_binary( range_end ) ]
    |> process( connection_handle.handle )
  end

  def decr( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "DECR", key ]
    |> process( connection_handle.handle )
  end

  def decrby( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( increment ) do
    [ "DECRBY", key, integer_to_binary( increment ) ]
    |> process( connection_handle.handle )
  end

  def get( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "GET", key ]
    |> process( connection_handle.handle )
  end

  def getbit( connection_handle, key, offset ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( offset )
       and offset >= 0 do
    [ "GETBIT", key, integer_to_binary( offset ) ]
    |> process( connection_handle.handle )
  end

  def getrange( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( range_start )
       and is_integer( range_end ) do
    [ "GETRANGE", key, integer_to_binary(range_start), integer_to_binary(range_end) ]
    |> process( connection_handle.handle )
  end

  def getset( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( value ) do
    [ "GETSET", key, value ]
    |> process( connection_handle.handle )
  end

  def incr( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) do
    [ "INCR", key ]
    |> process( connection_handle.handle )
  end

  def incrby( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( increment ) do
    [ "INCRBY", key, integer_to_binary( increment ) ]
    |> process( connection_handle.handle )
  end

  def incrbyfloat( connection_handle, key, increment ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( increment ) do
    [ "INCRBYFLOAT", key, increment ] 
    |> process( connection_handle.handle )
    |> binary_to_number
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
    [ "MGET" | key_list ]
    |> process( connection_handle.handle )
  end

  def mset( connection_handle, key_value_list ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_list( key_value_list )
       and length( key_value_list ) > 0
       and rem( length( key_value_list ), 2 ) == 0 do
    [ "MSET" | key_value_list ]
    |> process( connection_handle.handle )
  end

  def msetnx( connection_handle, key_value_list ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_list( key_value_list )
       and length( key_value_list ) > 0
       and rem( length( key_value_list ), 2 ) == 0 do
    [ "MSETNX" | key_value_list ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def psetex( connection_handle, key, milliseconds, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( milliseconds )
       and is_binary( value ) do
    [ "PSETEX", key, integer_to_binary( milliseconds ), value ]
    |> process( connection_handle.handle )
  end

  #TODO: Support set options
  def set( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( value ) do
    [ "SET", key, value ]
    |> process( connection_handle.handle )
  end

  def setbit( connection_handle, key, offset, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( offset )
       and offset >= 0
       and value in [ 0, 1 ] do
    [ "SETBIT", key, integer_to_binary( offset ), integer_to_binary( value ) ]
    |> process( connection_handle.handle )
  end

  def setex( connection_handle, key, seconds, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_integer( seconds )
       and seconds >= 0
       and is_binary( value ) do
    [ "SETEX", key, integer_to_binary( seconds ), value ]
    |> process( connection_handle.handle )
  end

  def setnx( connection_handle, key, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key )
       and is_binary( value ) do
    [ "SETNX", key, value ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def setrange( connection_handle, key, offset, value ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) 
       and is_integer( offset )
       and offset >= 0
       and is_binary( value ) do
    [ "SETRANGE", key, integer_to_binary( offset ), value ]
    |> process( connection_handle.handle )
  end

  def strlen( connection_handle, key ) 
      when is_record( connection_handle, ConnectionHandle ) 
       and is_binary( key ) do
    [ "STRLEN", key ]
    |> process( connection_handle.handle )
  end

  # Hash commands
  def hdel( connection_handle, key, field_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( field_list )
       and length( field_list ) > 0 do
    [ "HDEL", key | field_list ]
    |> process( connection_handle.handle )
  end

  def hexists( connection_handle, key, field )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) do
    [ "HEXISTS", key, field ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def hget( connection_handle, key, field )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) do
    [ "HGET", key, field ]
    |> process( connection_handle.handle )
  end

  def hgetall( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "HGETALL", key ]
    |> process( connection_handle.handle )
  end

  def hincrby( connection_handle, key, field, increment )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field )
       and is_integer( increment ) do
    [ "HINCRBY", key, field, integer_to_binary( increment ) ]
    |> process( connection_handle.handle )
  end

  def hincrbyfloat( connection_handle, key, field, increment )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) 
       and is_binary( increment ) do
    [ "HINCRBYFLOAT", key, field, increment ]
    |> process( connection_handle.handle )
    |> binary_to_number
  end

  def hincrbyfloat( connection_handle, key, field, increment )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) 
       and is_float( increment ) do
    hincrbyfloat( connection_handle, key, field, float_to_binary( increment ) )
  end

  def hkeys( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "HKEYS", key ]
    |> process( connection_handle.handle )
  end

  def hlen( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "HLEN", key ]
    |> process( connection_handle.handle )
  end

  def hmget( connection_handle, key, field_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( field_list )
       and length( field_list ) > 0 do
    [ "HMGET", key | field_list ]
    |> process( connection_handle.handle )
  end

  def hmset( connection_handle, key, field_value_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( field_value_list )
       and length( field_value_list ) > 0
       and rem( length( field_value_list ), 2 ) == 0 do
    [ "HMSET", key | field_value_list ]
    |> process( connection_handle.handle )
  end

  def hscan( connection_handle, key, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    [ "HSCAN", cursor | opt_list ]
    |> process( connection_handle.handle )
  end

  def hset( connection_handle, key, field, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field )
       and is_binary( value ) do
    result = [ "HSET", key, field, value ]
             |> process( connection_handle.handle )
    case result do
      1 -> :insert
      0 -> :update
      other -> other
    end
  end

  def hsetnx( connection_handle, key, field, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( field ) 
       and is_binary( value ) do
    [ "HSETNX", key, field, value ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def hvals( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "HVALS", key ]
    |> process( connection_handle.handle )
  end


  # List commands
  def blpop( connection_handle, key_list, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0
       and is_integer( seconds )
       and seconds >= 0 do
    [ "BLPOP" | key_list ] ++ [ integer_to_binary( seconds ) ]
    |> process( connection_handle.handle )
  end

  def brpop( connection_handle, key_list, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list )
       and length( key_list ) > 0
       and is_integer( seconds )
       and seconds >= 0 do
    [ "BRPOP" | key_list ] ++ [ integer_to_binary( seconds ) ]
    |> process( connection_handle.handle )
  end

  def brpoplpush( connection_handle, source, destination, seconds )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( source )
       and is_binary( destination )
       and is_integer( seconds )
       and seconds >= 0 do
    [ "BRPOPLPUSH", source, destination, integer_to_binary( seconds ) ]
    |> process( connection_handle.handle )
  end

  def lindex( connection_handle, key, index )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( index ) do
    [ "LINDEX", key, integer_to_binary( index ) ]
    |> process( connection_handle.handle )
  end

  def linsert( connection_handle, key, where, pivot, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and where in [ :before, :after ]
       and is_binary( pivot )
       and is_binary( value ) do
    [ "LINSERT", key, atom_to_binary( where ), pivot, value ]
    |> process( connection_handle.handle )
  end

  def llen( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "LLEN", key ]
    |> process( connection_handle.handle )
  end

  def lpop( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "LPOP", key ]
    |> process( connection_handle.handle )
  end

  def lpush( connection_handle, key, value_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 

       and is_list( value_list )
       and length( value_list ) > 0 do
    [ "LPUSH", key | value_list ]
    |> process( connection_handle.handle )
  end

  def lpushx( connection_handle, key, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( value ) do
    [ "LPUSHX", key, value ]
    |> process( connection_handle.handle )
  end

  def lrange( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_integer( range_start )
       and is_integer( range_end ) do
    [ "LRANGE", key, integer_to_binary( range_start ), integer_to_binary( range_end ) ]
    |> process( connection_handle.handle )
  end

  def lrem( connection_handle, key, count, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( count ) 
       and is_binary( value ) do
    [ "LREM", key, integer_to_binary( count ), value ]
    |> process( connection_handle.handle )
  end

  def lset( connection_handle, key, index, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( index )
       and is_binary( value ) do
    [ "LSET", key, integer_to_binary( index ), value ]
    |> process( connection_handle.handle )
  end

  def ltrim( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start )
       and is_integer( range_end ) do
    [ "LTRIM", key, integer_to_binary( range_start ), integer_to_binary( range_end ) ]
    |> process( connection_handle.handle )
  end

  def rpop( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "RPOP", key ]
    |> process( connection_handle.handle )
  end

  def rpoplpush( connection_handle, source, destination )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( source )
       and is_binary( destination ) do
    [ "RPOPLPUSH", source, destination ]
    |> process( connection_handle.handle )
  end

  def rpush( connection_handle, key, value_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( value_list )
       and length( value_list ) > 0 do
    [ "RPUSH", key | value_list ]
    |> process( connection_handle.handle )
  end

  def rpushx( connection_handle, key, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( value ) do
    [ "RPUSHX", key, value ]
    |> process( connection_handle.handle )
  end


  # Set commands
  def sadd( connection_handle, key, member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_list( member_list )
       and length( member_list ) > 0 do
    [ "SADD", key | member_list ]
    |> process( connection_handle.handle )
  end

  def scard( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "SCARD", key ]
    |> process( connection_handle.handle )
  end

  def sdiff( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "SDIFF" | key_list ]
    |> process( connection_handle.handle )
  end

  def sdiffstore( connection_handle, destination, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "SDIFFSTORE", destination | key_list ]
    |> process( connection_handle.handle )
  end

  def sinter( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "SINTER" | key_list ]
    |> process( connection_handle.handle )
  end

  def sinterstore( connection_handle, destination, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "SINTERSTORE", destination | key_list ]
    |> process( connection_handle.handle )
  end

  def sismember( connection_handle, key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( member ) do
    [ "SISMEMBER", key, member ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def smembers( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "SMEMBERS", key ]
    |> process( connection_handle.handle )
  end

  def smove( connection_handle, source_key, destination_key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( source_key ) 
       and is_binary( destination_key ) 
       and is_binary( member ) do
    [ "SMOVE", source_key, destination_key, member ]
    |> process( connection_handle.handle )
    |> integer_result_to_boolean
  end

  def spop( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "SPOP", key ]
    |> process( connection_handle.handle )
  end

  def srandmember( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "SRANDMEMBER", key ]
    |> process( connection_handle.handle )
  end

  def srandmember( connection_handle, key, count )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( count ) do
    [ "SRANDMEMBER", key, count ]
    |> process( connection_handle.handle )
  end

  def srem( connection_handle, key, member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( member_list ) 
       and length( member_list ) > 0 do
    [ "SREM", key | member_list ]
    |> process( connection_handle.handle )
  end

  def sscan( connection_handle, key, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    [ "SSCAN", cursor | opt_list ]
    |> process( connection_handle.handle )
  end

  def sunion( connection_handle, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "SUNION" | key_list ]
    |> process( connection_handle.handle )
  end

  def sunionstore( connection_handle, destination_key, key_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination_key ) 
       and is_list( key_list ) 
       and length( key_list ) > 0 do
    [ "SUNIONSTORE", destination_key | key_list ]
    |> process( connection_handle.handle )
  end


  # Sorted Set commands
  def zadd( connection_handle, key, score_member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( score_member_list )
       and length( score_member_list ) > 0
       and rem( length( score_member_list ), 2 ) == 0 do

    normalized_score_member_list = Enum.map( score_member_list, 
                                             fn(x) -> number_to_binary( x ) end )
         
    [ "ZADD", key | normalized_score_member_list ]
    |> process( connection_handle.handle )
  end

  def zcard( connection_handle, key )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) do
    [ "ZCARD", key ]
    |> process( connection_handle.handle )
  end

  #TODO: BEtter guarding of mix, max values
  def zcount( connection_handle, key, min, max ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( min ) 
       and is_binary( max ) do
    [ "ZCOUNT", key, min, max ]
    |> process( connection_handle.handle )
  end
  def zcount( connection_handle, key, min, max ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and ( is_integer( min ) or is_float( min ) )
       and ( is_integer( max ) or is_float( max ) ) do
    
    bin_min = number_to_binary( min )
    bin_max = number_to_binary( max )
    zcount( connection_handle, key, bin_min, bin_max )
  end

  #TODO: Better guard increment
  def zincrby( connection_handle, key, increment, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( increment )
       and is_binary( member ) do
     [ "ZINCRBY", key, increment, member ]
    |> process( connection_handle.handle )
    |> binary_to_score
  end

  def zincrby( connection_handle, key, increment, member ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_number( increment )
       and is_binary( member ) do
    zincrby( connection_handle, key, number_to_binary( increment ), member )
  end

  def zinterstore( connection_handle, destination, key_list, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination ) 
       and is_list( key_list )
       and length( key_list ) > 0 do

    opt_list = []

    if is_list( opts[:weights] ) do
      normalized_weights_list = Enum.map( opts[:weights],
                                          fn (x) -> number_to_binary(x) end )
      opt_list = [ "WEIGHTS" | :lists.concat( [ normalized_weights_list, opt_list ] ) ]
    end

    if is_atom( opts[:aggregate] ) and opts[:aggregate] in [ :sum, :min, :max ] do
      opt_list = [ "AGGREGATE", atom_to_binary( opts[:aggregate] ) | opt_list ]
    end

    command_list = [ "ZINTERSTORE", destination, integer_to_binary( length(key_list) ) | key_list ]
    :lists.append( command_list, opt_list )
    |> process( connection_handle.handle )
  end

  def zrange( connection_handle, key, range_start, range_end, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start ) 
       and is_integer( range_end ) do

    opt_list = []
    if opts[:withscores] do
      opt_list = [ "WITHSCORES" | opt_list ]
    end

    result = [ "ZRANGE", key, integer_to_binary( range_start ), integer_to_binary( range_end ) | opt_list ]
             |> process( connection_handle.handle )


    if opts[:withscores] do
      process_scorelist( result )
    else
      result
    end
  end

  def zrangebyscore( connection_handle, key, min, max, opts )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( min )
       and is_binary( max ) do

    opt_list = []
    if is_list( opts[:limit] ) do
      [ offset, count ] = opts[:limit]
      opt_list = [ "LIMIT", integer_to_binary(offset), integer_to_binary(count) | opt_list ]
    end

    if opts[:withscores], do: opt_list = [ "WITHSCORES" | opt_list ]

    result = [ "ZRANGEBYSCORE", key, min, max | opt_list ]
             |> process( connection_handle.handle )

    if opts[:withscores] do
      process_scorelist( result )
    else
      result
    end
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
    [ "ZRANK", key, member ]
    |> process( connection_handle.handle )
  end

  def zrem( connection_handle, key, member_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_list( member_list ) 
       and length( member_list ) > 0 do
    [ "ZREM", key | member_list ]
    |> process( connection_handle.handle )
  end

  def zremrangebyrank( connection_handle, key, range_start, range_end )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_integer( range_start ) 
       and is_integer( range_end ) do
    [ "ZREMRANGEBYRANK", key, integer_to_binary( range_start), integer_to_binary( range_end ) ]
    |> process( connection_handle.handle )
  end

  def zremrangebyscore( connection_handle, key, min, max )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( min ) 
       and is_binary( max ) do
    [ "ZREMRANGEBYSCORE", key, min, max ]
    |> process( connection_handle.handle )
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
    if opts[:withscores], do: opt_list = [ "WITHSCORES" | opt_list ] 

    result = [ "ZREVRANGE", key, integer_to_binary(range_start), integer_to_binary(range_end) | opt_list ]
             |> process( connection_handle.handle )
    if opts[:withscores] do
      process_scorelist( result )
    else
      result
    end
  end

  def zrevrangebyscore( connection_handle, key, max, min, opts ) 
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key ) 
       and is_binary( min )
       and is_binary( max ) do

    opt_list = []
    if is_list( opts[:limit] ) do 
      [ offset, count ] = opts[:limit]
      opt_list = [ "LIMIT", integer_to_binary( offset ), integer_to_binary( count ) | opt_list ]
    end

    if opts[:withscores], do: opt_list = [ "WITHSCORES" | opt_list ]

    result = [ "ZREVRANGEBYSCORE", key, max, min | opt_list ]
             |> process( connection_handle.handle )

    if opts[:withscores] do
      process_scorelist( result )
    else
      result
    end
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
    [ "ZREVRANK", key, member ]
    |> process( connection_handle.handle )
  end

  def zscan( connection_handle, key, cursor, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( cursor ) do
    opt_list = []
    if :count in opts, do: opt_list = [ "COUNT", opts[:count] | opt_list ]
    if :match in opts, do: opt_list = [ "MATCH", opts[:match] | opt_list ]

    [ "ZSCAN", cursor | opt_list ]
    |> process( connection_handle.handle )
  end


  def zscore( connection_handle, key, member )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( key )
       and is_binary( member ) do
    result = [ "ZSCORE", key, member ]
             |> process( connection_handle.handle )
    case result do
      nil -> nil
      x   -> binary_to_number( x )
    end
  end

  def zunionstore( connection_handle, destination, key_list, opts \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( destination ) 
       and is_list( key_list )
       and length( key_list ) > 0 do

    opt_list = []

    if is_list( opts[:weights] ) do
      normalized_weights_list = Enum.map( opts[:weights],
                                          fn (x) when is_binary(x) -> x
                                             (x) when is_number(x) -> number_to_binary(x)
                                          end )
        opt_list = [ "WEIGHTS" | :lists.concat( [ normalized_weights_list, opt_list ] ) ]
    end

    if is_atom( opts[:aggregate] ) and opts[:aggregate] in [ :sum, :min, :max ] do
      opt_list = [ "AGGREGATE", atom_to_binary( opts[:aggregate] ) | opt_list ]
    end

    command_list = [ "ZUNIONSTORE", destination, integer_to_binary( length(key_list) ) | key_list ]
    :lists.append( command_list, opt_list )
    |> process( connection_handle.handle )
  end


  # Pub/Sub commands
  #TODO: Implement these
  # def psubscribe( _client, _pattern ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def psubscribe( _client, _pattern_list ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def pubsub( _client, :channels ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def pubsub( _client, :channels, _pattern ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def pubsub( _client, :numsub, channel ) when is_binary( channel ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def pubsub( _client, :numsub, channel_list ) when is_list( channel_list ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def pubsub( _client, :numpat ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def punsubscribe( _client, _pattern ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def punsubscribe( _client, _pattern_list ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def subscribe( _client, _channel ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def subscribe( _client, _channel_list ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def unsubscribe( _client, _channel ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def unsubscribe( _client, _channel_list ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end

  #TODO: Implement these
  # Transactions
  # def discard( _client ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def exec( _client ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def multi( _client ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def unwatch( _client ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def watch( _client, _key ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end
  # def watch( _client, _key_list ) do
  #   command_list = []
  #   |> process( connection_handle.handle )
  # end

  # Scripting
  def eval( connection_handle, script, key_list, arg_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( script )
       and is_list( key_list ) 
       and is_list( arg_list ) do
    key_arg_list = :lists.append( key_list, arg_list )
    [ "EVAL", script, integer_to_binary( length( key_list ) ) | key_arg_list ]
    |> process( connection_handle.handle )
  end

  def evalsha( connection_handle, sha_digest, key_list, arg_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( sha_digest ) 
       and is_list( key_list )
       and is_list( arg_list ) do
    key_arg_list = :lists.append( key_list, arg_list )
    [ "EVALSHA", sha_digest, integer_to_binary( length( key_list ) ) | key_arg_list ]
    |> process( connection_handle.handle )
  end

  def script_exists( connection_handle, script_list )
      when is_record( connection_handle, ConnectionHandle )
       and is_list( script_list )
       and length( script_list ) > 0 do
    [ "SCRIPT", "EXISTS" | script_list ]
    |> process( connection_handle.handle )
  end

  def script_flush( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SCRIPT", "FLUSH" ]
    |> process( connection_handle.handle )
  end

  def script_kill( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SCRIPT", "KILL" ]
    |> process( connection_handle.handle )
  end

  def script_load( connection_handle, script )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( script ) do
    [ "SCRIPT", "LOAD", script ]
    |> process( connection_handle.handle )
  end

  
  # Connection
  def auth( connection_handle, password )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( password ) do
    [ "AUTH", password ]
    |> process( connection_handle.handle )
  end

  def echo( connection_handle, message )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( message ) do
    [ "ECHO", message ]
    |> process( connection_handle.handle )
  end

  def ping( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "PING" ]
    |> process( connection_handle.handle )
  end

  def quit( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "QUIT" ]
    |> process( connection_handle.handle )
  end

  def select( connection_handle, index )
      when is_record( connection_handle, ConnectionHandle )
       and is_integer( index )
       and index >= 0 do
    [ "SELECT", integer_to_binary( index ) ]
    |> process( connection_handle.handle )
  end


  # Server
  def bgrewriteaof( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "BGREWRITEAOF" ]
    |> process( connection_handle.handle )
  end

  def bgsave( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "BGSAVE" ]
    |> process( connection_handle.handle )
  end

  def client_getname( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "CLIENT", "GETNAME" ]
    |> process( connection_handle.handle )
  end

  def client_kill( connection_handle, ip, port )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( ip )
       and is_binary( port ) do
    [ "CLIENT", "KILL", "#{ip}:#{port}" ]
    |> process( connection_handle.handle )
  end

  def client_list( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "CLIENT", "LIST" ]
    |> process( connection_handle.handle )
  end

  def client_pause( connection_handle, timeout )
      when is_record( connection_handle, ConnectionHandle )
       and is_integer( timeout )
       and timeout >= 0 do
    [ "CLIENT", "PAUSE", integer_to_binary( timeout ) ]
    |> process( connection_handle.handle )
  end

  def client_setname( connection_handle, name )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( name ) do
    [ "CLIENT", "SETNAME", name ]
    |> process( connection_handle.handle )
  end

  def config_get( connection_handle, parameter )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( parameter ) do
    [ "CONFIG", "GET", parameter ]
    |> process( connection_handle.handle )
  end

  def config_resetstat( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "CONFIG", "RESETSTAT" ]
    |> process( connection_handle.handle )
  end

  def config_rewrite( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "CONFIG", "REWRITE" ]
    |> process( connection_handle.handle )
  end

  def config_set( connection_handle, parameter, value )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( parameter )
       and ( is_binary( value ) or is_integer( value ) ) do
    [ "CONFIG", "SET", parameter, value ]
    |> process( connection_handle.handle )
  end

  def dbsize( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "DBSIZE" ]
    |> process( connection_handle.handle )
  end

  def flushall( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "FLUSHALL" ]
    |> process( connection_handle.handle )
  end

  def flushdb( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "FLUSHDB" ]
    |> process( connection_handle.handle )
  end

  @infosections [ :server, :clients, :memory, :persistence, :stats, :replication, :cpu, :commandstats, :cluster, :keyspace, :all, :default ] 
  def info( connection_handle, section \\ :default )
      when is_record( connection_handle, ConnectionHandle )
       and section in @infosections do
    [ "INFO", atom_to_binary( section ) ]
    |> process( connection_handle.handle )
  end

  def lastsave( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "LASTSAVE" ]
    |> process( connection_handle.handle )
  end

  def save( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SAVE" ]
    |> process( connection_handle.handle )
  end

  def shutdown( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SHUTDOWN" ]
    |> process( connection_handle.handle )
  end

  def shutdown( connection_handle, arg )
      when is_record( connection_handle, ConnectionHandle )
       and arg in [ :save, :nosave ] do
    [ "SHUTDOWN", atom_to_binary( arg ) ]
    |> process( connection_handle.handle )
  end

  def slaveof( connection_handle, host, port )
      when is_record( connection_handle, ConnectionHandle )
       and is_binary( host )
       and is_binary( port ) do
    [ "SLAVEOF", host, port ]
    |> process( connection_handle.handle )
  end

  def slaveof( connection_handle, :noone )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SLAVEOF", "NO", "ONE" ]
    |> process( connection_handle.handle )
  end

  def slowlog( connection_handle, :len )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SLOWLOG", "LEN" ]
    |> process( connection_handle.handle )
  end

  def slowlog( connection_handle, :get )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SLOWLOG", "GET" ]
    |> process( connection_handle.handle )
  end

  def slowlog( connection_handle, :get, pos )
      when is_record( connection_handle, ConnectionHandle ) 
       and is_integer( pos ) do
    [ "SLOWLOG", "GET", integer_to_binary( pos ) ]
    |> process( connection_handle.handle )
  end

  def slowlog( connection_handle, :reset )
      when is_record( connection_handle, ConnectionHandle ) do
    [ "SLOWLOG", "RESET" ]
    |> process( connection_handle.handle )
  end

  def slowlog( connection_handle, subcommand, arguments \\ [] )
      when is_record( connection_handle, ConnectionHandle )
       and subcommand in [ :get, :len, :reset ] 
       and is_list( arguments ) do
    [ "SLOWLOG", atom_to_binary( subcommand ) | arguments ]
    |> process( connection_handle.handle )
  end

  def time( connection_handle )
      when is_record( connection_handle, ConnectionHandle ) do
    [ seconds, micros ] = [ "TIME" ]
                          |> process( connection_handle.handle )
    [ binary_to_integer( seconds ), binary_to_integer( micros ) ]
  end

  defp number_to_binary( number ) when is_integer( number ), do: integer_to_binary( number )
  defp number_to_binary( number ) when is_float( number ), do: float_to_binary( number )
  defp number_to_binary( binary ) when is_binary( binary ), do: binary

  defp binary_to_score( "+inf" ), do: "+inf"
  defp binary_to_score( "-inf" ), do: "-inf"
  defp binary_to_score( score ), do: binary_to_number( score )


  defp binary_to_number( string ) when is_binary( string ) do
    if String.contains?( string, "." ) do
      binary_to_float( string )
    else
      binary_to_integer( string ) * 1.0
    end
  end
  defp binary_to_number( value ), do: value

  defp integer_result_to_boolean( 1 ), do: true
  defp integer_result_to_boolean( 0 ), do: false
  defp integer_result_to_boolean( x ), do: x

  defp process_scorelist( value_score_list ) when is_list( value_score_list ) do
    Enum.chunk( value_score_list, 2 )
    |> Enum.map( fn ( [x, y] ) -> [ x, binary_to_score( y ) ] end )
    |> List.flatten
  end

end
