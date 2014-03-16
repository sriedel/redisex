defmodule RedisEx.Connection do
  use GenServer.Behaviour
  alias RedisEx.RespReceiver
  alias RedisEx.Proto

  @handled_commands ~W[ DEL 
                        DUMP 
                        EXISTS
                        EXPIRE
                        EXPIREAT
                        KEYS
                        MIGRATE
                        MOVE
                        OBJECT
                        PERSIST
                        PEXPIRE
                        PEXPIREAT
                        PTTL
                        RANDOMKEY
                        RENAME
                        RENAMENX
                        RESTORE
                        SCAN
                        SORT
                        TTL
                        TYPE
                        APPEND
                        BITCOUNT
                        BITOP
                        BITPOS
                        DECR
                        DECRBY
                        GET
                        GETBIT
                        GETRANGE
                        GETSET
                        INCR
                        INCRBY
                        INCRBYFLOAT
                        MGET
                        MSET
                        MSETNX
                        PSETEX
                        SET
                        SETBIT
                        SETEX
                        SETNX
                        SETRANGE
                        STRLEN
                        HDEL
                        HEXISTS
                        HGET
                        HGETALL
                        HINCRBY
                        HINCRBYFLOAT
                        HKEYS
                        HLEN
                        HMGET
                        HMSET
                        HSCAN
                        HSET
                        HSETNX
                        HVALS
                        BLPOP
                        BRPOP
                        BRPOPLPUSH
                        LINDEX
                        LINSERT
                        LLEN
                        LPOP
                        LPUSH
                        LPUSHX
                        LRANGE
                        LREM
                        LSET
                        LTRIM
                        RPOP
                        RPOPLPUSH
                        RPUSH
                        RPUSHX
                        SADD
                        SCARD
                        SDIFF
                        SDIFFSTORE
                        SINTER
                        SINTERSTORE
                        SISMEMBER
                        SMEMBERS
                        SMOVE
                        SPOP
                        SRANDMEMBER
                        SREM
                        SSCAN
                        SUNION
                        SUNIONSTORE
                        ZADD
                        ZCARD
                        ZCOUNT
                        ZINCRBY
                        ZINTERSTORE
                        ZRANGE
                        ZRANGEBYSCORE
                        ZRANGE
                        ZREM
                        ZREMRANGEBYRANK
                        ZREMRANGEBYSCORE
                        ZREVRANGE
                        ZREVRANGEBYSCORE
                        ZREVRANK
                        ZSCAN
                        ZSCORE
                        ZUNIONSTORE
                        DISCARD
                        EXEC
                        MULTI
                        UNWATCH
                        WATCH
                        EVAL
                        EVALSHA
                        SCRIPT
                        AUTH
                        ECHO
                        PING
                        QUIT
                        SELECT
                        BGREWRITEAOF
                        BGSAVE
                        CLIENT
                        CONFIG
                        DBSIZE
                        FLUSHALL
                        FLUSHDB
                        INFO
                        LASTSAVE
                        SAVE
                        SHUTDOWN
                        SLAVEOF
                        SLOWLOG
                        SYNC
                        TIME
                      ]
 

  def process( server_pid, command_list ) when is_pid( server_pid ) 
                                           and is_list( command_list ) do
    :gen_server.call( server_pid, command_list )
  end

  def start_link( args, opts \\ [] ) do
    :gen_server.start_link( __MODULE__, args, opts )
  end

  def init( [ hostname: hostname, port: port ] ) when is_binary( hostname ) 
                                  and is_integer( port ) 
                                  and port in ( 0..65535 ) do
    socket = connect( hostname, port )
    { :ok, [ socket: socket ] }
  end

  def handle_call( clist = [ "QUIT" | _ ], _from, state ) do
    socket = state[:socket]
    send_command( socket, clist )
    reply = RespReceiver.get_response( socket )
    disconnect( socket )
    { :stop, :normal, reply, [] }
  end

  def handle_call( clist = [ "SHUTDOWN" | _ ], _from, state ) do
    socket = state[:socket]
    send_command( socket, clist )
    reply = RespReceiver.get_response( socket )
    disconnect( socket )
    { :stop, :normal, reply, [] }
  end

  def handle_call( clist = [ command | _ ], _from, state ) when command in @handled_commands do
    socket = state[:socket]
    send_command( socket, clist )
    reply = RespReceiver.get_response( socket )
    { :reply, reply, state }
  end

  def terminate( _reason, state ) do
    case state[:socket] do
      nil    -> true
      socket -> send_command( socket, [ "QUIT" ] )
                disconnect( socket)
    end

    { :stop, :shutdown, [] }
  end

  defp connect( host, port ) when is_binary( host ) 
                             and is_integer( port ) do
    { :ok, host_list } = String.to_char_list( host )
    connect( host_list, port )
  end

  defp connect( host, port ) when is_list( host ) and 
                                 is_integer( port ) do
    { :ok, socket } = :gen_tcp.connect( host, 
                                        port,
                                        [ :binary, 
                                          { :packet, :line },
                                          { :nodelay, true },
                                          { :keepalive, true },
                                          { :packet_size, 10_000_000 },
                                          { :active, false } ] )
    socket
  end

  defp disconnect( sock ) do
    :ok = :gen_tcp.close( sock )
  end

  defp send_command( socket, command_list ) when is_list( command_list ) do
    send_data( socket, Proto.to_proto( command_list ) )
  end

  defp send_data( socket, data ) do
    :ok = :gen_tcp.send( socket, data )
  end

end
