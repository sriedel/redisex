defmodule RedisExClientTest do
  use ExUnit.Case, async: false
  alias RedisEx.Client

  setup_all do
    spawn( fn() -> :os.cmd( 'redis-server test/fixtures/redis.conf\n' ) end )
    :ok
  end

  #FIXME: Shutdown redis after all tests complete
  # teardown_all do
  #   client = Client.connect( "127.0.0.1", 6333 )
  #   Client.shutdown( client )
  #   :ok
  # end

  setup do
    RedisCli.run( "flushall" )
    client = Client.connect( "127.0.0.1", 6333 )
    { :ok, handle: client }
  end

  teardown meta do
    Client.disconnect( meta[:handle] ) 
  end

  test "del", meta  do
    client = meta[:handle]
    assert Client.del( client, "i_dont_exist" ) == 0

    RedisCli.run( "SET foo bar" )
    assert Client.del( client, "foo" ) == 1

    assert RedisCli.run( "EXISTS foo" ) == [ "0" ]

    RedisCli.run( "SET foo bar" )
    RedisCli.run( "SET foo1 bar" )
    RedisCli.run( "SET foo2 bar" )
    RedisCli.run( "SET foo3 bar" )
    assert Client.del( client, [ "foo", "foo1", "foo2", "foo3" ] ) == 4

    assert RedisCli.run( "EXISTS foo" ) == [ "0" ]
  end

  test "exists", meta do
    client = meta[:handle]

    assert Client.exists( client, "i_dont_exist" ) == false

    RedisCli.run( "SET foo bar" )
    assert Client.exists( client, "foo" ) == true
  end

  test "expire", meta do
    client = meta[:handle]

    assert Client.expire( client, "i_dont_exist", 3600 ) == false

    RedisCli.run( "SET foo bar" )
    assert Client.expire( client, "foo", 3600 ) == true

    assert RedisCli.run( "TTL foo" ) == [ "3600" ]
  end

  # test "expireat", meta do
  # end

  test "keys", meta do
    client = meta[:handle]
    assert Client.keys( client, "*" ) == []

    RedisCli.run( "SET foo bar" )
    assert Client.keys( client, "*" ) == [ "foo" ]

    RedisCli.run( "SET baz quux" )
    assert Client.keys( client, "*" ) |> Enum.sort == Enum.sort( [ "baz", "foo" ] )
    assert Client.keys( client, "f*" ) == [ "foo" ]
    assert Client.keys( client, "b*" ) == [ "baz" ]
  end

  # test "migrate", meta do
  # end

  # test "move", meta do
  # end

  # test "object", meta do
  # end

  test "persist", meta do
    client = meta[:handle]
    assert Client.persist( client, "foo" ) == false
    
    RedisCli.run( "SET bar quux" )
    assert Client.persist( client, "bar" ) == false

    RedisCli.run( "EXPIRE bar 3600" )
    assert Client.persist( client, "bar" ) == true

    assert RedisCli.run( "TTL bar" ) == [ "-1" ] 
  end

  test "pexpire", meta do
    client = meta[:handle]

    assert Client.pexpire( client, "i_dont_exist", 360000 ) == false

    RedisCli.run( "SET foo bar" )
    assert Client.pexpire( client, "foo", 360000 ) == true

    assert RedisCli.run( "TTL foo" ) == [ "360" ]
  end

  # test "pexpireat", meta do
  # end

  test "pttl", meta do
    client = meta[:handle]

    assert Client.pttl( client, "i_dont_exist" ) == nil
    
    RedisCli.run( "SET foo bar" )
    assert Client.pttl( client, "foo" ) == :no_ttl

    RedisCli.run( "PEXPIRE foo 3600" )
    assert Client.pttl( client, "foo" ) > 3590
  end

  test "randomkey", meta do
    client = meta[:handle]

    assert Client.randomkey( client ) == nil

    RedisCli.run( "SET foo bar" )
    assert Client.randomkey( client ) == "foo"
    #TODO: how to test this properly?
  end

  test "rename", meta do
    client = meta[:handle]

    assert Client.rename( client, "i_dont_exist", "foo" ) == { :redis_error, "ERR no such key" }

    RedisCli.run( "SET foo bar" )
    assert Client.rename( client, "foo", "foo" ) == { :redis_error, "ERR source and destination objects are the same" }

    assert Client.rename( client, "foo", "bar" ) == "OK"
    assert RedisCli.run( "GET bar" ) == [ "bar" ]

    RedisCli.run( "SET foo baz" )
    assert Client.rename( client, "foo", "bar" ) == "OK"
    assert RedisCli.run( "GET bar" ) == [ "baz" ]
  end

  test "renamenx", meta do
    client = meta[:handle]

    assert Client.renamenx( client, "i_dont_exist", "foo" ) == { :redis_error, "ERR no such key" }

    RedisCli.run( "SET foo bar" )
    assert Client.renamenx( client, "foo", "foo" ) == { :redis_error, "ERR source and destination objects are the same" }

    assert Client.renamenx( client, "foo", "bar" ) == 1
    assert RedisCli.run( "GET bar" ) == [ "bar" ]

    RedisCli.run( "SET foo baz" )
    assert Client.renamenx( client, "foo", "bar" ) == 0
    assert RedisCli.run( "GET bar" ) == [ "bar" ]
  end

  test "dump and restore", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    dumped = Client.dump( client, "foo" )
    RedisCli.run( "DEL foo" )
    assert Client.restore( client, "foo", 0, dumped ) == "OK"
    assert RedisCli.run( "GET foo" ) == [ "bar" ]
    assert RedisCli.run( "TTL foo" ) == [ "-1" ]


    RedisCli.run( "DEL foo" )

    RedisCli.run( "SET foo bar" )
    dumped = Client.dump( client, "foo" )
    RedisCli.run( "DEL foo" )
    assert Client.restore( client, "foo", 3600000, dumped ) == "OK"
    assert RedisCli.run( "GET foo" ) == [ "bar" ]
    assert RedisCli.run( "TTL foo" ) == [ "3600" ]
  end

  # test "scan", meta do
  # end

  test "sort", meta do
    client = meta[:handle]
    RedisCli.run( "SADD tosort 2 1 3" )

    assert Client.sort( client, "tosort" ) == [ "1", "2", "3"]
    assert Client.sort( client, "tosort", [ order: :asc ] ) == [ "1", "2", "3"]
    assert Client.sort( client, "tosort", [ order: :desc ] ) == [ "3", "2", "1" ]
    
    RedisCli.run( "SADD tosort 2 C 1 A 3 B" )
    assert Client.sort( client, "tosort" ) == { :redis_error, "ERR One or more scores can't be converted into double" }
    
    assert Client.sort( client, "tosort", [ alpha: true ] ) == [ "1", "2", "3", "A", "B", "C" ]
    assert Client.sort( client, "tosort", [ alpha: true, order: :asc ] ) == [ "1", "2", "3", "A", "B", "C" ]
    assert Client.sort( client, "tosort", [ alpha: true, order: :desc ] ) == [ "C", "B", "A", "3", "2", "1" ]

    assert Client.sort( client, "tosort", [ alpha: true, limit: 2..3 ] ) == [ "3", "A", "B" ]
    
    assert Client.sort( client, "tosort", [ alpha: true, limit: 2..3, store: "sorted" ] ) == 3
    assert RedisCli.run( "TYPE sorted" ) == [ "list" ]
    assert RedisCli.run( "LRANGE sorted 0 -1" ) == [ "3", "A", "B" ]

    RedisCli.run( "SET W_1 4" )
    RedisCli.run( "SET W_2 7" )
    RedisCli.run( "SET W_3 1" )
    RedisCli.run( "SET W_4 3" )
    RedisCli.run( "SET W_5 8" )
    RedisCli.run( "SET W_6 2" )
    assert Client.sort( client, "tosort", [ alpha: true, by: "w_*" ] ) |> Enum.sort == [ "C", "B", "A", "1", "3", "2" ] |> Enum.sort

    #TODO: Test GET
  end

  test "ttl", meta do
    client = meta[:handle]

    assert Client.ttl( client, "i_dont_exist" ) == nil

    RedisCli.run( "SET FOO BAR" )
    assert Client.ttl( client, "FOO" ) == :no_ttl

    RedisCli.run( "EXPIRE FOO 3600" )
    assert Client.ttl( client, "FOO" ) == 3600
  end

  test "type", meta do
    client = meta[:handle]

    RedisCli.run( "SET STRING BAR" )
    RedisCli.run( "SADD SET BAZ" )
    RedisCli.run( "ZADD ZSET 4 QUUX" )
    RedisCli.run( "HSET HSH FIELD BLUBB" )
    RedisCli.run( "LPUSH LST FOO" )

    assert Client.type( client, "STRING" ) == :string
    assert Client.type( client, "SET" ) == :set
    assert Client.type( client, "ZSET" ) == :zset
    assert Client.type( client, "HSH" ) == :hash
    assert Client.type( client, "LST" ) == :list
    assert Client.type( client, "i_dont_exist" ) == :none
  end

  test "append", meta do
    client = meta[:handle]

    assert Client.append( client, "i_dont_exist", "foo" ) == 3
    assert RedisCli.run( "GET i_dont_exist" ) == [ "foo" ]

    RedisCli.run( "SET foo bar" )
    assert Client.append( client, "foo", "baz" ) == 6

    assert RedisCli.run( "GET foo" ) == [ "barbaz" ]
  end

  test "bitcount", meta do
    client = meta[:handle]

    RedisCli.run "SET foo foo"
    assert Client.bitcount( client, "foo" ) == 16
    assert Client.bitcount( client, "foo", 1, 1 ) == 6
    assert Client.bitcount( client, "foo", -1, -1 ) == 6
  end

  test "bitop", meta do
    client = meta[:handle]

    RedisCli.run "SET foo bar"
    RedisCli.run "SET baz quux"

    assert Client.bitop( client, :NOT, "notfoo", "foo" ) == 3
    assert Client.bitop( client, :AND, "fooandbaz", ["foo", "baz"] ) == 4
    assert Client.bitop( client, :OR, "fooorbaz", ["foo", "baz"] ) == 4
    assert Client.bitop( client, :XOR, "fooxorbaz", ["foo", "baz"] ) == 4

    assert RedisCli.run( "GET notfoo" ) == [<<194, 157, 194, 158, 194, 141>>]
    assert RedisCli.run( "GET fooandbaz" ) == [<<96, 97, 112, 0>>]
    assert RedisCli.run( "GET fooorbaz" ) == ["suwx"]
    assert RedisCli.run( "GET fooxorbaz" ) == [<<19, 20, 7, 120>>]
  end

  # test "bitpos", meta do
#  #TODO: Re-enable after 2.8.7
#     client = meta[:handle]
# 
#     RedisCli.run "SET foo 0"
#     assert Client.bitpos( client, "foo", 0 ) == 0
#     assert Client.bitpos( client, "foo", 0, 2, 4 ) == 4
#     assert Client.bitpos( client, "foo", 0, 2, 3 ) == -1
  # end

  test "decr", meta do
    client = meta[:handle]

    assert Client.decr( client, "i_dont_exist" ) == -1
    assert Client.decr( client, "i_dont_exist" ) == -2
    assert RedisCli.run( "GET i_dont_exist" ) == [ "-2" ]

    RedisCli.run( "SET foo 10" )
    assert Client.decr( client, "foo" ) == 9
    assert RedisCli.run( "GET foo" ) == [ "9" ]
  end

  test "decrby", meta do
    client = meta[:handle]

    assert Client.decrby( client, "i_dont_exist", 2 ) == -2
    assert RedisCli.run( "GET i_dont_exist" ) == [ "-2" ]

    RedisCli.run( "SET foo 10" )
    assert Client.decrby( client, "foo", 3 ) == 7
    assert RedisCli.run( "GET foo" ) == [ "7" ]
  end


  test "get", meta do
    client = meta[:handle]
    
    assert Client.get( client, "i_dont_exist" ) == nil
    
    RedisCli.run( "SET foo bar" )
    assert Client.get( client, "foo" ) == "bar"
  end

  test "getbit", meta do
    client = meta[:handle]

    assert Client.getbit( client, "i_dont_exist", 17 ) == 0

    RedisCli.run( "SET foo bar" )
    assert Client.getbit( client, "foo", 0 ) == 0
    assert Client.getbit( client, "foo", 1 ) == 1
    assert Client.getbit( client, "foo", 99 ) == 0
  end

  test "getrange", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo quux" )
    assert Client.getrange( client, "foo", 1, 2 ) == "uu"
  end

  test "getset", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    assert Client.getset( client, "foo", "baz" ) == "bar"
    assert RedisCli.run( "GET foo" ) == [ "baz" ]
  end

  test "incr", meta do
    client = meta[:handle]

    assert Client.incr( client, "i_dont_exist" ) == 1
    assert Client.incr( client, "i_dont_exist" ) == 2
    assert RedisCli.run( "GET i_dont_exist" ) == [ "2" ]

    RedisCli.run( "SET foo 10" )
    assert Client.incr( client, "foo" ) == 11
    assert RedisCli.run( "GET foo" ) == [ "11" ]
  end


  test "incrby", meta do
    client = meta[:handle]

    assert Client.incrby( client, "i_dont_exist", 2 ) == 2
    assert Client.incrby( client, "i_dont_exist", 3 ) == 5
    assert RedisCli.run( "GET i_dont_exist" ) == [ "5" ]

    RedisCli.run( "SET foo 10" )
    assert Client.incrby( client, "foo", 5 ) == 15
    assert RedisCli.run( "GET foo" ) == [ "15" ]
  end


  test "incrbyfloat", meta do
    client = meta[:handle]

    assert Client.incrbyfloat( client, "i_dont_exist", 1.0 ) == 1.0
    assert Client.incrbyfloat( client, "i_dont_exist", 0.5 ) == 1.5
    assert RedisCli.run( "GET i_dont_exist" ) == [ "1.5" ]

    RedisCli.run( "SET foo 10" )
    assert Client.incrbyfloat( client, "foo", "1e5" ) == 100010.0
  end

  test "mget", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    RedisCli.run( "SET foo2 bar" )
    RedisCli.run( "SET foo4 bar" )

    assert Client.mget( client, [ "foo", "foo2", "foo3", "foo4" ] ) == [ "bar", "bar", nil, "bar" ]
  end

  test "mset", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    assert Client.mset( client, [ "bar", "baz", "foo", "quux", "bla", "blubb" ] ) == "OK"
    assert RedisCli.run( "GET bar" ) == [ "baz" ]
    assert RedisCli.run( "GET foo" ) == [ "quux" ]
    assert RedisCli.run( "GET bla" ) == [ "blubb" ]
  end

  test "msetnx", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    assert Client.msetnx( client, [ "bar", "baz", "foo", "quux", "bla", "blubb" ] ) == false

    RedisCli.run( "DEL foo" )
    assert Client.mset( client, [ "bar", "baz", "foo", "quux", "bla", "blubb" ] ) == "OK"
    assert RedisCli.run( "GET bar" ) == [ "baz" ]
    assert RedisCli.run( "GET foo" ) == [ "quux" ]
    assert RedisCli.run( "GET bla" ) == [ "blubb" ]
  end

  test "psetex", meta do
    client = meta[:handle]

    assert Client.psetex( client, "foo", 36000, "bar" ) == "OK"
    assert RedisCli.run( "GET foo" ) == [ "bar" ]
    assert RedisCli.run( "TTL foo" ) == [ "36" ]
  end

  test "set", meta do
    client = meta[:handle]

    assert Client.set( client, "foo", "bar" ) == "OK"
    assert RedisCli.run( "GET foo" ) == [ "bar" ]

    assert Client.set( client, "foo", "baz" ) == "OK"
    assert RedisCli.run( "GET foo" ) == [ "baz" ]
  end

  test "setbit", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    assert Client.setbit( client, "foo", 1, 0 ) == 1
    assert RedisCli.run( "GET foo" ) == ["\"ar"]
  end

  test "setex", meta do
    client = meta[:handle]

    assert Client.setex( client, "foo", 36, "bar" ) == "OK"
    assert RedisCli.run( "GET foo" ) == [ "bar" ]
    assert RedisCli.run( "TTL foo" ) == [ "36" ]
  end

  test "setnx", meta do
    client = meta[:handle]

    assert Client.setnx( client, "foo", "bar" ) == true
    assert RedisCli.run( "GET foo" ) == [ "bar" ]

    assert Client.setnx( client, "foo", "quux" ) == false
    assert RedisCli.run( "GET foo" ) == [ "bar" ]
  end


  test "setrange", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo blablubb" )
    assert Client.setrange( client, "foo", 3, "x" ) == 8
    assert RedisCli.run( "GET foo" ) == [ "blaxlubb" ]
  end

  test "strlen", meta do
    client = meta[:handle]

    assert Client.strlen( client, "i_dont_exist" ) == 0
    
    RedisCli.run( "SET foo blablubb" )
    assert Client.strlen( client, "foo" ) == 8
  end

  test "hdel", meta do
    client = meta[:handle]

    assert Client.hdel( client, "i_dont_exist", ["f1"] ) == 0

    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hdel( client, "myhash", ["f1"] ) == 1
    assert Client.hdel( client, "myhash", ["f1"] ) == 0
    assert RedisCli.run( "HGETALL myhash" ) == [ "f2", "v2", "f3", "v3" ]
  end

  test "hexists", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )

    assert Client.hexists( client, "i_dont_exists", "f1" ) == false
    assert Client.hexists( client, "myhash", "f10" ) == false
    assert Client.hexists( client, "myhash", "f1" ) == true
  end

  test "hget", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )

    assert Client.hget( client, "i_dont_exist", "f1" ) == nil
    assert Client.hget( client, "myhash", "i_dont_exist" ) == nil
    assert Client.hget( client, "myhash", "f1" ) == "v1"
  end

  test "hgetall", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hgetall( client, "i_dont_exist" ) == []
    assert Client.hgetall( client, "myhash" ) == [ "f1", "v1", "f2", "v2", "f3", "v3" ]
  end

  test "hincrby", meta do
    client = meta[:handle]
    assert Client.hincrby( client, "i_dont_exist", "i1", 1 ) == 1
    assert RedisCli.run( "HGET i_dont_exist i1" ) == [ "1" ]

    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hincrby( client, "myhash", "i1", 10 ) == 10
    assert RedisCli.run( "HGET myhash i1" ) == [ "10" ]
    assert Client.hincrby( client, "myhash", "i1", 10 ) == 20
    assert RedisCli.run( "HGET myhash i1" ) == [ "20" ]
  end

  test "hincrbyfloat", meta do
    client = meta[:handle]
    assert Client.hincrbyfloat( client, "i_dont_exist", "i1", 1.0 ) == 1.0
    assert RedisCli.run( "HGET i_dont_exist i1" ) == [ "1" ]

    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hincrbyfloat( client, "myhash", "i1", "1e5" ) == 100000.0
    assert RedisCli.run( "HGET myhash i1" ) == [ "100000" ]
    assert Client.hincrbyfloat( client, "myhash", "i1", 10.0 ) == 100010.0
    assert RedisCli.run( "HGET myhash i1" ) == [ "100010" ]
  end

  test "hkeys", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )

    assert Client.hkeys( client, "i_dont_exist" ) == []
    assert Client.hkeys( client, "myhash" ) == [ "f1", "f2", "f3" ]
  end

  test "hlen", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )

    assert Client.hlen( client, "i_dont_exist" ) == 0
    assert Client.hlen( client, "myhash" ) == 3 
  end

  test "hmget", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )

    assert Client.hmget( client, "i_dont_exist", [ "foo", "bar" ] ) == [nil, nil]
    assert Client.hmget( client, "myhash", [ "f1", "f5", "f3" ] ) == [ "v1", nil, "v3" ]
  end

  test "hmset", meta do
    client = meta[:handle]
    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )

    assert Client.hmset( client, "myhash", [ "f1", "x1", "f5", "x5" ] ) == "OK"
    assert RedisCli.run( "HGETALL myhash" ) == [ "f1", "x1", "f2", "v2", "f3", "v3", "f5", "x5" ]
  end

  # test "hscan", meta do
  # end

  test "hset", meta do
    client = meta[:handle]
    assert Client.hset( client, "newhash", "field", "value" ) == :insert
    assert Client.hset( client, "newhash", "field", "value2" ) == :update

    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hset( client, "myhash", "f2", "x2" ) == :update
    assert RedisCli.run( "HGETALL myhash" ) == [ "f1", "v1", "f2", "x2", "f3", "v3" ]
  end

  test "hsetnx", meta do
    client = meta[:handle]

    assert Client.hsetnx( client, "newhash", "field", "value" ) == true
    assert Client.hsetnx( client, "newhash", "field", "value2" ) == false

    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hsetnx( client, "myhash", "f2", "x2" ) == false
    assert RedisCli.run( "HGETALL myhash" ) == [ "f1", "v1", "f2", "v2", "f3", "v3" ]
  end

  test "hvals", meta do
    client = meta[:handle]

    assert Client.hvals( client, "i_dont_exist" ) == []

    RedisCli.run( "HMSET myhash f1 v1 f2 v2 f3 v3" )
    assert Client.hvals( client, "myhash" ) == [ "v1", "v2", "v3" ]
  end

  test "blpop", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist v1 v2" )
    RedisCli.run( "LPUSH mylist2 v3 v4" )

    assert Client.blpop( client, [ "mylist", "mylist2" ], 0 ) == [ "mylist", "v2" ]

    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "v1" ]
    assert RedisCli.run( "LRANGE mylist2 0 -1" ) == [ "v4", "v3" ]
    #TODO: Test timeout
  end

  test "brpop", meta do
    client = meta[:handle]

    RedisCli.run( "LPUSH mylist v1 v2" )
    RedisCli.run( "LPUSH mylist2 v3 v4" )

    assert Client.brpop( client, [ "mylist", "mylist2" ], 0 ) == [ "mylist", "v1" ]

    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "v2" ]
    assert RedisCli.run( "LRANGE mylist2 0 -1" ) == [ "v4", "v3" ]
    #TODO: Test timeout
  end

  test "brpoplpush", meta do
    client = meta[:handle]

    RedisCli.run( "LPUSH mylist v1 v2" )
    RedisCli.run( "LPUSH mylist2 v3 v4" )

    assert Client.brpoplpush( client, "mylist", "mylist2", 0 ) == "v1"

    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "v2" ]
    assert RedisCli.run( "LRANGE mylist2 0 -1" ) == [ "v1", "v4", "v3" ]

    #TODO: Test timeout
  end

  test "lindex", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist v1 v2" )
    assert Client.lindex( client, "mylist2", 0 ) == nil
    assert Client.lindex( client, "mylist", 0 ) == "v2"
    assert Client.lindex( client, "mylist", 1 ) == "v1"
    assert Client.lindex( client, "mylist", -1 ) == "v1"
    assert Client.lindex( client, "mylist", -2 ) == "v2"
    assert Client.lindex( client, "mylist", 3 ) == nil
  end

  test "linsert", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist v1 v2" )

    assert Client.linsert( client, "mylist", :before, "v1", "v0" ) == 3
    assert Client.linsert( client, "mylist", :after, "v2", "v3" ) == 4
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "v2", "v3", "v0", "v1" ]
  end

  test "llen", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist v1 v2" )
    
    assert Client.llen( client, "i_dont_exist" ) == 0
    assert Client.llen( client, "mylist" ) == 2
  end

  test "lpop", meta do
    client = meta[:handle]
    
    RedisCli.run( "LPUSH mylist v1 v2" )
    assert Client.lpop( client, "mylist" ) == "v2"
    assert Client.lpop( client, "mylist" ) == "v1"
    assert Client.lpop( client, "mylist" ) == nil
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == []
  end

  test "lpush", meta do
    client = meta[:handle]
    
    assert Client.lpush( client, "mylist", [ "v1" ] ) == 1
    assert Client.lpush( client, "mylist", [ "v2" ] ) == 2
    assert Client.lpush( client, "mylist", [ "v3", "v4"] ) == 4
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "v4", "v3", "v2", "v1" ]
  end

  test "lpushx", meta do
    client = meta[:handle]

    assert Client.lpushx( client, "i_dont_exist", "v1" ) == 0
    assert RedisCli.run( "LRANGE i_dont_exist 0 -1" ) == []

    assert Client.lpush( client, "mylist", [ "v1" ] ) == 1
    assert Client.lpushx( client, "mylist", "v2" ) == 2
    assert Client.lpushx( client, "mylist", "v3" ) == 3
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "v3", "v2", "v1" ]
  end

  test "lrange", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist v4 v3 v2 v1" )

    assert Client.lrange( client, "mylist", 0, 0 ) == [ "v1" ]
    assert Client.lrange( client, "mylist", 1, 2 ) == [ "v2", "v3" ]
    assert Client.lrange( client, "mylist", 0, -1 ) == [ "v1", "v2", "v3", "v4" ]
  end

  test "lrem", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist x o x o x o" )
    assert Client.lrem( client, "mylist", 3, "o" ) == 3
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "x", "x", "x" ]
  end

  test "lset", meta do
    client = meta[:handle]

    RedisCli.run( "LPUSH mylist 4 3 2 1" )
    assert Client.lset( client, "mylist", 2, "x" ) == "OK"
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "1", "2", "x", "4" ]
  end

  test "ltrim", meta do
    client = meta[:handle]

    RedisCli.run( "LPUSH mylist 4 3 2 1" )
    assert Client.ltrim( client, "mylist", 1, 2 ) == "OK"
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "2", "3" ]
  end

  test "rpop", meta do
    client = meta[:handle]

    RedisCli.run( "LPUSH mylist 4 3 2 1" )

    assert Client.rpop( client, "mylist" ) == "4"
    assert Client.rpop( client, "mylist" ) == "3"
    assert Client.rpop( client, "mylist" ) == "2"
    assert Client.rpop( client, "mylist" ) == "1"
    assert Client.rpop( client, "mylist" ) == nil
  end

  test "rpoplpush", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist 4 3 2 1" )
    RedisCli.run( "LPUSH mylist2 d c b a" )
    
    assert Client.rpoplpush( client, "mylist", "mylist2" ) == "4"
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "1", "2", "3" ]
    assert RedisCli.run( "LRANGE mylist2 0 -1" ) == [ "4", "a", "b", "c", "d" ]
  end

  test "rpush", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist 4 3 2 1" )

    assert Client.rpush( client, "mylist", [ "5" ] ) == 5
    assert Client.rpush( client, "mylist", [ "6", "7" ] ) == 7

    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "1", "2", "3", "4", "5", "6", "7" ]
  end

  test "prushx", meta do
    client = meta[:handle]
    RedisCli.run( "LPUSH mylist 4 3 2 1" )

    assert Client.rpushx( client, "i_dont_exist", "v1" ) == 0
    assert RedisCli.run( "LRANGE i_dont_exist 0 -1" ) == []

    assert Client.rpush( client, "mylist", [ "v1" ] ) == 5
    assert Client.rpushx( client, "mylist", "v2" ) == 6
    assert Client.rpushx( client, "mylist", "v3" ) == 7
    assert RedisCli.run( "LRANGE mylist 0 -1" ) == [ "1", "2", "3", "4", "v1", "v2", "v3" ]
  end

  test "sadd", meta do
    client = meta[:handle]
    assert Client.sadd( client, "myset", [ "foo", "bar" ] ) == 2
    assert Client.sadd( client, "myset", [ "foo", "baz" ] ) == 1

    assert RedisCli.run( "SMEMBERS myset" ) |> Enum.sort == [ "baz", "foo", "bar" ] |> Enum.sort
  end

  test "scard", meta do
    client = meta[:handle]

    RedisCli.run( "SADD myset foo bar baz" )
    assert Client.scard( client, "i_dont_exist" ) == 0
    assert Client.scard( client, "myset" ) == 3
  end

  test "sdiff", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set1 a b c" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sdiff( client, [ "set1", "set2" ] ) == [ "a" ]
  end

  test "sdiffstore", meta do
    client = meta[:handle]
    RedisCli.run( "SADD set1 a b c" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sdiffstore( client, "destset", [ "set1", "set2" ] ) == 1
    assert RedisCli.run( "SMEMBERS destset" ) == [ "a" ]
  end

  test "sinter", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set1 a b c" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sinter( client, [ "set1", "set2" ] ) == [ "c", "b" ]
  end

  test "sinterstore", meta do
    client = meta[:handle]
    RedisCli.run( "SADD set1 a b c" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sinterstore( client, "destset", [ "set1", "set2" ] ) == 2
    assert RedisCli.run( "SMEMBERS destset" ) |> Enum.sort == [ "c", "b" ] |> Enum.sort
  end

  test "sismember", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set b c" )
    assert Client.sismember( client, "i_dont_exist", "a" ) == false
    assert Client.sismember( client, "set", "a" ) == false
    assert Client.sismember( client, "set", "b" ) == true
  end

  test "smembers", meta do
    client = meta[:handle]
    RedisCli.run( "SADD set a b c" )
    assert Client.smembers( client, "set" ) |> Enum.sort == [ "c", "a", "b" ] |> Enum.sort
  end

  test "smove", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set1 a b c" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.smove( client, "set2", "set1", "a" ) == false
    assert Client.smove( client, "set1", "set2", "a" ) == true
  end

  test "spop", meta do
    client = meta[:handle]
    RedisCli.run( "SADD set a b c" )

    Client.spop( client, "set" )
    assert Client.scard( client, "set" ) == 2
    Client.spop( client, "set" )
    assert Client.scard( client, "set" ) == 1
    Client.spop( client, "set" )
    assert Client.spop( client, "set" ) == nil
  end

  # test "srandmember", meta do
  #   client = meta[:handle]
  # end

  test "srem", meta do
    client = meta[:handle]
    RedisCli.run( "SADD set a b c" )

    assert Client.srem( client, "set", [ "a", "c" ] ) == 2
    assert RedisCli.run( "SMEMBERS set" ) == [ "b" ]
  end

  test "sunion", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set1 a b d" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sunion( client, [ "set1", "set2" ] ) |> Enum.sort == Enum.sort( [ "c", "a", "b", "d" ] )
  end

  test "sunionstore", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set1 a b d" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sunionstore( client, "destset", [ "set1", "set2" ] ) == 4
    assert RedisCli.run( "SMEMBERS destset" ) |> Enum.sort == [ "a", "b", "c", "d" ]
  end

  # test "sscan", meta do
  #   client = meta[:handle]
  # end

  test "zadd", meta do
    client = meta[:handle]

    assert Client.zadd( client, "zset", [ 1, "a", 2, "b", 3.5, "c", "1e10", "d" ] ) == 4
    assert Client.zadd( client, "zset", [ 3, "a", 5, "x" ] ) == 1

    assert RedisCli.run( "ZRANGE zset 0 -1" ) == [ "b", "a", "c", "x", "d" ]
  end

  test "zcard", meta do
    client = meta[:handle]

    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )
    assert Client.zcard( client, "i_dont_exist" ) == 0
    assert Client.zcard( client, "zset" ) == 3
  end

  test "zcount", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )
    assert Client.zcount( client, "i_dont_exist", "-inf", "+inf" ) == 0
    assert Client.zcount( client, "zset", "-inf", "+inf" ) == 3
    assert Client.zcount( client, "zset", 0, 3 ) == 3
    assert Client.zcount( client, "zset", 1, 2 ) == 2
  end

  test "zincrby", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )
    assert Client.zincrby( client, "zset", 4, "a" ) == 5
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == [ "b", "c", "a" ]
  end

  test "zinterstore", meta do
    client = meta[:handle]
    
    RedisCli.run( "ZADD zset1 1 a 2 b 3 c" )
    RedisCli.run( "ZADD zset2 3 a 5 x 3 c" )

    assert Client.zinterstore( client, "simple", [ "zset1", "zset2" ] ) == 2
    assert RedisCli.run( "ZRANGE simple 0 -1 WITHSCORES" ) == [ "a", "4",
                                                                "c", "6" ]

    assert Client.zinterstore( client, "weighted", [ "zset1", "zset2" ], [ weights: [ 0.5, 2 ] ] ) == 2
    assert RedisCli.run( "ZRANGE weighted 0 -1 WITHSCORES" ) == [ "a", "6.5",
                                                                  "c", "7.5" ]

    assert Client.zinterstore( client, "aggregated", [ "zset1", "zset2" ], [ aggregate: :min ] ) == 2
    assert RedisCli.run( "ZRANGE aggregated 0 -1 WITHSCORES" ) == [ "a", "1",
                                                                  "c", "3" ]
  end

  test "zrange", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrange( client, "zset", 0, -1 ) == [ "a", "b", "c" ]
    assert Client.zrange( client, "zset", 1, 2 ) == [ "b", "c" ]

    assert Client.zrange( client, "zset", 0, -1, [ withscores: true ] ) == [ "a", 1.0,
                                                                             "b", 2.0,
                                                                             "c", 3.0 ]
  end

  test "zrangebyscore", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrangebyscore( client, "zset", "-inf", "+inf" ) == [ "a", "b", "c" ]
    assert Client.zrangebyscore( client, "zset", 0.0, 10.0 ) == [ "a", "b", "c" ]
    assert Client.zrangebyscore( client, "zset", 1.5, 3.0 ) == [ "b", "c" ]
    assert Client.zrangebyscore( client, "zset", "(1", "(3" ) == [ "b" ]

    assert Client.zrangebyscore( client, "zset", 1.5, 3.0, [ withscores: true ] ) == [ "b", 2.0,
          "c", 3.0 ]
    assert Client.zrangebyscore( client, "zset", 0.0, 10.0, [ limit: [ 1, 2 ] ] ) == [ "b", "c" ]
  end

  test "zrank", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrank( client, "i_dont_exist", "a" ) == nil
    assert Client.zrank( client, "zset", "a" ) == 0
    assert Client.zrank( client, "zset", "b" ) == 1
    assert Client.zrank( client, "zset", "c" ) == 2
    assert Client.zrank( client, "zset", "d" ) == nil
  end

  test "zrem", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrem( client, "zset", [ "a", "c" ] ) == 2
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == [ "b" ]
    assert Client.zrem( client, "zset", [ "b", "z" ] ) == 1
    assert RedisCli.run( "ZRANGE zset 0 -1" ) ==  []
  end

  test "zremrangebyrank", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zremrangebyrank( client, "zset", 2, -1 ) == 1
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == [ "a", "b" ]
    assert Client.zremrangebyrank( client, "zset", 0, 1 ) == 2
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == []
  end

  test "zremrangebyscore", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zremrangebyscore( client, "zset", "-inf", "+inf" ) == 3
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == []

    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )
    assert Client.zremrangebyscore( client, "zset", 0.0, 10.0 ) == 3
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == []

    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )
    assert Client.zremrangebyscore( client, "zset", 1.5, 3.0 ) == 2
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == [ "a" ]

    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )
    assert Client.zremrangebyscore( client, "zset", "(1", "(3" ) == 1
    assert RedisCli.run( "ZRANGE zset 0 -1" ) == [ "a", "c" ]
  end

  test "zrevrange", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrevrange( client, "zset", 0, -1 ) == [ "c", "b", "a" ]
    assert Client.zrevrange( client, "zset", 1, 2 ) == [ "b", "a" ]

    assert Client.zrevrange( client, "zset", 0, -1, [ withscores: true ] ) == [ "c", 3.0,
                                                                             "b", 2.0,
                                                                             "a", 1.0 ]
  end

  test "zrevrangebyscore", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrevrangebyscore( client, "zset", "+inf", "-inf" ) == [ "c", "b", "a" ]
    assert Client.zrevrangebyscore( client, "zset", 0.0, 10.0 ) == [ "c", "b", "a" ]
    assert Client.zrevrangebyscore( client, "zset", 1.5, 3.0 ) == [ "c", "b" ]
    assert Client.zrevrangebyscore( client, "zset", "(3", "(1" ) == [ "b" ]

    assert Client.zrevrangebyscore( client, "zset", 1.5, 3.0, [ withscores: true ] ) == [ "c", 3.0,
             "b", 2.0 ]
    assert Client.zrevrangebyscore( client, "zset", 0.0, 10.0, [ limit: [ 1, 2 ] ] ) == [ "b", "a" ]
  end

  test "zrevrank", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zrevrank( client, "i_dont_exist", "a" ) == nil
    assert Client.zrevrank( client, "zset", "a" ) == 2
    assert Client.zrevrank( client, "zset", "b" ) == 1
    assert Client.zrevrank( client, "zset", "c" ) == 0
    assert Client.zrevrank( client, "zset", "d" ) == nil
  end

  test "zscore", meta do
    client = meta[:handle]
    RedisCli.run( "ZADD zset 1 a 2 b 3 c" )

    assert Client.zscore( client, "zset", "a" ) == 1.0
    assert Client.zscore( client, "zset", "d" ) == nil
    assert Client.zscore( client, "i_dont_exist", "x" ) == nil
  end

  test "zunionstore", meta do
    client = meta[:handle]
    
    RedisCli.run( "ZADD zset1 1 a 2 b 3 c" )
    RedisCli.run( "ZADD zset2 3 a 5 x 3 c" )

    assert Client.zunionstore( client, "simple", [ "zset1", "zset2" ] ) == 4
    assert RedisCli.run( "ZRANGE simple 0 -1 WITHSCORES" ) == [ "b", "2",
                                                                "a", "4",
                                                                "x", "5",
                                                                "c", "6" ]

    assert Client.zunionstore( client, "weighted", [ "zset1", "zset2" ], [ weights: [ 0.5, 2 ] ] ) == 4
    assert RedisCli.run( "ZRANGE weighted 0 -1 WITHSCORES" ) == [ "b", "1",
                                                                  "a", "6.5",
                                                                  "c", "7.5",
                                                                  "x", "10" ]

    assert Client.zunionstore( client, "aggregated", [ "zset1", "zset2" ], [ aggregate: :min ] ) == 4
    assert RedisCli.run( "ZRANGE aggregated 0 -1 WITHSCORES" ) == [ "a", "1",
                                                                    "b", "2",
                                                                    "c", "3",
                                                                    "x", "5" ]
  end

  # test "zscan", meta do
  #   client = meta[:handle]
  # end

  test "eval", meta do
    client = meta[:handle]

    assert Client.eval( client, "return 'Hello World'", [], [] ) == "Hello World" 
  end

  test "evalsha", meta do
    client = meta[:handle]
    script = "return \'Hello World\'"
    [sha] = RedisCli.run( "SCRIPT LOAD \"#{script}\"" )
    assert Client.evalsha( client, sha, [], [] ) == "Hello World"
  end

  test "script_exists", meta do
    client = meta[:handle]
    assert Client.script_exists( client, [ "e0e1f9fabfc9d4800c877a703b823ac0578ff8db" ] ) == [ 0 ]

    script = "return \'Hello World\'"
    [sha] = RedisCli.run( "SCRIPT LOAD \"#{script}\"" )

    assert Client.script_exists( client, [ sha ] ) == [ 1 ]
  end

  test "script_flush", meta do
    client = meta[:handle]
    script = "return \'Hello World\'"
    [sha] = RedisCli.run( "SCRIPT LOAD \"#{script}\"" )
    assert Client.script_flush( client ) == "OK"
    assert RedisCli.run( "SCRIPT EXISTS #{sha}" ) == [ "0" ]
  end

  #TODO: How to test this? Open a second connection and kill the script, 
  # and verify that the first connection is responsive?
  # test "script_kill", meta do
  #   client = meta[:handle]
  # end

  test "script_load", meta do
    client = meta[:handle]
    script = "return \'Hello World\'"
    [sha] = RedisCli.run( "SCRIPT LOAD \"#{script}\"" )
    assert Client.script_load( client, script ) == sha
  end

  test "auth", meta do
    client = meta[:handle]
    assert Client.auth( client, "foo" ) == { :redis_error, "ERR Client sent AUTH, but no password is set" }
  end

  test "echo", meta do
    client = meta[:handle]
    assert Client.echo( client, "foo" ) == "foo"
  end

  test "ping", meta do
    client = meta[:handle]
    assert Client.ping( client ) == "PONG"
  end

  test "quit", meta do
    client = meta[:handle]
    [ "#", "Clients", client_info | _ ] = RedisCli.run( "INFO clients" )
    [ _, clients_count ] = String.split( client_info, ":" )
    before_clients_count = binary_to_integer(clients_count)

    assert Client.quit( client ) == "OK"

    [ "#", "Clients", client_info | _ ] = RedisCli.run( "INFO clients" )
    [ _, clients_count ] = String.split( client_info, ":" )
    after_clients_count = binary_to_integer(clients_count)

    assert after_clients_count == before_clients_count - 1
  end

  test "select", meta do
    client = meta[:handle]
    assert Client.select( client, 1 ) == "OK"
  end
  
  test "bgrewriteaof", meta do
    client = meta[:handle]
    result = Client.bgrewriteaof( client ) 
    assert result in [ "Background append only file rewriting started",
                       "Background append only file rewriting scheduled" ]
  end

  test "bgsave", meta do
    client = meta[:handle]
    assert Client.bgsave( client ) == "Background saving started"
  end

  test "client_getname and client_setname", meta do
    client = meta[:handle]
    assert Client.client_setname( client, "myclient" ) == "OK"
    assert Client.client_getname( client ) == "myclient"
  end

  test "client_list and client_kill", meta do
    client = meta[:handle]
    Client.client_setname( client, "killme" )
    client_list = Client.client_list( client ) |> String.split( "\n" )
    my_client_entry = Enum.find( client_list,
                                 fn( l ) -> Regex.match?( ~r/name=killme/, l ) end )
    [ addr | _ ] = String.split( my_client_entry )
    ["addr", ip_port] = String.split( addr, "=" )
   
    [ip, port] = String.split( ip_port, ":" )
    assert Client.client_kill( client, ip, port ) == "OK"
  end

  #NOTE: Only available for Redis 3.0 beta
  # test "client pause", meta do
  #   client = meta[:handle]
  #   assert Client.client_pause( client, 0 ) == "OK"
  #   assert Client.client_pause( client, 10 ) == "OK"
  # end

  # test "config_get", meta do
  #   #FIXME This is broken somehow; request for * gets mangled, the replies for 'save' are empty
  #   client = meta[:handle]
  #   config = RedisCli.run( "CONFIG GET save" )
  #   assert Client.config_get( client, "save" ) == config
  # end

  test "config_resetstat", meta do
    client = meta[:handle]
    assert Client.config_resetstat( client ) == "OK"
    results = RedisCli.run( "INFO" )
    assert Enum.find( results, fn(x) -> Regex.match?( ~r/keyspace_hits:0/, x ) end )
  end

  #FIXME
  # test "config_rewrite", meta do
  #   client = meta[:handle]
  #   assert Client.config_rewrite( client ) == "OK"
  # end

  test "config_set and config_rewrite", meta do
    client = meta[:handle]
    assert Client.config_set( client, "loglevel", "debug" ) == "OK"
  end

  test "dbsize", meta do
    client = meta[:handle]
    [ result ] = RedisCli.run( "DBSIZE" )
    assert Client.dbsize( client ) == binary_to_integer( result )
  end

  test "flushall", meta do
    client = meta[:handle]
    Enum.each( 0..31, fn (db) -> 
      RedisCli.run( "SELECT #{db}" )
      RedisCli.run( "SET foo bar" )
    end )
    assert Client.flushall( client ) == "OK"
    Enum.each( 0..31, fn (db) -> 
      RedisCli.run( "SELECT #{db}" )
      assert RedisCli.run( "DBSIZE" ) == [ "0" ]
    end )
  end

  test "flushdb", meta do
    client = meta[:handle]
    RedisCli.run( "SET foo bar" )
    assert Client.flushdb( client ) == "OK"
    assert RedisCli.run( "DBSIZE" ) == [ "0" ]
  end

  test "info", meta do
    client = meta[:handle]
    info = RedisCli.run( "INFO DEFAULT" )
    redis_version = Enum.find( info, fn(x) -> Regex.match?( ~r/redis_version/, x ) end )
    client_info = Client.info( client ) 
    client_version = String.split( client_info, "\r\n" ) 
                       |> Enum.find( fn(x) -> Regex.match?( ~r/redis_version/, x ) end )

    assert redis_version == client_version
  end

  test "lastsave", meta do
    client = meta[:handle]
    [result] = RedisCli.run( "LASTSAVE" ) 
    assert Client.lastsave( client ) == binary_to_integer( result )
  end

  test "save", meta do
    client = meta[:handle]
    assert Client.save( client ) == "OK"
  end

  test "slaveof", meta do
    client = meta[:handle]
    assert Client.slaveof( client, :noone ) == "OK"
  end

  test "slowlog", meta do
    client = meta[:handle]
    RedisCli.run( "SLOWLOG RESET" )
    RedisCli.run( "EVAL 'for var=1,1000000 do end' 0" )
    Client.script_kill( client ) 

    assert Client.slowlog( client, :len ) == 1
    [[id: id, start_time: start, runtime: run, command: cmd]] = Client.slowlog( client, :get, 1 )
    assert is_integer( id )
    assert is_integer( start )
    assert is_integer( run )
    assert is_list( cmd )
    assert Client.slowlog( client, :reset ) == "OK"
    assert Client.slowlog( client, :len ) == 0 
  end

  test "time", meta do
    client = meta[:handle]
    result = Client.time( client )
    assert is_list( result )
    assert length( result ) == 2
    [seconds, micros] = result
    assert is_integer( seconds )
    assert is_integer( micros )
    assert seconds > 0
  end

end
