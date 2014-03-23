defmodule RedisExClientTest do
  use ExUnit.Case, async: false
  alias RedisEx.Client

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
    assert Client.keys( client, "*" ) == [ "baz", "foo" ]
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
    assert Client.sort( client, "tosort", [ alpha: true, by: "w_*" ] ) == [ "C", "B", "A", "1", "3", "2" ] 

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

    assert RedisCli.run( "SMEMBERS myset" ) == [ "baz", "foo", "bar" ]
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
    RedisCli.run( "SMEMBERS destset" ) == [ "a" ]
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
    RedisCli.run( "SMEMBERS destset" ) == [ "c", "b" ]
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
    assert Client.smembers( client, "set" ) == [ "c", "a", "b" ]
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

    assert Client.sunion( client, [ "set1", "set2" ] ) == [ "c", "a", "b", "d" ]
  end

  test "sunionstore", meta do
    client = meta[:handle]

    RedisCli.run( "SADD set1 a b d" )
    RedisCli.run( "SADD set2 b c" )

    assert Client.sunionstore( client, "destset", [ "set1", "set2" ] ) == 4
    RedisCli.run( "SMEMBERS destset" ) == [ "a", "b", "c", "d" ]
  end

  # test "sscan", meta do
  #   client = meta[:handle]
  # end

  test "zadd", meta do
    client = meta[:handle]
  end

  test "zcard", meta do
    client = meta[:handle]
  end

  test "zcount", meta do
    client = meta[:handle]
  end

  test "zincrby", meta do
    client = meta[:handle]
  end

  test "zinterstore", meta do
    client = meta[:handle]
  end

  test "zrange", meta do
    client = meta[:handle]
  end

  test "zrangebyscore", meta do
    client = meta[:handle]
  end

  test "zrank", meta do
    client = meta[:handle]
  end

  test "zrem", meta do
    client = meta[:handle]
  end

  test "zremrangebyrank", meta do
    client = meta[:handle]
  end

  test "zremrangebyscore", meta do
    client = meta[:handle]
  end

  test "zrevrange", meta do
    client = meta[:handle]
  end

  test "zrevrangebyscore", meta do
    client = meta[:handle]
  end

  test "zrevrank", meta do
    client = meta[:handle]
  end

  test "zscore", meta do
    client = meta[:handle]
  end

  test "zunionstore", meta do
    client = meta[:handle]
  end

  test "zscan", meta do
    client = meta[:handle]
  end


end
