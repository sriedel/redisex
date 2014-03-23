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

  test "expireat", meta do
  end

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

  test "migrate", meta do
  end

  test "move", meta do
  end

  test "object", meta do
  end

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

  test "pexpireat", meta do
  end

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

  test "scan", meta do
  end

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

  test "bitpos", meta do
#  #TODO: Re-enable after 2.8.7
#     client = meta[:handle]
# 
#     RedisCli.run "SET foo 0"
#     assert Client.bitpos( client, "foo", 0 ) == 0
#     assert Client.bitpos( client, "foo", 0, 2, 4 ) == 4
#     assert Client.bitpos( client, "foo", 0, 2, 3 ) == -1
  end

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
    RedisCli.run( "GET bar" ) == [ "baz" ]
    RedisCli.run( "GET foo" ) == [ "quux" ]
    RedisCli.run( "GET bla" ) == [ "blubb" ]
  end

  test "msetnx", meta do
    client = meta[:handle]

    RedisCli.run( "SET foo bar" )
    assert Client.msetnx( client, [ "bar", "baz", "foo", "quux", "bla", "blubb" ] ) == false

    RedisCli.run( "DEL foo" )
    assert Client.mset( client, [ "bar", "baz", "foo", "quux", "bla", "blubb" ] ) == "OK"
    RedisCli.run( "GET bar" ) == [ "baz" ]
    RedisCli.run( "GET foo" ) == [ "quux" ]
    RedisCli.run( "GET bla" ) == [ "blubb" ]
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
end
