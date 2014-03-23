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
end
