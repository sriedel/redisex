Code.require_file "test_helper.exs", __DIR__

defmodule RedisExProtoTest do
  use ExUnit.Case
  import RedisEx.Proto

  test "to_proto" do
    assert to_proto([]) == ""
    assert to_proto( [ "foo" ] ) == "*1\r\n$3\r\nfoo\r\n"
    assert to_proto( [ "SET", "mykey", "myvalue" ] ) == "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
  end

  test "to_argument" do
    assert to_argument( "" ) == ""
    assert to_argument( "foo" ) == "$3\r\nfoo\r\n"
  end

  test "from_proto reading a status message" do
    assert from_proto( "+OK\r\n" ) == { :ok, "OK" }
  end

  test "from_proto reading an error message" do
    assert from_proto( "-ERR Something happened\r\n" ) == { :error, { :err, "Something happened" } }
    assert from_proto( "-WRONGTYPE\r\n" ) == { :error, { :wrongtype, "" } }
  end

  test "from_proto reading an integer message" do
    assert from_proto( ":0\r\n" ) == { :ok, 0 }
    assert from_proto( ":1000\r\n" ) == { :ok, 1000 }
  end

  test "from_proto reading a bulk message" do
    assert from_proto( "$-1\r\n" ) == { :ok, nil }
    assert from_proto( "$6\r\nfoobar\r\n" ) == { :ok, "foobar" }
  end

  test "from_proto reading a multi response" do
    assert from_proto( "*-1\r\n" ) == { :ok, nil }
    assert from_proto( "*0\r\n" ) == { :ok, [] }
    assert from_proto( "*1\r\n:1\r\n" ) == { :ok, [ 1 ] }
    assert from_proto( "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n:5\r\n" ) == { :ok, [1,2,3,4,5] }
    assert from_proto( "*2\r\n$3\r\nfoo\r\n$4\r\nquux\r\n" ) == { :ok, [ "foo", "quux" ] }
    assert from_proto( "*3\r\n:4\r\n$3\r\nfoo\r\n+OK\r\n" ) == { :ok, [ 4, "foo", "OK" ] }
  end
end

