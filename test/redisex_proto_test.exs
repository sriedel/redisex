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
end

