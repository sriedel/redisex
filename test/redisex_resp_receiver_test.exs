defmodule RedisExRespReceiverTest do
  use ExUnit.Case, async: true
  import RedisEx.RespReceiver

  defp create_socketpair do
    { :ok, listening_socket } = :gen_tcp.listen( 0, [ :binary,
                                                      nodelay: true,
                                                      keepalive: true,
                                                      active: false,
                                                      packet: :raw ] )
    { :ok, port } = :inet.port( listening_socket )
    { :ok, client_socket } = :gen_tcp.connect( '127.0.0.1', port, [ :binary,
                                                                    nodelay: true,
                                                                    keepalive: true,
                                                                    active: false,
                                                                    packet: :line ] )
    { :ok, accepted_socket } = :gen_tcp.accept( listening_socket )
    :gen_tcp.close( listening_socket )
    { accepted_socket, client_socket }
  end

  test "socket_pair" do
    sent_data = "foo\r\n"
    { s1, s2 } = create_socketpair
    :ok = :gen_tcp.send( s1, sent_data )
    { :ok, received_data } = :gen_tcp.recv( s2, 0 )
    :gen_tcp.close( s1 )
    :gen_tcp.close( s2 )
    assert received_data == sent_data
  end

  test "simple string decoding" do
    sent_data = "+OK\r\n"
    { s1, s2 } = create_socketpair
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    :gen_tcp.close( s1 )
    :gen_tcp.close( s2 )
    assert received_data == "OK"
  end

  test "integer decoding" do
    sent_data = ":100\r\n"
    { s1, s2 } = create_socketpair
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == 100

    sent_data = ":-1\r\n" 
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == -1
    :gen_tcp.close( s1 )
    :gen_tcp.close( s2 )
  end

  test "error decoding" do
    sent_data = "-WARNING\r\n"
    { s1, s2 } = create_socketpair
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    :gen_tcp.close( s1 )
    :gen_tcp.close( s2 )
    assert received_data == { :redis_error, "WARNING" }
  end

  test "bulk string decoding" do
    sent_data = "$-1\r\n"
    { s1, s2 } = create_socketpair
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == nil

    sent_data = "$0\r\n\r\n" 
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == ""

    sent_data = "$3\r\nfoo\r\n" 
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == "foo"

    :gen_tcp.close( s1 )
    :gen_tcp.close( s2 )
  end

  test "array decoding" do
    sent_data = "*-1\r\n"
    { s1, s2 } = create_socketpair
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == nil

    sent_data = "*0\r\n" 
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == []

    sent_data = "*2\r\n+foo\r\n+bar\r\n" 
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == [ "foo", "bar" ]

    sent_data = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == [ "foo", "bar" ]

    sent_data = "*3\r\n:1\r\n:2\r\n:3\r\n"
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == [ 1,2,3 ]

    sent_data = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == [ 1,2,3,4,"foobar" ]

    sent_data = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
    :ok = :gen_tcp.send( s1, sent_data )
    received_data = get_response( s2 )
    assert received_data == [ [ 1, 2, 3 ], [ "Foo", { :redis_error, "Bar" } ] ]


    :gen_tcp.close( s1 )
    :gen_tcp.close( s2 )
  end
end
