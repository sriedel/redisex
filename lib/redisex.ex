defmodule RedisEx do
  def connect do
    { :ok, conn } = :gen_tcp.connect( 'localhost', 6379, [ :inet, { :nodelay, true }, { :packet, :raw } ] )
  end

  def set( conn ) do
    :ok = :gen_tcp.send( conn, "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n" )
  end
end
