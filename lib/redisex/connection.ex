defmodule RedisEx.Connection do
  #TODO: unit test
  #TODO: This should be an OTP server!
  import RedisEx.Proto

  @moduledoc """
  Handles the tcp connection to the redis server.
  """

  def connect( host, port ) when is_binary( host ) 
                             and is_integer( port ) do
    { :ok, host_list } = String.to_char_list( host )
    connect( host_list, port )
  end

  def connect( host, port ) when is_list( host ) and 
                                 is_integer( port ) do
    { :ok, socket } = :gen_tcp.connect( host, 
                                        port,
                                        [ :binary, 
                                          { :packet, 0 },
                                          { :active, false } ] )
    socket
  end

  def disconnect( sock ) do
    :ok = :gen_tcp.close( sock )
  end

  def send_command( socket, command_list ) when is_list( command_list ) do
    send_data( socket, to_proto( command_list ) )
  end

  defp send_data( socket, data ) do
    :ok = :gen_tcp.send( socket, data )
  end

  def get_response( socket ) do
    { :ok, data } = receive_data( socket )
    from_proto( data )
  end

  defp receive_data( socket ) do
    #TODO: line protocol; move *<n> handling here
    #TODO: Timeout handling
    :gen_tcp.recv( socket, 0, 30000 )
  end


end
