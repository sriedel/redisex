defmodule RedisEx.Client do
  #TODO: Unit test
  alias RedisEx.Connection

  defrecord Client, socket: nil

  def connect( hostname, port ) when is_binary( hostname )
                                 and is_binary( port ) do
    connect( hostname, binary_to_integer( port ) )
  end

  def connect( hostname, port ) when is_binary( hostname )
                                 and is_integer( port ) do
    Client.new( socket: Connection.connect( hostname, port ) )
  end

  def disconnect( client ) do
    Connection.disconnect( client.socket )
  end

  def set( client, key, value ) when is_binary( key ) and is_binary( value ) do
    command_list = [ "SET", key, value ] 
    process_command( client, command_list )
  end

  def get( client, key ) when is_binary( key ) do
    command_list = [ "GET", key ]
    process_command( client, command_list )
  end

  defp process_command( client, command_list ) do
    Connection.send_command( client.socket, command_list )
    Connection.get_response( client.socket ) 
  end
end
