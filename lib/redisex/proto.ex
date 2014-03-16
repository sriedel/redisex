defmodule RedisEx.Proto do
  @moduledoc """
    This module supplies methods to convert to and from the redis "unified"
    protocol. 
    
    See http://www.redis.io/topics/protocol for protocol details.
  """

  @doc """
  Converts a list into the redis unified multi bulk format. It is assumed that
  the list elements are command strings and arguments (strings or binaries), 
  so that when the resulting unified message string is sent to redis, 
  it will know what to do with it.
  """
  def to_proto( [] ), do: ""
  def to_proto( command_list ) when is_list( command_list ) do
    _to_proto( command_list, "*#{length( command_list )}\r\n" )
  end

  defp _to_proto( [], acc ), do: acc
  defp _to_proto( [command|rest], acc ) do
    _to_proto( rest, acc <> to_argument( command ) )
  end

  def to_argument(""), do: ""
  def to_argument( arg ) do
    "$#{byte_size( arg )}\r\n#{arg}\r\n"
  end
end
