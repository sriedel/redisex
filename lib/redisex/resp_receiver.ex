defmodule RedisEx.RespReceiver do
  @timeout 30_000

  def get_response( socket ) do
    :ok = :inet.setopts( socket, [ { :packet, :line } ] )
    { :ok, data } = :gen_tcp.recv( socket, 0, @timeout )
    decode( socket, data )
  end

  # Simple strings
  defp decode( _socket, <<?+, remainder::binary>> ) do
    remove_crlf( remainder )
  end

  # Integer
  defp decode( _socket, <<?:, remainder::binary>> ) do
    remove_crlf( remainder ) |> binary_to_integer
  end

  # Error messages
  defp decode( _socket, <<?-, remainder::binary>> ) do
    message = remove_crlf( remainder )
    { :redis_error, message }
  end

  # Bulk strings
  defp decode( socket, <<"$-1\r\n">> ), do: nil
  defp decode( socket, <<"$0\r\n\r\n">> ), do: ""
  defp decode( socket, <<?$, remainder::binary>> ) do
    payload_bytes = remove_crlf( remainder ) |> binary_to_integer

    :ok = :inet.setopts( socket, [ packet: :raw ] )
    # Fetch payload
    { :ok, data } = :gen_tcp.recv( socket, payload_bytes + 2, @timeout )
    :ok = :inet.setopts( socket, [ packet: :line ] )
    
    # remove the trailing \r\n
    <<payload::[bytes, size(payload_bytes)], ?\r, ?\n>> = data

    payload
  end

  defp decode( _socket, <<"*-1\r\n">> ), do: nil
  defp decode( _socket, <<"*0\r\n">> ), do: []
  defp decode( socket, <<?*, remainder::binary>> ) do
    payload_lines = remove_crlf( remainder ) |> binary_to_integer
    process_bulk_reply( socket, payload_lines, [] )
  end

  defp process_bulk_reply( _socket, 0, acc ), do: :lists.reverse( acc )
  defp process_bulk_reply( socket, lines_remaining, acc ) do
    { :ok, data } = :gen_tcp.recv( socket, 0, @timeout )
    decoded = decode( socket, data )
    process_bulk_reply( socket, lines_remaining - 1, [ decoded | acc ] )
  end

  defp remove_crlf( data ) when is_binary( data ) do
    :binary.part( data, 0, byte_size( data ) - 2 )
  end
end
