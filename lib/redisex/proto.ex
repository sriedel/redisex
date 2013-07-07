defmodule RedisEx.Proto do
  def to_proto( [] ), do: ""
  def to_proto( command_list ) when is_list( command_list ) do
    _to_proto( command_list, "*" <> to_binary( length( command_list ) ) <> "\r\n" )
  end

  defp _to_proto( [], acc ), do: acc
  defp _to_proto( [command|rest], acc ) do
    _to_proto( rest, acc <> to_argument( command ) )
  end

  def to_argument(""), do: ""
  def to_argument( arg ) do
    "$" <> to_binary( byte_size( arg ) ) <> "\r\n" <> arg <> "\r\n"
  end

  def from_proto( message ) do
    { token, rest } = next_token( message )
    { payload, rest } = extract_payload( token, rest )
    construct_reply( payload )
  end

  def construct_reply( payload ) when is_tuple( payload ), do: { :error, payload }
  def construct_reply( payload ), do: { :ok, payload }

  def extract_payload( <<?-, remainder::binary>>, message_rest ) do
    error_contents = String.split( remainder, %r{\s+}, global: false )
    { payload, rest } = extract_error_payload( error_contents, message_rest )
  end

  def extract_payload( <<?+, remainder::binary>>, message_rest ) do
    { remainder, message_rest }
  end

  def extract_payload( <<?:, remainder::binary>>, message_rest ) do
    { binary_to_integer( remainder ), message_rest }
  end

  def extract_payload( <<?$, remainder::binary>>, message_rest ) do
    payload_bytesize = binary_to_integer( remainder )
    extract_bulk_payload( payload_bytesize, message_rest )
  end

  def extract_payload( <<?*, remainder::binary>>, message_rest ) do
    payload_count = binary_to_integer( remainder )
    extract_multi_payloads( payload_count, message_rest, [] )
  end

  defp extract_error_payload( [ error_type ], message_rest ) do
    { { build_error_type_atom( error_type ), "" }, message_rest }
  end

  defp extract_error_payload( [ error_type | error_message ], message_rest ) do
    { { build_error_type_atom( error_type ), hd( error_message ) }, message_rest }
  end

  defp build_error_type_atom( error_type ) do
    String.downcase( error_type ) |> binary_to_atom
  end

  defp extract_multi_payloads( -1, message_rest, acc ), do: { nil, message_rest }
  defp extract_multi_payloads(  0, message_rest, acc ), do: { Enum.reverse( acc ), message_rest }
  defp extract_multi_payloads( payload_count, message_rest, acc ) do
    { token, rest } = next_token( message_rest )
    { payload, rest } = extract_payload( token, rest )
    extract_multi_payloads( payload_count - 1, rest, [ payload | acc ] )
  end

  defp extract_bulk_payload( -1, message_rest ), do: { nil, message_rest }
  defp extract_bulk_payload( size_in_bytes, message_rest ) do
    <<payload::[bytes, size(size_in_bytes)], ?\r, ?\n, rest::binary>> = message_rest
    { payload, rest }
  end

  defp next_token( message ) do
    _next_token( message, "" )
  end

  defp _next_token( "", acc ), do: { acc, "" }
  defp _next_token( <<?\r, ?\n, rest::binary>>, acc ), do: { acc, rest }
  defp _next_token( <<character::[ size(1), bytes ], rest::binary>>, acc ) do
    _next_token( rest, acc <> character )
  end

end
