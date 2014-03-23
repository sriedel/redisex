defmodule RedisCli do
  @rediscli 'redis-cli -p 6333 '

  def run( command ) when is_binary( command ) do
    {:ok, command_list} = String.to_char_list( command )
    run( command_list )
  end

  def run( command ) when is_list( command ) do
    cmd = [ @rediscli | command ] |> List.flatten
    output = :os.cmd( cmd <> "\n" )
    { :ok, result } = String.from_char_list( output )
    String.split( result )
  end
end
