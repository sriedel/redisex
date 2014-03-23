ExUnit.start


defmodule RedisCli do
  @rediscli 'redis-cli -p 6333 '

  def run( command ) when is_binary( command ) do
    {:ok, command_list} = String.to_char_list( command )
    run( command_list )
  end

  def run( command ) when is_list( command ) do
    cmd = :lists.concat( [ @rediscli, command, '\n' ] )
    output = :os.cmd( cmd )
    { :ok, result } = String.from_char_list( output )
    String.split( result )
  end
end
