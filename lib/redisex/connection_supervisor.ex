defmodule RedisEx.ConnectionSupervisor do
  use Supervisor.Behaviour

  @process_name :redisex_connection_supervisor

  def start_link() do
    start_link( [] )
  end

  def start_link( args ) do
    :supervisor.start_link( { :local, @process_name }, __MODULE__, args )
  end

  def add_connection( args ) when is_list( args ) do
    { :ok, server_pid } = :supervisor.start_child( @process_name, [args] )
    server_pid
  end

  def remove_connection( server_pid ) do
    :ok = :supervisor.terminate_child( @process_name, server_pid )
  end

  def init( args ) do
    child_definitions = [ worker( RedisEx.Connection, args, [ restart: :transient ] ) ]
    supervise( child_definitions, [ strategy: :simple_one_for_one ] )
  end
end
