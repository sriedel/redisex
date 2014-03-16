defmodule RedisEx.ConnectionSupervisor do
  use Supervisor.Behaviour

  def start_link() do
    :supervisor.start_link( __MODULE__, [] )
  end

  def start_link( Args ) do
    :supervisor.start_link( __MODULE__, Args )
  end

  def init( Args ) do
    child_definitions = [ worker( RedisEx.Connection, Args, [ restart: :transient ] ) ]
    supervise( child_definitions, [ strategy: :simple_one_for_one ] )
  end
end
