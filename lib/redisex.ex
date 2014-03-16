defmodule RedisEx do
  use Application.Behaviour

  def start( _type, args ) do
    RedisEx.ConnectionSupervisor.start_link( args )
  end
end
