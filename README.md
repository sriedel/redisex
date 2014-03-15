# RedisEx

A simple Redis connector for Elixir

currently only the SET and GET commands are implemented

## Usage
```
iex(1)>   client = RedisEx.Client.connect( "127.0.0.1", 6379 )
RedisEx.Client.Client[socket: #Port<0.3393>]
iex(2)>  :gen_tcp.recv( client.socket, 0, 30000 )
{:error, :timeout}
iex(3)> RedisEx.Client.set( client, "foo", "bar" )
{:ok, "OK"}
iex(4)> RedisEx.Client.get( client, "foo" )
{:ok, "bar"}
```

## TODOs
- unit test client and connection
- make connection handling an OTP server with supervisor
- add a connection pool
- Implement ALL the commands!
- Add an OTP Server for pub/sub operations
