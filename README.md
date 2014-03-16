# RedisEx

A simple Redis connector for Elixir

currently only the SET and GET commands are implemented

## Usage
```
iex(1)>   client = RedisEx.Client.connect( "127.0.0.1", 6379 )
RedisEx.Client.Client[socket: #Port<0.3393>]
iex(2)>   RedisEx.Client.set( client, "foo", "bar" )
{:ok, "OK"}
iex(3)>   RedisEx.Client.get( client, "foo" )
{:ok, "bar"}
```

## TODOs
- unit test client 
- Implement ALL the commands!
- Add an OTP Server for pub/sub operations
- add a connection pool
