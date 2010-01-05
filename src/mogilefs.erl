-module(mogilefs).

-compile(export_all).

create_domain(Domain) ->
	do_request("create_domain", [{domain, Domain}]).

check_domain(Domain) ->
	case do_request("list_keys", [{domain, Domain}, {limit, 1}]) of
		{ok, _} ->  % bucket with keys
			ok;
		{err, none_match, _} -> % bucket with no keys
			ok;
		_ ->
			not_found
		end.

new_file(Domain, Key, Content) ->
	do_request("create_open", [{domain, Domain}, {key, Key}]).


% sock() -> tcp socket to communicate to mogilefs tracker
sock() ->
    {ok, Sock} = gen_tcp:connect("10.0.0.11", 6001, 
                                 [list, {active, false}, {packet, 0}]),
	Sock.

% @spec do_request(string() | binary(), Options) -> {ok, Objects} | {err, Code, Message}
% @doc  perform a request against new socket to mogilefs tracker, then parse results returning to caller
% FIXME: should recv data until it hits \r\n
do_request(Method, Options) ->
	Sock = sock(),
	ok = gen_tcp:send(Sock, mog_encode(Method, Options)),
	{ok, Data} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock),
	mog_decode(Data).

% @spec mog_encode(string() | binary(), Options) -> string()
% @doc  encode a command and options dictionary into a string to be sent over the wire
mog_encode(Method, Options) ->
	lists:flatten([Method, " ", mochiweb_util:urlencode(Options), "\r\n"]).


% @spec mog_decode(string()) -> {ok, Dictionary} | {err, code, message}
% @doc  decode encoded response from a mogilefs request into an object
mog_decode("OK " ++ String) ->
	mog_decode(ok, String);

mog_decode("ERR " ++ String) ->
	mog_decode(err, String).

mog_decode(err, String) ->
	{match, [{Split, 1}]} = regexp:matches(String, " "),
    Code = string:substr(String, 1, Split-1),
    Reason = mochiweb_util:unquote(string:substr(String, Split+1, string:len(String) - Split - 2)),
	{err, Code, Reason};

mog_decode(ok, String) ->
	{ok, mochiweb_util:parse_qs(String)}.