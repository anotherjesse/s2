-module(mogilefs).

-compile(export_all).

-define(SERVER, global:whereis_name(?MODULE)).

test() ->
	do_request("stats", "all=1").

% 
% sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
% sock.setblocking(0)
% sock.connect_ex(Ip, Port)
% 
sock() ->
    {ok, Sock} = gen_tcp:connect("10.0.0.11", 6001, 
                                 [list, {active, false}, {packet, 0}]),
	Sock.

create_domain(Domain) ->
	"OK" ++ _ = do_request("create_domain", io_lib:format("domain=~s", [Domain])),
	ok.
	
check_domain(Domain) ->
	case do_request("list_keys", "domain=" ++ Domain ++ "&limit=1") of
		"OK" ++ _ ->  % bucket with keys
			true;
		"ERR none_match" ++ _ -> % bucket with no keys
			 true;
		_ ->
			not_found
		end.

new_file(Domain, Key, Content) ->
	do_request("create_open", "domain=" ++ Domain ++ "&key=" ++ Key).

% argstr = self._encode_url_string(args); 
% sock.sendall(req, self.FLAG_NOSIGNAL)
do_request(Method, Options) ->
	Sock = sock(),
	io:format("sending: ~p~n", [encode(Method, Options)]),
	ok = gen_tcp:send(Sock, encode(Method, Options)),
	{ok, Data} = gen_tcp:recv(Sock, 0),
	io:format("Recv: ~p~n", [Data]),
    ok = gen_tcp:close(Sock),
	Data.    

% req = "%s %s\r\n" % (cmd, argstr)
encode(Method, Options) ->
	lists:flatten([Method, " ", Options, "\r\n"]).

    % def _encode_url_string(self, args):
    %     return "&".join(
    %         ["%s=%s" % (
    %                     quote_plus(str(k)),
    %                     quote_plus(str(v))
    %                    ) 
    %         for k,v in args.items() if v]
    %     )

