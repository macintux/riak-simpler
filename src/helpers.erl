-module(helpers).
-export([get/1, update/1, put/1]).
-compile({no_auto_import,[put/2]}).

get(Riak) ->
    fun(Bucket, Key, raw) ->
            {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
            Obj;
       (Bucket, Key, value) ->
            {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
            lists:map(fun(X) -> unicode:characters_to_list(X) end, riakc_obj:get_values(Obj))
    end.

update(Riak) ->
    fun(Bucket, Key, Value) ->
            {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
            update(Riak, Obj, Value)
    end.

%% Create a new object with the bucket, key, and vector clock of the
%% original. Put a new value in it and put it back into Riak.
update(Riak, Object, Value) ->
    put(Riak,
        riakc_obj:set_vclock(
          replace(Object, Value),
          riakc_obj:vclock(Object))).

replace(Object, Value) ->
    new(riakc_obj:bucket(Object), riakc_obj:key(Object), l2b(Value)).

new(Bucket, Key, Value) when is_list(Bucket) ->
    riakc_obj:new(l2b(Bucket), l2b(Key), l2b(Value));
new(Bucket, Key, Value) ->
    riakc_obj:new(Bucket, Key, Value).

put(Riak) ->
    fun(Bucket, Key, Value) ->
            Obj = new(Bucket, Key, Value),
            put(Riak, Obj)
    end.

put(Riak, Object) ->
    riakc_pb_socket:put(Riak, Object).

%%
%% Convert strings to binary, but also handle bucket types
l2b({Type, Bucket}) ->
    {unicode:characters_to_binary(Type), unicode:characters_to_binary(Bucket)};
l2b(String) ->
    unicode:characters_to_binary(String).
