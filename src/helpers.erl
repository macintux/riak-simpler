-module(helpers).
-compile(export_all).

raw_get(Riak, Bucket, Key) ->
    {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
    Obj.

get(Riak, Bucket, Key) ->
    {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
    lists:map(fun(X) -> binary_to_list(X) end, riakc_obj:get_values(Obj)).

update(Riak, Bucket, Key, Value) ->
    {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
    update(Riak, Obj, Value).

update(Riak, Object, Value) ->
    helpers:put(Riak,
                riakc_obj:set_vclock(
                  replace(Object, Value),
                  riakc_obj:vclock(Object))).

l2b(String) ->
    unicode:characters_to_binary(String).

replace(Object, Value) ->
    new(riakc_obj:bucket(Object), riakc_obj:key(Object), l2b(Value)).

new(Bucket, Key, Value) when is_list(Bucket) ->
    riakc_obj:new(l2b(Bucket), l2b(Key), l2b(Value));
new(Bucket, Key, Value) ->
    riakc_obj:new(Bucket, Key, Value).

put(Riak, Bucket, Key, Value) ->
    {ok, Obj} = new(Bucket, Key, Value),
    riakc_pb_socket:put(Riak, Obj).

put(Riak, Object) ->
    riakc_pb_socket:put(Riak, Object).
