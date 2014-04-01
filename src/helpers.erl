-module(helpers).
-export([get/1, update/1, put/1, mult/3, add/0, del/0, new_set/0, new_set/1]).
-compile({no_auto_import,[put/2]}).


new_set() ->
    riakc_set:new().

new_set(List) ->
    lists:foldl(fun(X, Acc) ->
                       riakc_set:add_element(l2b(X), Acc)
               end,
               riakc_set:new(),
               List).

%% Set = riakc_set:new().

%% SetBucket = {<<"sets">>, <<"food!">>}.

%% Set2 = riakc_set:add_element(<<"eggs">>, Set).

%% Set3 = riakc_set:add_element(<<"bacon">>, Set2).

%% riakc_set:to_op(Set3).

%% riakc_pb_socket:update_type(Pid, SetBucket, <<"breakfast">>, riakc_set:to_op(Set3), [return_body]).

add() ->
    fun(Set, Value) ->
            riakc_set:add_element(l2b(Value), Set)
    end.

del() ->
    fun(Set, Value) ->
            riakc_set:del_element(l2b(Value), Set)
    end.

mult(Riak, Bucket, Bool) ->
    riakc_pb_socket:set_bucket(Riak, l2b(Bucket), [{allow_mult, Bool}]).

%% Need a way to fetch sets distinctly from objects
%% riakc_pb_socket:fetch_type(Pid, SetBucket, <<"breakfast">>).

get(Riak) ->
    fun(Bucket, Key, raw) ->
            {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
            Obj;
       (Bucket, Key, value) ->
            {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
            lists:map(fun(X) -> unicode:characters_to_list(X) end, riakc_obj:get_values(Obj))
    end.

update(Riak) ->
    fun(Bucket, Key, Value) when is_list(Value) ->
            {ok, Obj} = riakc_pb_socket:get(Riak, l2b(Bucket), l2b(Key)),
            update(Riak, Obj, Value);
       (Bucket, Key, DataType) ->
            riakc_pb_socket:update_type(Riak, l2b(Bucket), l2b(Key), to_op(DataType))
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
    fun(Bucket, Key, Value) when is_list(Value) ->
            Obj = new(Bucket, Key, Value),
            put(Riak, Obj);
       (Bucket, Key, DataType) ->
            riakc_pb_socket:update_type(Riak, l2b(Bucket), l2b(Key), to_op(DataType))
    end.

put(Riak, Object) ->
    riakc_pb_socket:put(Riak, Object).

%%
%% Convert strings to binary, but also handle bucket types
l2b({Type, Bucket}) ->
    {unicode:characters_to_binary(Type), unicode:characters_to_binary(Bucket)};
l2b(String) ->
    unicode:characters_to_binary(String).

to_op(Type) ->
    to_op(Type, riakc_datatype:module_for_term(Type)).

to_op(Type, Module) ->
    Module:to_op(Type).
