-module(xmq_crypto).

-export([aes_cbc_128_decrypt/2, aes_cbc_128_encrypt/2, pad/2, unpad/1]).

-define(NULL_IV_128, <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>).

aes_cbc_128_decrypt(Data, Key) ->
    crypto:aes_cbc_128_decrypt(Key, ?NULL_IV_128, Data).

aes_cbc_128_encrypt(Data, Key) ->
    crypto:aes_cbc_128_encrypt(Key, ?NULL_IV_128, Data).

pad(Bin, BlockSize) ->
    PadCount = BlockSize - (size(Bin) rem BlockSize),
    Pad = binary:copy(<<PadCount>>, PadCount),
    <<Bin/binary, Pad/binary>>.

unpad(Bin) ->
    try
	binary:part(Bin, 0, size(Bin) - binary:last(Bin))
    catch
	error:badarg -> error(invalid_padding)
    end.
