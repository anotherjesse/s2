#!/bin/sh
cd `dirname $0`
make
exec erl -mnesia dir '"/tmp"' -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl -s reloader -s s2
