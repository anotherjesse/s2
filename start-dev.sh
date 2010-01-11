#!/bin/sh
cd `dirname $0`
make
exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl -mnesia dir '"/tmp"' -s reloader -s s2
