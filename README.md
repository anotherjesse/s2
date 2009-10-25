S2: Simplistic S3
=================

Getting Started
---------------

Note: I'm a beginner at erlang so I don't really know what I'm doing here.  
I'm certain that this is the slow way of doing things - but it works.

First run:

    cd src
    erlc *.erl && erl
    req:first_run().
    req:start().

After making changes to a file:

    erlc *.erl && erl
    req:start().

Or simply:

    c(req). % or whatever filename


Notes / Ideas
-------------

This project is currently being worked on when I need to unwind or have a few minutes here and there


  * backend is mnesia (bootstraping)
  * Goal is application layer is in charge of  
    replication to many storage backends

Thanks
------

S2 is based on Misultin (hacking in support for unsupported methods)
and uses the Boto S3 library as a jumpstart for unit testing (acts
as a todo for implementation)
