search-party [![Build Status][build-status]][build-log]
============

Often in search problems you don't care exactly *which* solution you
get, only that you are guaranteed to get one if it exists. Furthermore
the problem itself might be expensive. This makes cases where the
problem can be re-arranged to checking the suitability of some
collection of candidates suitable for parallelisation.

This package provides a monad for short-circuiting search problems,
where blocking only occurs when the result of something is
demanded. In particular, this means that parallelism can be preserved
and blocking avoided completely if just using the Functor,
Applicative, or Alternative instances.

The documentation of the latest developmental version is
[available online][docs].

Note
----

search-party depends on [dejafu][], which has yet to be pushed to
hackage. Assuming you are using sandboxes, you can do something like
this:

~~~~{.bash}
git clone https://github.com/barrucadu/search-party.git
git clone https://github.com/barrucadu/dejafu.git
cd search-party
cabal sandbox init
cabal sandbox add-source ../dejafu
cabal install --only-dependencies
cabal build
~~~~

Contributing
------------

Bug reports, pull requests, and comments are very welcome!

Feel free to contact me on GitHub, through IRC (#haskell on freenode),
or email (mike@barrucadu.co.uk).

[build-status]: http://ci.barrucadu.co.uk/job/search-party/badge/icon?style=plastic
[build-log]:    http://ci.barrucadu.co.uk/job/search-party/
[docs]:         https://barrucadu.github.io/search-party
[dejafu]:       https://github.com/barrucadu/dejafu
