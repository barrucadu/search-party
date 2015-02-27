-- | Concurrent nondeterministic search.
module Control.Concurrent.Find
  ( -- * @Find@ monad
    Find
  , runFind
  , unsafeRunFind
  , hasResult
  -- * Result Streams
  , Stream
  , readStream
  , takeStream
  , unsafeReadStream
  , gatherStream
  , toFind
  -- * Basic Searches
  , success, failure
  , oneOf, allOf
  -- * Combinators
  , (!), (?), (@!), (@?)
  , findIn, findIn'
  , findInMapped, findInMapped'
  , findBoth, findEither
  , merging
  ) where

import Control.Applicative (Applicative(..), Alternative(..), (<$>))
import Control.Concurrent.Find.Internal
import Control.Monad (MonadPlus(..), void, liftM)
import Control.Monad.Conc.Class (MonadConc, STMLike, atomically)
import Data.Maybe (isJust, fromJust)
import Data.Monoid (Monoid(..), mempty)
import System.IO.Unsafe (unsafePerformIO)

-- | A value of type @Find m a@ represents a concurrent search
-- computation (happening in the 'MonadConc' monad @m@) which may
-- produce a value of type @a@, or fail. If a value can be returned,
-- one will be (although it's nondeterministic which one will actually
-- be returned). Usually you will be working with values of type @Find
-- IO a@, but the generality allows for testing.
--
-- You should prefer using the 'Applicative' instance over the 'Monad'
-- instance if you can, as the 'Applicative' preserves parallelism.
newtype Find m a = Find { unFind :: m (WorkItem m a) }

-- | A value of type @Stream m a@ represents a streaming sequence of
-- results being computed concurrently in the 'MonadConc' monad @m@,
-- which contains some values of type @a@.
newtype Stream m a = Stream { unStream :: Chan (STMLike m) a }

--------------------------------------------------------------------------------
-- Instances

-- | 'fmap' delays applying the function until the value is demanded,
-- to avoid blocking.
instance MonadConc m => Functor (Find m) where
  fmap g (Find mf) = Find $ fmap g `liftM` mf

-- | 'fmap' delays applying the function until the values are
-- demanded.
instance MonadConc m => Functor (Stream m) where
  fmap f (Stream c) = Stream $ f <$> c

-- | '<*>' performs both computations in parallel, and immediately
-- fails as soon as one does, giving a symmetric short-circuiting
-- behaviour.
instance MonadConc m => Applicative (Find m) where
  pure a = Find . workItem' $ Just a

  (Find mf) <*> (Find ma) = Find $ do
    f <- mf
    a <- ma

    success <- blockOn [void f, void a]

    if success
    then do
      fres <- unsafeResult f
      ares <- unsafeResult a

      workItem' . Just $ fres ares

    else workItem' Nothing

-- | '>>=' should be avoided, as it necessarily imposes sequencing,
-- and blocks until the value being bound has been computed.
instance MonadConc m => Monad (Find m) where
  return = pure

  fail _ = Find $ workItem' Nothing

  (Find mf) >>= g = Find $ do
    f   <- mf
    res <- result f

    case res of
      Just a  -> unFind $ g a
      Nothing -> fail ""

-- | '<|>' is a nondeterministic choice if both computations succeed,
-- otherwise it returns the nonfailing one. If both fail, this fails.
instance MonadConc m => Alternative (Find m) where
  empty = fail ""

  a <|> b = oneOf [a, b]

-- | 'mplus' is the same as '<|>', and follows the left distribution
-- law.
instance MonadConc m => MonadPlus (Find m) where
  mzero = empty
  mplus = (<|>)

-- | 'mappend' performs both computations in parallel, blocking until
-- both complete.
instance (MonadConc m, Monoid o) => Monoid (Find m o) where
  mempty = fail ""
  mappend (Find ma) (Find mb) = Find $ do
    a <- ma
    b <- mb
    ares <- result a
    bres <- result b

    workItem' $
      case (ares, bres) of
        (Just a', Just b') -> Just $ a' `mappend` b'
        (Just a', Nothing) -> Just a'
        (Nothing, Just b') -> Just b'
        (Nothing, Nothing) -> Nothing

--------------------------------------------------------------------------------
-- Execution

-- | Execute a 'Find' computation, returning a 'Just' value if there
-- was at least one result (and a different value may be returned each
-- time), or 'Nothing' if there are no results.
runFind :: MonadConc m => Find m a -> m (Maybe a)
runFind (Find mf) = mf >>= result

-- | Unsafe version of 'runFind'. This will error if the computation
-- fails.
unsafeRunFind :: MonadConc m => Find m a -> m a
unsafeRunFind (Find mf) = mf >>= unsafeResult

-- | Check if a computation has a result. This will block until a
-- result is found.
hasResult :: Find IO a -> Bool
{-# NOINLINE hasResult #-}
hasResult f = unsafePerformIO $ isJust `liftM` runFind f

--------------------------------------------------------------------------------
-- Result Streams

-- | Read the head of stream, if the stream is finished 'Nothing' will
-- be returned.
readStream :: MonadConc m => Stream m a -> m (Maybe a)
readStream stream = atomically . readChan . unChan $ unStream stream

-- | Take some values from the start of a stream, if the stream does
-- not contain that many elements, a shorter result list is returned.
takeStream :: MonadConc m => Int -> Stream m a -> m [a]
takeStream 0 _ = return []
takeStream n stream = do
  hd <- readStream stream
  case hd of
    Just a  -> (a:) `liftM` takeStream (n-1) stream
    Nothing -> return []

-- | Unsafe version of 'readStream'. This will error if the stream is
-- empty.
unsafeReadStream :: MonadConc m => Stream m a -> m a
unsafeReadStream = liftM fromJust . readStream

-- | Gather all the values of the stream together. This will block
-- until the entire computation terminates.
gatherStream :: (MonadConc m, Monoid o) => Stream m o -> m o
gatherStream stream = go mempty where
  go acc = readStream stream >>= maybe (return acc) (go . mappend acc)

-- | Convert a 'Stream' to a 'Find'. This will block until the entire
-- computation terminates.
toFind :: MonadConc m => Stream m a -> Find m [a]
toFind stream = Find $ do
  as <- gatherStream $ (:[]) <$> stream
  workItem' $ Just as

--------------------------------------------------------------------------------
-- Basic Searches

-- | Search which always succeeds.
success :: MonadConc m => a -> Find m a
success = return

-- | Search which always fails.
failure :: MonadConc m => Find m a
failure = fail ""

-- | Return one non-failing result nondeterministically.
oneOf :: MonadConc m => [Find m a] -> Find m a
oneOf [] = failure
oneOf as = Find $ do
  Left (var, kill) <- work False $ map unFind as
  return $ workItem var id kill

-- | Return all non-failing results, the order is nondeterministic.
allOf :: MonadConc m => [Find m a] -> m (Stream m a)
allOf [] = do
  c <- atomically newChan
  atomically $ endChan c
  return . Stream $ chan c

allOf as = do
  -- TODO: how to handle killing streaming computations?
  Right c <- work True $ map unFind as
  return . Stream $ chan c

--------------------------------------------------------------------------------
-- Combinators

-- | Flipped infix version of 'findIn'.
(!) :: MonadConc m => [a] -> (a -> Bool) -> Find m a
(!) = flip findIn

-- | Flipped infix version of 'findInMapped'.
(?) :: MonadConc m => [a] -> (a -> Maybe b) -> Find m b
(?) = flip findInMapped

-- | Flipped infix version of 'findIn''.
(@!) :: MonadConc m => [a] -> (a -> Bool) -> m (Stream m a)
(@!) = flip findIn'

-- | Flipped infix version of 'findInMapped''.
(@?) :: MonadConc m => [a] -> (a -> Maybe b) -> m (Stream m b)
(@?) = flip findInMapped'

-- | Find an element of a list satisfying a predicate.
findIn :: MonadConc m => (a -> Bool) -> [a] -> Find m a
findIn f as = oneOf [if f a then success a else failure | a <- as]

-- | Variant of 'findIn' which returns all successes.
findIn' :: MonadConc m => (a -> Bool) -> [a] -> m (Stream m a)
findIn' f as = allOf [if f a then success a else failure | a <- as]

-- | Find an element of a list after some transformation.
findInMapped :: MonadConc m => (a -> Maybe b) -> [a] -> Find m b
findInMapped f = oneOf . map (maybe failure success . f)

-- | Variant of 'findInMapped' which returns all 'Just's.
findInMapped' :: MonadConc m => (a -> Maybe b) -> [a] -> m (Stream m b)
findInMapped' f = allOf . map (maybe failure success . f)

-- | Find elements from a pair of lists satisfying predicates. Both
-- lists are searched in parallel.
findBoth :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (a, b)
findBoth f g as bs = (,) <$> findIn f as <*> findIn g bs

-- | Find an element from one of two lists which satisfies a
-- predicate. Both lists are searched in parallel.
findEither :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (Either a b)
findEither f g as bs = (Left <$> findIn f as) <|> (Right <$> findIn g bs)

-- | Gather all non-failing results and 'mconcat' them together. The
-- order of merging is nondeterministic, and so this is most likely to
-- be useful in the context of gathering a list of results.
--
-- If there are likely to be many 'mempty' results, this is faster
-- than
--
-- > fmap mconcat $ xs @? Just . f
merging :: (MonadConc m, Monoid o, Eq o) => (a -> o) -> [a] -> Find m o
merging f as = Find $ do
  stream <- as @? \a -> let o = f a in if o == mempty then Nothing else Just o
  o <- gatherStream stream
  workItem' $ Just o
