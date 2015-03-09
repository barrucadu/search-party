-- | Concurrent nondeterministic search.
module Control.Concurrent.Find
  ( -- * @Find@ monad
    Find
  , runFind
  , unsafeRunFind
  , hasResult
  , toMaybe
  -- * Result Streams
  , Stream
  , readStream
  , takeStream
  , unsafeReadStream
  , gatherStream
  , zipStream
  , toList
  -- * Basic Searches
  , success, failure
  , oneOf, allOf
  , firstOf, orderedOf
  -- * Combinators
  , (!), (?), (@!), (@?)
  , both
  , Control.Concurrent.Find.either
  , merging
  -- ** Deterministic
  -- | These are all safe to extract the result from with
  -- 'toMaybe' and 'toList'.
  , (.!), (.?), (>!), (>?)
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

    completed <- blockOn [void f, void a]

    if completed
    then do
      fres <- unsafeResult f
      ares <- unsafeResult a

      workItem' . Just $ fres ares

    else workItem' Nothing

-- | '<*>' acts like a zipping function, truncating when one stream
-- ends.
instance MonadConc m => Applicative (Stream m) where
  pure a    = Stream $ builderChan (return $ Just a)
  sf <*> sa = zipStream ($) Nothing Nothing sf sa

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
  empty   = fail ""
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

-- | 'mappend' acts like a zipping function, truncating when one
-- stream ends.
instance (MonadConc m, Monoid o) => Monoid (Stream m o) where
  mempty        = pure mempty
  mappend sa sb = mappend <$> sa <*> sb

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

-- | Convert a 'Find' to a 'Maybe'. This is only safe if the result
-- found is deterministic (eg, generated by 'firstOf').
--
-- Example safe usage:
--
-- > first :: (a -> Bool) -> [a] -> Maybe a
-- > first p xs = toMaybe $ xs .! p
--
-- This uses 'unsafePerformIO' internally.
toMaybe :: Find IO a -> Maybe a
toMaybe = unsafePerformIO . runFind

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
readStream stream = liftM (fmap fst) . atomically . readChan $ unStream stream

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

-- | Zip two streams together, with an optional values to supply when
-- a component stream runs out. If no value is given, the resulting
-- stream ends as soon as that component stream does.
zipStream :: MonadConc m => (a -> b -> c) -> Maybe a -> Maybe b -> Stream m a -> Stream m b -> Stream m c
zipStream f fillera fillerb (Stream sa) (Stream sb) = Stream . builderChan $ do
    a <- readChan sa
    b <- readChan sb

    let zipped x y  = return . Just $ f x y
    let endOfStream = return Nothing

    case (a, b) of
      -- If both streams have a value, apply the function
      (Just (a', _), Just (b', _)) -> zipped a' b'

      -- If one stream has a value and the other is empty, fill in the
      -- blank. If there is no filler, push the value just received
      -- back to the stream and indicate the end.
      (Just (a', undo), Nothing) -> case fillerb of
        Just b' -> zipped a' b'
        Nothing -> undo >> endOfStream
      (Nothing, Just (b', undo)) -> case fillera of
        Just a' -> zipped a' b'
        Nothing -> undo >> endOfStream

      -- If both streams are empty, empty.
      (Nothing, Nothing) -> endOfStream

-- | Convert a 'Stream' to a list. This is only safe if the stream is
-- never used again after calling this function, and the order of the
-- stream is deterministic (eg, generated by 'orderedOf').
--
-- Example safe usage:
--
-- > filterPar :: (a -> Bool) -> [a] -> [a]
-- > filterPar p xs = toList . unsafePerformIO $ xs >! p
--
-- This uses 'unsafePerformIO' internally.
toList :: Stream IO a -> [a]
{-# NOINLINE toList #-}
toList stream = stream `seq` maybe [] (:toList stream) a where
  a = unsafePerformIO $ readStream stream

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
  Left (var, kill) <- work False False $ map unFind as
  return $ workItem var id kill

-- | Return the first non-failing result. This is deterministic.
firstOf :: MonadConc m => [Find m a] -> Find m a
firstOf [] = failure
firstOf as = Find $ do
  Left (var, kill) <- work False True $ map unFind as
  return $ workItem var id kill

-- | Return all non-failing results, the order is nondeterministic.
allOf :: MonadConc m => [Find m a] -> m (Stream m a)
allOf [] = Stream `liftM` atomically closedChan
allOf as = do
  -- Streaming computations don't really short-circuit, they just
  -- block when there are enough items in the stream.
  Right c <- work True False $ map unFind as
  return $ Stream c

-- | Return all non-failing results, in the order they appear in the
-- list. This is deterministic.
orderedOf :: MonadConc m => [Find m a] -> m (Stream m a)
orderedOf [] = Stream `liftM` atomically closedChan
orderedOf as = do
  Right c <- work True True $ map unFind as
  return $ Stream c

--------------------------------------------------------------------------------
-- Combinators

-- | Return an element in the list matching the predicate.
(!) :: MonadConc m => [a] -> (a -> Bool) -> Find m a
as ! p = oneOf [if p a then success a else failure | a <- as]

-- | Variant of '!' which returns the first such element.
(.!) :: MonadConc m => [a] -> (a -> Bool) -> Find m a
as .! p = firstOf [if p a then success a else failure | a <- as]

-- | Return an element in the list giving a 'Just'.
(?) :: MonadConc m => [a] -> (a -> Maybe b) -> Find m b
as ? f = oneOf $ map (maybe failure success . f) as

-- | Variant of '?' which returns the first such element.
(.?) :: MonadConc m => [a] -> (a -> Maybe b) -> Find m b
as .? f = firstOf $ map (maybe failure success . f) as

-- | Variant of '!' which returns all such elements.
(@!) :: MonadConc m => [a] -> (a -> Bool) -> m (Stream m a)
as @! p = allOf [if p a then success a else failure | a <- as]

-- | Variant of '@!' which returns the elements in the same order that
-- they appear in the original list.
(>!) :: MonadConc m => [a] -> (a -> Bool) -> m (Stream m a)
as >! p = orderedOf [if p a then success a else failure | a <- as]

-- | Variant of '?' which returns all such elements.
(@?) :: MonadConc m => [a] -> (a -> Maybe b) -> m (Stream m b)
as @? f = allOf $ map (maybe failure success . f) as

-- | Variant of '@?' which returns the elements in the same order that
-- they appear in the original list.
(>?) :: MonadConc m => [a] -> (a -> Maybe b) -> m (Stream m b)
as >? f = orderedOf $ map (maybe failure success . f) as

-- | Find elements from a pair of lists satisfying predicates. Both
-- lists are searched in parallel.
both :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (a, b)
both p q as bs = (,) <$> as ! p <*> bs ! q

-- | Find an element from one of two lists which satisfies a
-- predicate. Both lists are searched in parallel.
either :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (Either a b)
either p q as bs = (Left <$> as ! p) <|> (Right <$> bs ! q)

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
