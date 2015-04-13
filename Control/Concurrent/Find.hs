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
  , readStream, unsafeReadStream
  , takeStream, takeMapStream
  , gatherStream
  , zipStream
  , toList, unsafeToList
  -- * Basic Searches
  , success, failure
  , oneOf, allOf
  , firstOf, orderedOf
  -- * Combinators
  , (!), (?), (@!), (@?)
  , find, findAll, mapped, mappedAll
  , both
  , Control.Concurrent.Find.either
  , merging
  -- ** Deterministic
  -- | These are all safe to extract the result from with
  -- 'toMaybe' and 'toList'.
  , (.!), (.?), (>!), (>?)
  , first, inOrder, mappedFirst, mappedInOrder
  -- * List Operations
  , parMap, parFilter
  , parConcatMap, parMapMaybe
  , parAny, parAll
  , parSeq, parForce
  ) where

import Control.Applicative (Applicative(..), Alternative(..), (<$>))
import Control.Concurrent.Find.Internal
import Control.DeepSeq (NFData, deepseq, force)
import Control.Monad (MonadPlus(..), void, liftM)
import Control.Monad.Conc.Class (MonadConc, STMLike, atomically)
import Data.Foldable (Foldable)
import Data.Maybe (isJust, fromJust)
import Data.Monoid (Monoid(..), (<>), mempty)
import System.IO.Unsafe (unsafePerformIO)

import qualified Data.Foldable as F

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
{-# NOINLINE toMaybe #-}
toMaybe = unsafePerformIO . runFind

-- | Check if a computation has a result. This will block until a
-- result is found.
--
-- This uses 'unsafePerformIO' internally.
hasResult :: Find IO a -> Bool
hasResult = isJust . toMaybe

--------------------------------------------------------------------------------
-- Result Streams

-- | Read the head of stream, if the stream is finished 'Nothing' will
-- be returned.
readStream :: MonadConc m => Stream m a -> m (Maybe a)
readStream stream = liftM (fmap fst) . atomically . readChan $ unStream stream

-- | Take some values from the start of a stream, if the stream does
-- not contain that many elements, a shorter result collection is
-- returned.
--
-- You will probably use this with the type @:: MonadConc m -> Int -> Stream m a -> m [a]@
takeStream :: (MonadConc m, Applicative t, Monoid (t a)) => Int -> Stream m a -> m (t a)
takeStream n = takeMapStream n pure

-- | Like 'takeStream', but return an arbitrary monoid.
takeMapStream :: (MonadConc m, Monoid o) => Int -> (a -> o) -> Stream m a -> m o
takeMapStream 0 _ _ = return mempty
takeMapStream n f stream = do
  hd <- readStream stream
  case hd of
    Just a  -> (f a <>) `liftM` takeMapStream (n-1) f stream
    Nothing -> return mempty

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
--
-- As streams are not duplicated when zipped, the original streams
-- should not be used again: this goes for functions which call
-- 'zipStream', like 'mappend' and '<*>' too, otherwise this sort of
-- thing can happen (abusing list syntax for streams):
--
-- > let s1 = [Sum 1, Sum 2, Sum 3]
-- > let s2 = s1 <> mempty
-- > let s3 = s1 <> mempty
-- > readStream s2 == Sum 1
-- > readStream s3 == Sum 2
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

-- | Take a computation returning a 'Stream', and convert it to a
-- list. This is only safe if the same stream is never used again
-- after calling this function, and the order of the stream is
-- deterministic (eg, generated by 'orderedOf').
--
-- This uses 'unsafePerformIO' internally.
toList :: IO (Stream IO a) -> [a]
{-# NOINLINE toList #-}
toList = unsafeToList . unsafePerformIO

-- | Convert a 'Stream' to a list. This is only safe if the stream is
-- never used again after calling this function, and the order of the
-- stream is deterministic (eg, generated by 'orderedOf').
--
-- Example safe usage:
--
-- > parFilter :: (a -> Bool) -> [a] -> [a]
-- > parFilter p xs = unsafeToList . unsafePerformIO $ xs >! p
--
-- This uses 'unsafePerformIO' internally.
unsafeToList :: Stream IO a -> [a]
{-# NOINLINE unsafeToList #-}
unsafeToList stream = stream `seq` maybe [] (:unsafeToList stream) a where
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
oneOf :: (MonadConc m, Foldable t) => t (Find m a) -> Find m a
oneOf as = case F.toList as of
  []  -> failure
  as' -> Find $ do
    Left (var, kill) <- work False False $ map unFind as'
    return $ workItem var id kill

-- | Return the first (in the toList of the 'Foldable') non-failing
-- result. This is deterministic.
firstOf :: (MonadConc m, Foldable t) => t (Find m a) -> Find m a
firstOf as = case F.toList as of
  []  -> failure
  as' -> Find $ do
    Left (var, kill) <- work False True $ map unFind as'
    return $ workItem var id kill

-- | Return all non-failing results, the order is nondeterministic.
allOf :: (MonadConc m, Foldable t) => t (Find m a) -> m (Stream m a)
allOf as = case F.toList as of
  []  -> Stream `liftM` atomically closedChan
  as' -> do
    -- Streaming computations don't really short-circuit, they just
    -- block when there are enough items in the stream.
    Right c <- work True False $ map unFind as'
    return $ Stream c

-- | Return all non-failing results, in the order they appear in the
-- toList of the 'Foldable'. This is deterministic.
orderedOf :: (MonadConc m, Foldable t) => t (Find m a) -> m (Stream m a)
orderedOf as = case F.toList as of
  []  -> Stream `liftM` atomically closedChan
  as' -> do
    Right c <- work True True $ map unFind as'
    return $ Stream c

--------------------------------------------------------------------------------
-- Combinators

-- | Flipped infix version of 'find'.
(!) :: (MonadConc m, Foldable t) => t a -> (a -> Bool) -> Find m a
as ! p = oneOf $ fomap (\a -> if p a then success a else failure) as

-- | Return an element in the traversal matching the predicate.
find :: (MonadConc m, Foldable t) => (a -> Bool) -> t a -> Find m a
find = flip (!)

-- | Flipped infix version of 'first'.
(.!) :: (MonadConc m, Foldable t) => t a -> (a -> Bool) -> Find m a
as .! p = firstOf $ fomap (\a -> if p a then success a else failure) as

-- | Variant of 'find' which returns the first such element.
first :: (MonadConc m, Foldable t) => (a -> Bool) -> t a -> Find m a
first = flip (.!)

-- | Flipped infix version of 'mapped'.
(?) :: (MonadConc m, Foldable t) => t a -> (a -> Maybe b) -> Find m b
as ? f = oneOf $ fomap (maybe failure success . f) as

-- | Return an element in the traversal giving a 'Just'.
mapped :: (MonadConc m, Foldable t) => (a -> Maybe b) -> t a -> Find m b
mapped = flip (?)

-- | Flipped infix version of 'mappedFirst'.
(.?) :: (MonadConc m, Foldable t) => t a -> (a -> Maybe b) -> Find m b
as .? f = firstOf $ fomap (maybe failure success . f) as

-- | Variant of 'mapped' which returns the first such element.
mappedFirst :: (MonadConc m, Foldable t) => (a -> Maybe b) -> t a -> Find m b
mappedFirst = flip (.?)

-- | Flipped infix version of 'findAll'.
(@!) :: (MonadConc m, Foldable t) => t a -> (a -> Bool) -> m (Stream m a)
as @! p = allOf $ fomap (\a -> if p a then success a else failure) as

-- | Variant of 'find' which returns all such elements.
findAll :: (MonadConc m, Foldable t) => (a -> Bool) -> t a -> m (Stream m a)
findAll = flip (@!)

-- | Flipped infix version of 'inOrder'.
(>!) :: (MonadConc m, Foldable t) => t a -> (a -> Bool) -> m (Stream m a)
as >! p = orderedOf $ fomap (\a -> if p a then success a else failure) as

-- | Variant of 'findAll' which returns the elements in the same order
-- that they appear in the original traversal.
inOrder :: (MonadConc m, Foldable t) => (a -> Bool) -> t a -> m (Stream m a)
inOrder = flip (>!)

-- | Flipped infix version of 'mappedAll'.
(@?) :: (MonadConc m, Foldable t) => t a -> (a -> Maybe b) -> m (Stream m b)
as @? f = allOf $ fomap (maybe failure success . f) as

-- | Variant of 'mapped' which returns all such elements.
mappedAll :: (MonadConc m, Foldable t) => (a -> Maybe b) -> t a -> m (Stream m b)
mappedAll = flip (@?)

-- | Flipped infix version of 'mappedInOrder'.
(>?) :: (MonadConc m, Foldable t) => t a -> (a -> Maybe b) -> m (Stream m b)
as >? f = orderedOf $ fomap (maybe failure success . f) as

-- | Variant of 'mappedAll' which returns the elements in the same
-- order that they appear in the original traversal.
mappedInOrder :: (MonadConc m, Foldable t) => (a -> Maybe b) -> t a -> m (Stream m b)
mappedInOrder = flip (>?)

-- | Find elements from a pair of traversables satisfying
-- predicates. Both traversals are searched in parallel.
both :: (MonadConc m, Foldable t) => (a -> Bool) -> (b -> Bool) -> t a -> t b -> Find m (a, b)
both p q as bs = (,) <$> as ! p <*> bs ! q

-- | Find an element from one of two traversables which satisfies a
-- predicate. Both traversals are searched in parallel.
either :: (MonadConc m, Foldable t) => (a -> Bool) -> (b -> Bool) -> t a -> t b -> Find m (Either a b)
either p q as bs = (Left <$> as ! p) <|> (Right <$> bs ! q)

-- | Gather all non-failing results and 'mconcat' them together. The
-- order of merging is nondeterministic, and so this is most likely to
-- be useful in the context of gathering a list of results.
--
-- If there are likely to be many 'mempty' results, this is faster
-- than
--
-- > fmap gatherStream $ xs @? Just . f
merging :: (MonadConc m, Monoid o, Eq o, Foldable t) => (a -> o) -> t a -> Find m o
merging f as = Find $ do
  stream <- as @? \a -> let o = f a in if o == mempty then Nothing else Just o
  o <- gatherStream stream
  workItem' $ Just o

--------------------------------------------------------------------------------
-- List operations

-- | Lazy parallel 'map'. This evaluates elements to WHNF in parallel.
parMap :: (a -> b) -> [a] -> [b]
{-# NOINLINE [1] parMap #-}
parMap f xs = toList $ xs >? (\x -> x `seq` Just (f x))
{-# RULES
"map/map"    forall f g xs. parMap f (parMap    g xs) = parMap (f . g) xs
"map/filter" forall f p xs. parMap f (parFilter p xs) = toList $ xs >? bool p f
"map/seq"    forall f xs.   parMap f (parSeq      xs) = parMap f xs
"map/force"  forall f xs.   parMap f (parForce    xs) = parMap (\x -> x `deepseq` f x) xs
 #-}

-- | Parallel 'concatMap'. This evaluates results to WHNF in parallel,
-- and should be preferred to @concat . parMap f@.
parConcatMap :: (a -> [b]) -> [a] -> [b]
{-# INLINE parConcatMap #-}
parConcatMap f = concat . parMapMaybe go where
  go x = let x' = f x in if null x' then Nothing else Just x'

-- | Lazy parallel 'mapMaybe'. This evaluates results to WHNF in
-- parallel.
parMapMaybe :: (a -> Maybe b) -> [a] -> [b]
parMapMaybe f xs = toList $ xs >? f
{-# RULES
"mapmaybe/mapmaybe" forall f g xs. parMapMaybe f (parMapMaybe g xs) = parMapMaybe (\x -> f x >>= g) xs
 #-}

-- | Lazy parallel 'filter'. This checks the predicate in parallel.
parFilter :: (a -> Bool) -> [a] -> [a]
{-# NOINLINE [1] parFilter #-}
parFilter p xs = toList $ xs >! p
{-# RULES
"filter/filter" forall p q xs. parFilter p (parFilter q xs) = parFilter (\x -> p x && q x) xs
"filter/map"    forall f p xs. parFilter p (parMap    f xs) = toList $ xs >? (\x -> x `seq` bool p id (f x))
"filter/seq"    forall p xs.   parFilter p (parSeq      xs) = parFilter (\x -> x `seq`     p x) xs
"filter/force"  forall p xs.   parFilter p (parForce    xs) = parFilter (\x -> x `deepseq` p x) xs
 #-}

-- | Lazy parallel 'any'. This checks the predicate in parallel.
parAny :: (a -> Bool) -> [a] -> Bool
{-# NOINLINE [1] parAny #-}
parAny p xs = hasResult $ xs ! p
{-# RULES
"any/map"   forall f p xs. parAny p (parMap f xs) = hasResult $ xs ? (bool p id . f)
"any/seq"   forall p xs.   parAny p (parSeq   xs) = parAny p xs
"any/force" forall p xs.   parAny p (parForce xs) = parAny p xs
 #-}

-- | Lazy parallel 'all'. This checks the predicate in parallel.
parAll :: (a -> Bool) -> [a] -> Bool
{-# INLINE parAll #-}
parAll p = parAny $ not . p

-- | Evaluate each element of a list to weak-head normal form in
-- parallel.
parSeq :: [a] -> [a]
{-# NOINLINE [1] parSeq #-}
parSeq xs = toList $ xs >! (`seq` True)
{-# RULES
"seq/seq"    forall xs.   parSeq (parSeq      xs) = parSeq xs
"seq/force"  forall xs.   parSeq (parForce    xs) = parForce xs
"seq/map"    forall f xs. parSeq (parMap    f xs) = parMap f xs
"seq/filter" forall p xs. parSeq (parFilter p xs) = parFilter (\x -> x `seq` p x) xs
 #-}

-- | Evaluate each element of a list to normal form in parallel.
parForce :: NFData a => [a] -> [a]
{-# NOINLINE [1] parForce #-}
parForce xs = toList $ xs >! (`deepseq` True)
{-# RULES
"force/force"  forall xs.   parForce (parForce  xs)   = parForce xs
"force/seq"    forall xs.   parForce (parSeq    xs)   = parForce xs
"force/map"    forall f xs. parForce (parMap    f xs) = parMap (force . f) xs
"force/filter" forall p xs. parForce (parFilter p xs) = parFilter (\x -> x `deepseq` p x) xs
 #-}

--------------------------------------------------------------------------------
-- Utilities

-- | Turn a 'Foldable' into a list and map a function over it.
fomap :: Foldable t => (a -> b) -> t a -> [b]
fomap f = map f . F.toList

-- | Check if a condition holds and, if so, transform the value.
bool :: (a -> Bool) -> (a -> b) -> a -> Maybe b
{-# INLINE bool #-}
bool p f x = if p x then Just $ f x else Nothing
