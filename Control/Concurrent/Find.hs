-- | Concurrent nondeterministic search.
module Control.Concurrent.Find
  ( -- * @Find@ monad
    Find
  , runFind
  , unsafeRunFind
  , hasResult
  -- * Basic Searches
  , success, failure
  , oneOf, allOf
  -- * Combinators
  , (!), (?), (@!), (@?)
  , findIn, findIn'
  , findInMapped, findInMapped'
  , findBoth, findEither
  ) where

import Control.Applicative (Applicative(..), Alternative(..), (<$>))
import Control.Concurrent.Find.Internal
import Control.Monad (MonadPlus(..), void, liftM)
import Control.Monad.Conc.Class (MonadConc)
import Data.Maybe (isJust)
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

--------------------------------------------------------------------------------
-- Instances

-- | 'fmap' delays applying the function until the value is demanded,
-- to avoid blocking.
instance MonadConc m => Functor (Find m) where
  fmap g (Find mf) = Find $ fmap g `liftM` mf

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

--------------------------------------------------------------------------------
-- Execution

-- | Execute a 'Find' computation, returning a 'Just' value if there
-- was at least one result (and a different value may be returned each
-- time), or 'Nothing' if there are no results.
runFind :: MonadConc m => Find m a -> m (Maybe a)
runFind (Find mf) = mf >>= result

-- | Unsafe version of 'runFind'. This will error at runtime if the
-- computation fails.
unsafeRunFind :: MonadConc m => Find m a -> m a
unsafeRunFind (Find mf) = mf >>= unsafeResult

-- | Check if a computation has a result. This will block until a
-- result is found.
hasResult :: Find IO a -> Bool
{-# NOINLINE hasResult #-}
hasResult f = unsafePerformIO $ isJust `liftM` runFind f

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
  (var, kill) <- work True $ map unFind as
  return $ workItem var head kill

-- | Return all non-failing results, the order is nondeterministic.
allOf :: MonadConc m => [Find m a] -> Find m [a]
allOf [] = success []
allOf as = Find $ do
  (var, kill) <- work False $ map unFind as
  return $ workItem var id kill

--------------------------------------------------------------------------------
-- Combinators

-- | Flipped infix version of 'findIn'.
(!) :: MonadConc m => [a] -> (a -> Bool) -> Find m a
(!) = flip findIn

-- | Flipped infix version of 'findInMapped'.
(?) :: MonadConc m => [a] -> (a -> Maybe b) -> Find m b
(?) = flip findInMapped

-- | Flipped infix version of 'findIn''.
(@!) :: MonadConc m => [a] -> (a -> Bool) -> Find m [a]
(@!) = flip findIn'

-- | Flipped infix version of 'findInMapped''.
(@?) :: MonadConc m => [a] -> (a -> Maybe b) -> Find m [b]
(@?) = flip findInMapped'

-- | Find an element of a list satisfying a predicate.
findIn :: MonadConc m => (a -> Bool) -> [a] -> Find m a
findIn f as = oneOf [if f a then success a else failure | a <- as]

-- | Variant of 'findIn' which returns all successes.
findIn' :: MonadConc m => (a -> Bool) -> [a] -> Find m [a]
findIn' f as = allOf [if f a then success a else failure | a <- as]

-- | Find an element of a list after some transformation.
findInMapped :: MonadConc m => (a -> Maybe b) -> [a] -> Find m b
findInMapped f = oneOf . map (maybe failure success . f)

-- | Variant of 'findInMapped' which returns all 'Just's.
findInMapped' :: MonadConc m => (a -> Maybe b) -> [a] -> Find m [b]
findInMapped' f = allOf . map (maybe failure success . f)

-- | Find elements from a pair of lists satisfying predicates. Both
-- lists are searched in parallel.
findBoth :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (a, b)
findBoth f g as bs = (,) <$> findIn f as <*> findIn g bs

-- | Find an element from one of two lists which satisfies a
-- predicate. Both lists are searched in parallel.
findEither :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (Either a b)
findEither f g as bs = (Left <$> findIn f as) <|> (Right <$> findIn g bs)
