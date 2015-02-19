{-# LANGUAGE RankNTypes #-}
-- | Concurrent nondeterministic search.
module Control.Concurrent.Find
  ( -- * @Find@ monad
    Find
  , runFind
  -- * Combinators
  , (!)
  , success, failure
  , findIn, findBoth, findEither, oneOf
  ) where

import Control.Applicative (Applicative(..), Alternative(..), (<$>))
import Control.Concurrent.Find.Internal
import Control.Concurrent.STM.CTMVar (newCTMVar)
import Control.Monad (MonadPlus(..), liftM)
import Control.Monad.Conc.Class (MonadConc, atomically)
import Data.Maybe (fromJust)

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
  fmap g (Find mf) = Find $ do
    f <- mf
    return $ workItem (_result $ unWrap f) (g . _mapped (unWrap f))

-- | '<*>' performs both computations in parallel, and immediately
-- fails as soon as one does, giving a symmetric short-circuiting
-- behaviour.
instance MonadConc m => Applicative (Find m) where
  pure a = Find $ do
    var <- atomically . newCTMVar $ Just a
    return $ workItem var id

  (Find mf) <*> (Find ma) = Find $ do
    f <- mf
    a <- ma

    success <- blockOn [voidW f, voidW a]

    if success
    then do
      fres <- fromJust `liftM` result f
      ares <- fromJust `liftM` result a

      var <- atomically . newCTMVar . Just $ fres ares
      return $ workItem var id

    else fail ""

-- | '>>=' should be avoided, as it necessarily imposes sequencing,
-- and blocks until the value being bound has been computed.
instance MonadConc m => Monad (Find m) where
  return = pure

  fail _ = Find $ do
    var <- atomically $ newCTMVar Nothing
    return $ workItem var id

  (Find mf) >>= g = Find $ do
    f <- mf
    res <- result f
    case res of
      Just a -> do
        let (Find mb) = g a
        mb
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
-- Combinators

-- | Flipped infix version of 'findIn'.
(!) :: MonadConc m => [a] -> (a -> Bool) -> Find m a
(!) = flip findIn

-- | Search which always succeeds.
success :: MonadConc m => a -> Find m a
success = return

-- | Search which always fails.
failure :: MonadConc m => Find m a
failure = fail ""

-- | Find an element of a list satisfying a predicate.
findIn :: MonadConc m => (a -> Bool) -> [a] -> Find m a
findIn f as = oneOf [if f a then a `seq` success a else failure | a <- as]

-- | Find elements from a pair of lists satisfying predicates. Both
-- lists are searched in parallel.
findBoth :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (a, b)
findBoth f g as bs = (,) <$> findIn f as <*> findIn g bs

-- | Find an element from one of two lists which satisfies a
-- predicate. Both lists are searched in parallel.
findEither :: MonadConc m => (a -> Bool) -> (b -> Bool) -> [a] -> [b] -> Find m (Either a b)
findEither f g as bs = (Left `liftM` findIn f as) <|> (Right `liftM` findIn g bs)

-- | Return one non-failing result nondeterministically.
oneOf :: MonadConc m => [Find m a] -> Find m a
oneOf [] = failure
oneOf as = Find $ do
  var <- work $ map unFind as
  return $ workItem var id

--------------------------------------------------------------------------------
-- Execution

-- | Execute a 'Find' computation, returning a 'Just' value if there
-- was at least one result (and a different value may be returned each
-- time), or 'Nothing' if there are no results.
runFind :: MonadConc m => Find m a -> m (Maybe a)
runFind (Find mf) = mf >>= result
