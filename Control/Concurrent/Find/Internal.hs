{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE Rank2Types         #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal where

import Control.Concurrent.STM.CTMVar (CTMVar, newEmptyCTMVar, readCTMVar, isEmptyCTMVar, putCTMVar, tryPutCTMVar)
import Control.Monad (liftM)
import Control.Monad.Conc.Class
import Control.Monad.STM.Class
import Data.Maybe (isNothing)
import Unsafe.Coerce (unsafeCoerce)

newtype FWrap m a = FWrap { unWrap :: forall x. F m x a }

data F m x a = F
  { _result :: CTMVar (STMLike m) (Maybe x)
  -- ^ The result of the computation.
  , _mapped :: x -> a
  -- ^ A function to change the type.
  }

data FState = StillComputing | HasFailed | HasSucceeded
  deriving (Eq)

-- | Ignore the contents of an 'FWrap' (for passing lists of them
-- around).
voidF :: FWrap m a -> FWrap m ()
voidF f = wrap $ F (_result $ unWrap f) $ const ()

-- | Construct an 'FWrap' from an 'F'.
wrap :: F m x a -> FWrap m a
-- Really not nice, but I have had difficulty getting GHC to unify
-- @F m x a@ with @forall x. F m x a@
wrap = FWrap . unsafeCoerce

--------------------------------------------------------------------------------
-- Concurrency

-- | Block until all computations interested in have successfully
-- completed. If any fail, this immediately returns 'False'.
blockOn :: MonadConc m => [FWrap m ()] -> m Bool
blockOn fs = atomically $ do
  states <- mapM getState fs
  case (HasFailed `elem` states, HasSucceeded `elem` states) of
    (True, _) -> return False
    (_, True) -> return True
    _ -> retry

-- | Get the result of a computation, this blocks until the result is
-- present, so be careful not to lose parallelism.
result :: MonadConc m => FWrap m a -> m (Maybe a)
result f = fmap (_mapped $ unWrap f) `liftM` res where
  res = atomically . readCTMVar . _result $ unWrap f

-- | Push a batch of work to the queue, returning a 'CTMVar' that can
-- be blocked on to get the result. As soon as one succeeds, the
-- others are killed.
work :: MonadConc m => [m (FWrap m a)] -> m (CTMVar (STMLike m) (Maybe a))
work workitems = do
  res  <- atomically newEmptyCTMVar
  caps <- getNumCapabilities
  _    <- fork $ driver caps res
  return res

  where
    -- If there's only one capability don't bother with threads.
    driver 1 res = go workitems where
      go [] = fail ""
      go (item:rest) = process item (putCTMVar res) (go rest)

    driver caps res = do
      remaining <- atomically $ newCTVar workitems
      tids <- mapM (\cap -> forkOn cap $ worker res remaining) [1..caps]
      _ <- atomically $ readCTMVar res
      mapM_ killThread tids

    worker res remaining = do
      witem <- steal remaining
      case witem of
        Just item ->
          process item (liftM (const ()) . tryPutCTMVar res) (worker res remaining)
        Nothing -> return ()

    process item store continue = do
      fwrap  <- item
      maybea <- result fwrap
      case maybea of
        Just _  -> atomically $ store maybea
        Nothing -> continue

    steal remaining = atomically $ do
      remaining' <- readCTVar remaining
      case remaining' of
        (x:xs) -> do
          writeCTVar remaining xs
          return $ Just x
        [] -> return Nothing

--------------------------------------------------------------------------------
-- State snapshots

-- | Get the current state of a computation.
getState :: MonadConc m => FWrap m a -> STMLike m FState
getState f = do
  empty <- isEmptyCTMVar . _result $ unWrap f
  if empty
  then return StillComputing
  else do
    failed <- hasFailed f
    return $ if failed then HasFailed else HasSucceeded

-- | Check if a computation has failed. If the computation has not
-- terminated, this immediately returns 'False'.
hasFailed :: MonadConc m => FWrap m a -> STMLike m Bool
hasFailed f = do
  working <- isEmptyCTMVar . _result $ unWrap f
  if working
  then return False
  else isNothing `liftM` readCTMVar (_result $ unWrap f)
