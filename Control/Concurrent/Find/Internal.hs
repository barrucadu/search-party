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

-- | A unit of work in a monad @m@ which will produce a final result
-- of type @a@.
newtype WorkItem m a = WorkItem { unWrap :: forall x. WorkItem' m x a }

-- | A unit of work in a monad @m@ producing a result of type @x@,
-- which will then be transformed into a value of type @a@.
data WorkItem' m x a = WorkItem'
  { _result :: CTMVar (STMLike m) (Maybe x)
  -- ^ The result of the computation.
  , _mapped :: x -> a
  -- ^ A function to change the type.
  }

-- | The possible states that a work item may be in.
data WorkState = StillComputing | HasFailed | HasSucceeded
  deriving (Eq)

-- | Ignore the contents of a 'WorkItem' (for passing lists of them
-- around).
voidW :: WorkItem m a -> WorkItem m ()
voidW f = workItem (_result $ unWrap f) $ const ()

-- | Construct a 'WorkItem'.
workItem :: (CTMVar (STMLike m) (Maybe x)) -> (x -> a) -> WorkItem m a
-- Really not nice, but I have had difficulty getting GHC to unify
-- @F m x a@ with @forall x. F m x a@
workItem res mapp = wrap $ WorkItem' res mapp where
  wrap :: forall m x a. WorkItem' m x a -> WorkItem m a
  wrap = WorkItem . unsafeCoerce

--------------------------------------------------------------------------------
-- Concurrency

-- | Block until all computations interested in have successfully
-- completed. If any fail, this immediately returns 'False'.
blockOn :: MonadConc m => [WorkItem m ()] -> m Bool
blockOn fs = atomically $ do
  states <- mapM getState fs
  case (HasFailed `elem` states, HasSucceeded `elem` states) of
    (True, _) -> return False
    (_, True) -> return True
    _ -> retry

-- | Get the result of a computation, this blocks until the result is
-- present, so be careful not to lose parallelism.
result :: MonadConc m => WorkItem m a -> m (Maybe a)
result f = fmap (_mapped $ unWrap f) `liftM` res where
  res = atomically . readCTMVar . _result $ unWrap f

-- | Push a batch of work to the queue, returning a 'CTMVar' that can
-- be blocked on to get the result. As soon as one succeeds, the
-- others are killed.
work :: MonadConc m => [m (WorkItem m a)] -> m (CTMVar (STMLike m) (Maybe a))
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

-- | Get the current state of a work item.
getState :: MonadConc m => WorkItem m a -> STMLike m WorkState
getState f = do
  empty <- isEmptyCTMVar . _result $ unWrap f
  if empty
  then return StillComputing
  else do
    failed <- hasFailed f
    return $ if failed then HasFailed else HasSucceeded

-- | Check if a work item has failed. If the computation has not
-- terminated, this immediately returns 'False'.
hasFailed :: MonadConc m => WorkItem m a -> STMLike m Bool
hasFailed f = do
  working <- isEmptyCTMVar . _result $ unWrap f
  if working
  then return False
  else isNothing `liftM` readCTMVar (_result $ unWrap f)
