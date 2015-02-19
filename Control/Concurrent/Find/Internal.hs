{-# LANGUAGE Rank2Types #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal where

import Control.Concurrent.STM.CTMVar (CTMVar, newEmptyCTMVar, newCTMVar, readCTMVar, isEmptyCTMVar, putCTMVar, tryPutCTMVar)
import Control.Monad (liftM)
import Control.Monad.Conc.Class
import Control.Monad.STM.Class
import Data.Maybe (isNothing)
import Unsafe.Coerce (unsafeCoerce)

--------------------------------------------------------------------------------
-- Types

-- | A unit of work in a monad @m@ which will produce a final result
-- of type @a@.
newtype WorkItem m a = WorkItem { unWrap :: forall x. WorkItem' m x a }

instance Functor (WorkItem m) where
  fmap f (WorkItem w) = workItem (_result w) (f . _mapped w)

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

-- | Construct a 'WorkItem'.
workItem :: CTMVar (STMLike m) (Maybe x) -> (x -> a) -> WorkItem m a
-- Really not nice, but I have had difficulty getting GHC to unify
-- @F m x a@ with @forall x. F m x a@
workItem res mapp = wrap $ WorkItem' res mapp where
  wrap :: forall m x a. WorkItem' m x a -> WorkItem m a
  wrap = WorkItem . unsafeCoerce

-- | Construct a 'WorkItem' containing a result.
workItem' :: MonadConc m => Maybe a -> m (WorkItem m a)
workItem' a = flip workItem id `liftM` atomically (newCTMVar a)

--------------------------------------------------------------------------------
-- Processing work items

-- | Block until all computations interested in have successfully
-- completed. If any fail, this immediately returns 'False'.
blockOn :: MonadConc m => [WorkItem m ()] -> m Bool
blockOn fs = atomically $ do
  states <- mapM getState fs
  case (HasFailed `elem` states, all (==HasSucceeded) states) of
    (True, _) -> return False
    (_, True) -> return True
    _ -> retry

-- | Get the result of a computation, this blocks until the result is
-- present, so be careful not to lose parallelism.
result :: MonadConc m => WorkItem m a -> m (Maybe a)
result f = fmap (_mapped $ unWrap f) `liftM` res where
  res = atomically . readCTMVar . _result $ unWrap f

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

--------------------------------------------------------------------------------
-- Concurrency

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
      go [] = atomically $ putCTMVar res Nothing
      go (item:rest) = process item (putCTMVar res) (go rest)

    -- Fork off as many threads as there are capabilities, and queue
    -- up the remaining work.
    driver caps res = do
      remaining <- atomically $ newCTVar workitems
      tids      <- mapM (\cap -> forkOn cap $ worker res remaining) [1..caps]
      _         <- atomically $ readCTMVar res

      mapM_ killThread tids

    -- Grab the work item at the head of the list, process it, and
    -- then either store a successful result or loop. Terminate when
    -- there is no work left.
    worker res remaining = do
      witem <- steal remaining

      case witem of
        Just item ->
          process item (liftM (const ()) . tryPutCTMVar res) (worker res remaining)
        Nothing -> atomically $ const () `liftM` tryPutCTMVar res Nothing

    -- Process a work item and store the result if it is a success,
    -- otherwise continue.
    process item store continue = do
      fwrap  <- item
      maybea <- result fwrap

      case maybea of
        Just _  -> atomically $ store maybea
        Nothing -> continue

    -- Take the work item at the head of the list.
    steal remaining = atomically $ do
      remaining' <- readCTVar remaining

      case remaining' of
        (x:xs) -> do
          writeCTVar remaining xs
          return $ Just x
        [] -> return Nothing
