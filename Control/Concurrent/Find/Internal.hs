{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE Rank2Types         #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal where

import Control.Concurrent.STM.CTMVar (CTMVar, newEmptyCTMVar, newCTMVar, readCTMVar, isEmptyCTMVar, putCTMVar, tryPutCTMVar, tryTakeCTMVar)
import Control.Monad (liftM, unless)
import Control.Monad.Conc.Class
import Control.Monad.STM.Class
import Data.Maybe (fromJust, isNothing)
import Unsafe.Coerce (unsafeCoerce)

--------------------------------------------------------------------------------
-- Types

-- | A unit of work in a monad @m@ which will produce a final result
-- of type @a@.
newtype WorkItem m a = WorkItem { unWrap :: forall x. WorkItem' m x a }

instance Functor (WorkItem m) where
  fmap f (WorkItem w) = workItem (_result w) (f . _mapped w) (_killme w)

-- | A unit of work in a monad @m@ producing a result of type @x@,
-- which will then be transformed into a value of type @a@.
data WorkItem' m x a = WorkItem'
  { _result :: CTMVar (STMLike m) (Maybe x)
  -- ^ The future result of the computation.
  , _mapped :: x -> a
  -- ^ Some post-processing to do.
  , _killme :: m ()
  -- ^ Fail the computation, if it's still running.
  }

-- | The possible states that a work item may be in.
data WorkState = StillComputing | HasFailed | HasSucceeded
  deriving (Eq)

-- | Construct a 'WorkItem'.
workItem :: CTMVar (STMLike m) (Maybe x) -> (x -> a) -> m () -> WorkItem m a
workItem res mapp kill = wrap $ WorkItem' res mapp kill where
  -- Really not nice, but I have had difficulty getting GHC to unify
  -- @WorkItem' m x a@ with @forall x. WorkItem' m x a@
  --
  -- This needs ImpredicativeTypes in GHC 7.8.
  wrap :: WorkItem' m x a -> WorkItem m a
  wrap = WorkItem . unsafeCoerce

-- | Construct a 'WorkItem' containing a result.
workItem' :: MonadConc m => Maybe a -> m (WorkItem m a)
workItem' a = (\v -> workItem v id $ return ()) `liftM` atomically (newCTMVar a)

--------------------------------------------------------------------------------
-- Processing work items

-- | Block until all computations interested in have successfully
-- completed. If any fail, this immediately returns 'False' and kills
-- the still-running ones.
blockOn :: MonadConc m => [WorkItem m ()] -> m Bool
blockOn fs = do
  -- Block until one thing fails, or everything succeeds.
  success <- atomically $ do
    states <- mapM getState fs
    case (HasFailed `elem` states, all (==HasSucceeded) states) of
      (True, _) -> return False
      (_, True) -> return True
      _ -> retry

  -- Kill everything if something failed.
  unless success $ mapM_ (_killme . unWrap) fs

  return success

-- | Get the result of a computation, this blocks until the result is
-- present, so be careful not to lose parallelism.
result :: MonadConc m => WorkItem m a -> m (Maybe a)
result f = fmap (_mapped $ unWrap f) `liftM` res where
  res = atomically . readCTMVar . _result $ unWrap f

-- | Unsafe version of 'result', this will error at runtime if the
-- computation fails.
unsafeResult :: MonadConc m => WorkItem m a -> m a
unsafeResult = liftM fromJust . result

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
-- Work stealing

-- | Push a batch of work to the queue, returning a 'CTMVar' that can
-- be blocked on to get the result, and an action that can be used to
-- kill the computation. As soon as one succeeds, the others are
-- killed.
work :: MonadConc m => [m (WorkItem m a)] -> m (CTMVar (STMLike m) (Maybe a), m ())
work workitems = do
  res    <- atomically newEmptyCTMVar
  kill   <- atomically newEmptyCTMVar
  caps   <- getNumCapabilities
  dtid   <- fork $ driver caps res kill
  killme <- atomically $ readCTMVar kill

  return (res, killme >> killThread dtid)

  where
    -- If there's only one capability don't bother with threads.
    driver 1 res kill = do
      atomically . putCTMVar kill $ failit res
      process workitems res

    -- Fork off as many threads as there are capabilities, and queue
    -- up the remaining work.
    driver caps res kill = do
      -- Given a cap, get the work for that cap.
      let remaining cap = map (!!cap) $ filter (\xs -> length xs >= cap) $ chunks caps workitems

      -- Fork off chunks of work onto separate caps
      tids <- mapM (\cap -> forkOn cap $ process (remaining cap) res) [0..caps-1]

      -- Construct an action to short-circuit the computation.
      atomically . putCTMVar kill $ failit res >> mapM_ killThread tids

      -- Block until there is a result then kill any still-running
      -- threads.
      atomically (readCTMVar res) >> mapM_ killThread tids

    -- Split a list into chunks.
    chunks i xs = map (take i) $ splitter xs (:) [] where
      splitter [] _ n = n
      splitter l c n = l `c` splitter (drop i l) c n

    -- Process a work item and store the result if it is a success,
    -- otherwise continue.
    process [] res = failit res
    process (item:rest) res = do
      fwrap  <- item
      maybea <- result fwrap

      case maybea of
        Just _  -> atomically $ const () `liftM` (tryTakeCTMVar res >> putCTMVar res maybea)
        Nothing -> process rest res

    -- Record that a computation failed.
    failit res = atomically $ const () `liftM` tryPutCTMVar res Nothing