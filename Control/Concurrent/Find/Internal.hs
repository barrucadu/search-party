{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE Rank2Types         #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal where

import Control.Concurrent.STM.CTMVar (CTMVar, newEmptyCTMVar, readCTMVar, isEmptyCTMVar, tryPutCTMVar)
import Control.Monad (liftM, unless)
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
  case (any (==HasFailed) states, any (==HasSucceeded) states) of
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
work :: MonadConc m => (x -> m (FWrap m a)) -> [x] -> m (CTMVar (STMLike m) (Maybe a))
work tofwrap workitems = do
  res <- atomically $ newEmptyCTMVar
  threadlim <- getNumCapabilities
  _ <- fork $ driver threadlim res [] workitems
  return res

  where
    driver threadlim res running rest = do
      -- If we're at the thread limit, block until some threads have
      -- terminated.
      bail <- if length running == threadlim
             then do
               ts <- atomically $ do
                 states <- mapM (\(v,t) -> readCTVar v >>= \v' -> return (v', t)) running
                 if any ((/=StillComputing) . fst) states
                 then return states
                 else retry

               -- If a thread succeded, kill all remaining threads.
               if (any ((==HasSucceeded) . fst) ts)
               then mapM_ (killThread . snd) running >> return True
               else return False
             else return False

      -- If we're still going, start new threads up to the limit and
      -- loop.
      unless bail $ do
        ts <- atomically $ mapM (\(v,t) -> readCTVar v >>= \v' -> return (v', t)) running
        let live = map snd $ filter ((==StillComputing) . fst) ts
        let (starting, rest') = splitAt (threadlim - length live) rest
        vars <- atomically $ mapM (const $ newCTVar StillComputing) starting
        tids <- mapM (\(w, v) -> fork $ go w v res) $ zip starting vars

        driver threadlim res (zip vars tids ++ filter (\(_,t) -> t `elem` live) running) rest'

    go workitem var res = do
      -- Force (blocks) and store the result.
      fwrap  <- tofwrap workitem
      maybea <- result fwrap
      case maybea of
        Just _ -> atomically $ do
          _ <- tryPutCTMVar res maybea
          writeCTVar var HasSucceeded

        Nothing -> atomically $ writeCTVar var HasFailed

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
    if failed
    then return HasFailed
    else return HasSucceeded

-- | Check if a computation has failed. If the computation has not
-- terminated, this immediately returns 'False'.
hasFailed :: MonadConc m => FWrap m a -> STMLike m Bool
hasFailed f = do
  working <- isEmptyCTMVar . _result $ unWrap f
  if working
  then return False
  else isNothing `liftM` readCTMVar (_result $ unWrap f)
