{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE Rank2Types         #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal where

import Control.Concurrent.STM.CTVar (modifyCTVar')
import Control.Concurrent.STM.CTMVar
import Control.Monad (liftM, unless,)
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

-- | A channel containing values of type @a@.
newtype Chan m a = Chan { unChan :: forall x. Chan' m x a }

-- | A channel containing values of type @x@, which will be
-- transformed to type @a@ when read.
data Chan' m x a
  = BuilderChan'
    { _builder :: m (Maybe (Maybe x))
    -- ^ Function to produce a new value.
    , _bmapped :: x -> a
    -- ^ The post-processing.
    }

  | Chan'
    { _closed    :: CTVar m Bool
    -- ^ Whether the channel has been closed or not.
    , _remaining :: CTVar m Int
    -- ^ Number of things to compute before pausing the threads.
    , _readHead  :: CTMVar m (CStream m x)
    -- ^ The read head of the channel.
    , _writeHead :: CTMVar m (CStream m x)
    -- ^ The write head of the channel.
    , _cmapped   :: x -> a
    -- ^ The post-processing.
    }

instance Functor (Chan m) where
  fmap f (Chan b@(BuilderChan' _ _)) = Chan $ b { _bmapped = f . _bmapped b }
  fmap f (Chan c) = Chan $ c { _cmapped = f . _cmapped c }

type CStream m x = CTMVar m (SItem m x)
data SItem m x = SItem x (CStream m x)

-- | Construct a 'Chan'.
chan :: Chan' m x a -> Chan m a
chan = wrap where
  wrap :: Chan' m x a -> Chan m a
  wrap = Chan . unsafeCoerce

-- | Construct a builder 'Chan'.
builderChan :: m (Maybe (Maybe x)) -> Chan m x
builderChan mx = chan $ BuilderChan' mx id

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
-- Processing streams

-- | Create a new channel.
newChan :: MonadSTM m => m (Chan' m x x)
newChan = do
  hole      <- newEmptyCTMVar
  readHead  <- newCTMVar hole
  writeHead <- newCTMVar hole
  closed    <- newCTVar False
  remaining <- newCTVar 100
  return $ Chan' closed remaining readHead writeHead id

-- | Get the head of a channel. @Nothing@ indicates that the
-- computation was paused and there was no result, but that it has
-- been resumed and should be tried again.
readChan :: MonadSTM m => Chan' m x a -> m (Maybe (Maybe a))
readChan b@(BuilderChan' _ _) = do
  x <- _builder b
  case x of
    Just x' -> return . Just $ _bmapped b `fmap` x'
    Nothing -> return Nothing

readChan c = do
  closed    <- readCTVar $ _closed c
  remaining <- readCTVar $ _remaining c
  -- If closed, stop here
  if closed
  then return $ Just Nothing
  else do
    stream <- takeCTMVar $ _readHead c
    item   <- tryTakeCTMVar stream
    -- Try reading an item, if there is one, return it. If paused,
    -- resume the computation and return an indication; otherwise,
    -- block until there is a result.
    case item of
      Just (SItem x rest) -> do
        putCTMVar (_readHead c) rest
        return . Just . Just $ _cmapped c x
      Nothing
        | remaining == 0 -> do
          putCTMVar (_readHead c) stream
          writeCTVar (_remaining c) 100
          return Nothing
        | otherwise -> retry

-- | Append to a channel. If the channel has been closed, this drops
-- writes.
writeChan :: MonadSTM m => Chan' m x a -> x -> m ()
writeChan (BuilderChan' _ _) _ = return ()
writeChan c x = do
  closed <- readCTVar $ _closed c
  unless closed $ do
    newHole <- newEmptyCTMVar
    oldHole <- takeCTMVar $ _writeHead c
    putCTMVar oldHole (SItem x newHole)
    putCTMVar (_writeHead c) newHole

-- | End a channel.
endChan :: MonadSTM m => Chan' m x a -> m ()
endChan (BuilderChan' _ _) = return ()
endChan c = writeCTVar (_closed c) True

--------------------------------------------------------------------------------
-- Work stealing

-- | Push a batch of work to the queue. If streaming, return a
-- channel, if not, return a 'CTMVar' that can be blocked on to get
-- the result, and an action to kill the computation.
work :: MonadConc m
     => Bool -> [m (WorkItem m a)] -> m (Either (CTMVar (STMLike m) (Maybe a), m ()) (Chan' (STMLike m) a a))
work streaming workitems = do
  kill <- atomically newEmptyCTMVar
  caps <- getNumCapabilities

  res <- if streaming
        then Right `liftM` atomically newChan
        else Left  `liftM` atomically newEmptyCTMVar

  dtid   <- fork $ driver caps res kill
  killme <- atomically $ readCTMVar kill

  return $ case res of
    Left  var -> Left  (var, killme >> killThread dtid)
    Right chn -> Right chn

  where
    -- If there's only one capability don't bother with threads.
    driver 1 res kill = do
      remaining <- newCRef workitems
      atomically . putCTMVar kill $ failit res
      process remaining res

    -- Fork off as many threads as there are capabilities, and queue
    -- up the remaining work.
    driver caps res kill = do
      remaining <- newCRef workitems
      tids <- mapM (\cap -> forkOn cap $ process remaining res) [1..caps]

      -- Construct an action to short-circuit the computation.
      atomically . putCTMVar kill $ failit res >> mapM_ killThread tids

      -- If not streaming, block until there is a result then kill any
      -- still-running threads.
      case res of
        Left ctmvar -> atomically (readCTMVar ctmvar) >> mapM_ killThread tids
        _ -> return ()

    -- Process a work item and store the result if it is a success,
    -- otherwise continue.
    process remaining res = do
      mitem <- modifyCRef remaining $ \rs -> if null rs then ([], Nothing) else (tail rs, Just $ head rs)
      case mitem of
        Just item -> do
          -- If streaming, and the buffer is full, block.
          atomically $ case res of
            Right chn -> readCTVar (_remaining chn) >>= check . (/= 0)
            _ -> return ()

          fwrap  <- item
          maybea <- result fwrap

          case (maybea, res) of
            (Just a, Right chn)  -> atomically (writeChan chn a >> modifyCTVar' (_remaining chn) (subtract 1)) >> process remaining res
            (Just a, Left  var) -> atomically . putCTMVar var $ Just a
            _ -> process remaining res
        Nothing -> failit res

    -- Record that a computation failed.
    failit (Left  var) = atomically $ const () `liftM` tryPutCTMVar var Nothing
    failit (Right chn) = atomically $ endChan chn
