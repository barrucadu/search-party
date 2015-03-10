{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE Rank2Types         #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal where

import Control.Concurrent.STM.CTVar (modifyCTVar')
import Control.Concurrent.STM.CTMVar
import Control.Monad (liftM, unless, when)
import Control.Monad.Conc.Class
import Control.Monad.STM.Class
import Data.Maybe (fromJust, isJust, isNothing)
import Unsafe.Coerce (unsafeCoerce)

--------------------------------------------------------------------------------
-- Work Items

-- | A unit of work in a monad @m@ which will produce a final result
-- of type @a@.
newtype WorkItem m a = WorkItem { unWrap :: forall x. WorkItem' m x a }

instance Functor (WorkItem m) where
  fmap f = wrap . fmap f . unWrap where
    wrap = WorkItem . unsafeCoerce

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

instance Functor (WorkItem' m x) where
  fmap f w = w { _mapped = f . _mapped w }

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
-- Channels

-- | A channel containing values of type @a@.
newtype Chan m a = Chan { unChan :: forall x. Chan' m x a }

instance Functor (Chan m) where
  fmap f = chan . fmap f . unChan

-- | A channel containing values of type @x@, which will be
-- transformed to type @a@ when read.
data Chan' m x a
  = BuilderChan'
    { _builder :: m (Maybe x)
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

instance Functor (Chan' m x) where
  fmap f b@(BuilderChan' _ _) = b { _bmapped = f . _bmapped b }
  fmap f c = c { _cmapped = f . _cmapped c }

type CStream m x = CTMVar m (SItem m x)
data SItem m x = SItem x (CStream m x)

-- | Construct a 'Chan'.
chan :: Chan' m x a -> Chan m a
chan = wrap where
  wrap :: Chan' m x a -> Chan m a
  wrap = Chan . unsafeCoerce

-- | Construct a builder 'Chan'.
builderChan :: m (Maybe x) -> Chan m x
builderChan mx = chan $ BuilderChan' mx id

-- | Construct a new closed 'Chan'.
closedChan :: MonadSTM m => m (Chan m a)
closedChan = do
  c <- newChan'
  endChan' c
  return $ chan c

--------------------------------------------------------------------------------
-- Processing channels

-- | Create a new channel.
newChan' :: MonadSTM m => m (Chan' m x x)
newChan' = do
  hole      <- newEmptyCTMVar
  readHead  <- newCTMVar hole
  writeHead <- newCTMVar hole
  closed    <- newCTVar False
  remaining <- newCTVar 100
  return $ Chan' closed remaining readHead writeHead id

-- | Get the head of a channel, and an action to undo this
-- read. @Nothing@ indicates that the computation was paused and there
-- was no result, but that it has been resumed and should be tried
-- again.
readChan :: MonadSTM m => Chan m a -> m (Maybe (a, m ()))
readChan (Chan b@(BuilderChan' _ _)) = do
  item <- _builder b
  return $ (\x -> (_bmapped b x, return ())) `fmap` item
readChan (Chan c) = do
  closed    <- readCTVar $ _closed c
  remaining <- readCTVar $ _remaining c

  -- Try to take an item from the stream.
  stream <- takeCTMVar $ _readHead c
  mitem  <- tryTakeCTMVar stream
  case mitem of
    Just (item@(SItem x rest)) -> do
      putCTMVar (_readHead c) rest

      -- Maintain the buffer
      when (remaining < 50) $ writeCTVar (_remaining c) 100

      return $ Just (_cmapped c x, newCTMVar item >>= putCTMVar (_readHead c))

    -- If there's nothing there, retry if the stream hasn't been
    -- closed.
    Nothing -> check closed >> putCTMVar (_readHead c) stream >> return Nothing

-- | Append to a channel. If the channel has been closed, this drops
-- writes.
writeChan' :: MonadSTM m => Chan' m x a -> x -> m ()
writeChan' (BuilderChan' _ _) _ = return ()
writeChan' c x = do
  closed <- readCTVar $ _closed c
  unless closed $ do
    newHole <- newEmptyCTMVar
    oldHole <- takeCTMVar $ _writeHead c
    putCTMVar oldHole (SItem x newHole)
    putCTMVar (_writeHead c) newHole
    modifyCTVar' (_remaining c) $ subtract 1

-- | End a channel.
endChan' :: MonadSTM m => Chan' m x a -> m ()
endChan' (BuilderChan' _ _) = return ()
endChan' c = writeCTVar (_closed c) True

--------------------------------------------------------------------------------
-- Work stealing

-- | Push a batch of work to the queue.
-- 
-- If streaming, return a channel, if not, return a 'CTMVar' that can
-- be blocked on to get the result, and an action to kill the
-- computation.
--
-- If in order, return results in the order in which they appear in
-- the work item list.

work :: MonadConc m
     => Bool -> Bool -> [m (WorkItem m a)] -> m (Either (CTMVar (STMLike m) (Maybe a), m ()) (Chan (STMLike m) a))
work streaming inorder workitems = do
  kill <- atomically newEmptyCTMVar
  caps <- getNumCapabilities
  res <- if streaming
        then Right `liftM` atomically newChan'
        else Left  `liftM` atomically newEmptyCTMVar
  dtid   <- fork $ driver caps res kill
  killme <- atomically $ readCTMVar kill
  return $ case res of
    Left  var -> Left  (var, killme >> killThread dtid)
    Right chn -> Right $ chan chn

  where
    -- If there's only one capability don't bother with threads.
    driver 1 res kill = do
      remaining <- atomically . newCTVar $ zip workitems ([0..] :: [Int])
      currently <- atomically $ newCTVar []
      liveworkers <- atomically $ newCTVar (1::Int)
      atomically . putCTMVar kill $ failit res
      process remaining currently liveworkers res
      failit res

    -- Fork off as many threads as there are capabilities, and queue
    -- up the remaining work.
    driver caps res kill = do
      remaining <- atomically . newCTVar $ zip workitems ([0..] :: [Int])
      currently <- atomically $ newCTVar []
      liveworkers <- atomically $ newCTVar caps
      tids <- mapM (\cap -> forkOn cap $ process remaining currently liveworkers res) [1..caps]

      -- Construct an action to short-circuit the computation.
      atomically . putCTMVar kill $ failit res >> mapM_ killThread tids

      -- Block until there is a result, or every thread terminates,
      -- and then clean up.
      atomically $ do
        gotres <- case res of
                   Left ctmvar -> tryReadCTMVar ctmvar
                   _ -> return Nothing
        live <- readCTVar liveworkers
        check $ live == 0 || fmap isJust gotres == Just True

      failit res
      mapM_ killThread tids

    -- Process a work item and store the result if it is a success,
    -- otherwise continue.
    process remaining currently liveworkers res = do
      mitem <- atomically $ do
        work <- readCTVar remaining
        case work of
          (w@(_, idx):ws) -> do
            writeCTVar remaining ws
            when inorder $ modifyCTVar' currently (idx:)
            return $ Just w
          [] -> return Nothing

      case mitem of
        Just (item, idx) -> do
          -- If streaming, and the buffer is full, block.
          atomically $ case res of
            Right chn -> readCTVar (_remaining chn) >>= check . (/= 0)
            _ -> return ()

          -- Compute the result
          fwrap  <- item
          maybea <- result fwrap

          -- Indicate that the work item has been stored in the result
          -- variable, and so workers with later syccessful work items
          -- can continue.
          let finish = when inorder . modifyCTVar' currently $ filter (/=idx)

          case maybea of
            Just a -> do
              -- If in order, block until there are no workers
              -- processing an earlier work item.
              when inorder . atomically $ readCTVar currently >>= check . all (>=idx)

              -- Store the result
              case res of
                Right chn -> atomically (writeChan' chn a >> finish) >> process remaining currently liveworkers res
                Left  var -> atomically $ putCTMVar var (Just a) >> finish
            _ -> atomically finish >> process remaining currently liveworkers res
        Nothing -> atomically . modifyCTVar' liveworkers $ subtract 1

    -- Record that a computation failed.
    failit (Left  var) = atomically $ const () `liftM` tryPutCTMVar var Nothing
    failit (Right chn) = atomically $ endChan' chn
