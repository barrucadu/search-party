{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE Rank2Types         #-}

-- | The guts of the Find monad.
module Control.Concurrent.Find.Internal
  (  -- * Work Items
    WorkItem
  , workItem
  , workItem'

  -- * Channels
  , Chan
  , builderChan
  , readChan
  , closedChan

  -- * Processing work items
  , result
  , unsafeResult
  , blockOn

  -- * Work stealing
  , work
  ) where

import Control.Concurrent.Classy.STM.TVar (modifyTVar')
import Control.Concurrent.Classy.STM.TMVar
import Control.DeepSeq (force)
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
  { _result :: TMVar (STM m) (Maybe x)
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
workItem :: TMVar (STM m) (Maybe x) -> (x -> a) -> m () -> WorkItem m a
workItem res mapp kill = wrap $ WorkItem' res mapp kill where
  -- Really not nice, but I have had difficulty getting GHC to unify
  -- @WorkItem' m x a@ with @forall x. WorkItem' m x a@
  --
  -- This needs ImpredicativeTypes in GHC 7.8.
  wrap :: WorkItem' m x a -> WorkItem m a
  wrap = WorkItem . unsafeCoerce

-- | Construct a 'WorkItem' containing a result.
workItem' :: MonadConc m => Maybe a -> m (WorkItem m a)
workItem' a = (\v -> workItem v id $ return ()) `liftM` atomically (newTMVar a)

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
  res = atomically . readTMVar . _result $ unWrap f

-- | Unsafe version of 'result', this will error at runtime if the
-- computation fails.
unsafeResult :: MonadConc m => WorkItem m a -> m a
unsafeResult = liftM fromJust . result

-- | Get the current state of a work item.
getState :: MonadConc m => WorkItem m a -> STM m WorkState
getState f = do
  empty <- isEmptyTMVar . _result $ unWrap f
  if empty
  then return StillComputing
  else do
    failed <- hasFailed f
    return $ if failed then HasFailed else HasSucceeded

-- | Check if a work item has failed. If the computation has not
-- terminated, this immediately returns 'False'.
hasFailed :: MonadConc m => WorkItem m a -> STM m Bool
hasFailed f = do
  working <- isEmptyTMVar . _result $ unWrap f
  if working
  then return False
  else isNothing `liftM` readTMVar (_result $ unWrap f)

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
    { _closed    :: TVar m Bool
    -- ^ Whether the channel has been closed or not.
    , _var       :: TMVar m x
    -- ^ Output variable.
    , _unget     :: TVar m (Maybe x)
    -- ^ Allows undoing a single read.
    , _cmapped   :: x -> a
    -- ^ The post-processing.
    }

instance Functor (Chan' m x) where
  fmap f b@(BuilderChan' _ _) = b { _bmapped = f . _bmapped b }
  fmap f c = c { _cmapped = f . _cmapped c }

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
  empty <- newEmptyTMVar
  c <- newChan' empty
  endChan' c
  return $ chan c

--------------------------------------------------------------------------------
-- Processing channels

-- | Create a new channel.
newChan' :: MonadSTM m => TMVar m x -> m (Chan' m x x)
newChan' var = do
  closed <- newTVar False
  unget  <- newTVar Nothing
  return $ Chan' closed var unget id

-- | Get the head of a channel, and an action to undo this read
-- ('BuilderChan's cannot have reads undone). @Nothing@ indicates that
-- the computation was paused and there was no result, but that it has
-- been resumed and should be tried again.
readChan :: MonadSTM m => Chan m a -> m (Maybe (a, m ()))
readChan (Chan b@(BuilderChan' _ _)) = do
  item <- _builder b
  return $ (\x -> (_bmapped b x, return ())) `fmap` item
readChan (Chan c) = do
  ung <- readTVar $ _unget c

  case ung of
    -- First check if the last read was undone
    Just x  -> do
      writeTVar (_unget c) Nothing
      return $ Just (_cmapped c x, writeTVar (_unget c) ung)

    -- Then try to read from the TMVars in turn
    Nothing -> do
      closed <- readTVar     $ _closed c
      res    <- tryTakeTMVar $ _var c

      case res of
        -- Return the value read
        Just x -> return $ Just (_cmapped c x, writeTVar (_unget c) res)

        -- If not closed, block.
        Nothing -> check closed >> return Nothing

-- | End a channel.
endChan' :: MonadSTM m => Chan' m x a -> m ()
endChan' (BuilderChan' _ _) = return ()
endChan' c = writeTVar (_closed c) True

--------------------------------------------------------------------------------
-- Work stealing

-- | Push a batch of work to the queue.
-- 
-- If streaming, return a channel, if not, return a 'TMVar' that can
-- be blocked on to get the result, and an action to kill the
-- computation.
--
-- If in order, return results in the order in which they appear in
-- the work item list.

work :: MonadConc m
     => Bool -> Bool -> [m (WorkItem m a)] -> m (Either (TMVar (STM m) (Maybe a), m ()) (Chan (STM m) a))
work streaming inorder workitems = do
  caps <- getNumCapabilities

  -- Create the workers and the output variable(s)
  liveworkers <- atomically $ newTVar caps
  (explorers, res) <- atomically $ do
    remaining   <- newTVar $ zip workitems [0..]
    currently   <- newTVar []

    if streaming
    then do
      var <- newEmptyTMVar
      chn <- newChan' var
      let explorers = map (\_ -> explorer True inorder caps remaining id currently liveworkers var) [1..caps]
      return (explorers, Right chn)

    else do
      var <- newEmptyTMVar
      let explorers = map (\_ -> explorer False inorder caps remaining Just currently liveworkers var) [1..caps]
      return (explorers, Left var)

  ctids <- atomically newEmptyTMVar
  dtid  <- fork $ driver explorers res liveworkers ctids
  ctids <- atomically $ readTMVar ctids

  return $ case res of
    Left  var -> Left  (var, mapM_ killThread ctids >> failit res >> killThread dtid)
    Right chn -> Right $ chan chn

  where
    -- If there's only one capability don't bother with threads.
    driver [explorer] res _ ctids = do
      atomically $ putTMVar ctids []
      explorer
      failit res

    -- Fork off as many threads as there are capabilities, and queue
    -- up the remaining work.
    driver explorers res liveworkers ctids = do
      tids <- mapM (\(explorer, cap) -> forkOn cap explorer) $ zip explorers [0..]

      -- Construct an action to short-circuit the computation.
      atomically $ putTMVar ctids tids

      -- Block until there is a result, or every thread terminates,
      -- and then clean up.
      atomically $ do
        gotres <- case res of
                   Left tmvar -> tryReadTMVar tmvar
                   _ -> return Nothing
        live <- readTVar liveworkers
        check $ live == 0 || isJust gotres

      failit res
      mapM_ killThread tids

    -- Record that a computation failed.
    failit (Left  var) = atomically $ const () `liftM` tryPutTMVar var Nothing
    failit (Right chn) = atomically $ endChan' chn

-- | A member of the search party, works through a queue of
-- 'WorkItem's, storing successful results and terminating when there
-- is nothing left to do.
explorer :: MonadConc m
         => Bool
         -> Bool
         -> Int
         -> TVar  (STM m) [(m (WorkItem m a), Int)]
         -> (a -> b)
         -> TVar  (STM m) [(Int, Int)]
         -> TVar  (STM m) Int
         -> TMVar (STM m) b
         -> m ()
-- If there is only one explorer, skip all the synchronisation stuff.
explorer looping _ 1 workitems wf _ liveworkers var = do
  work <- atomically (readTVar workitems)
  loop work
  atomically $ writeTVar liveworkers 0

  where
    loop ((item,_):rest) = item >>= result >>= maybe (loop rest) (\a -> do
      atomically . putTMVar var $ wf a
      when looping $ loop rest)

    loop [] = return ()

explorer looping inorder caps workitems wf currently liveworkers var = loop ([1..caps] ++ repeat caps) [] where
  -- Claim some more work items atomically and then process them. This
  -- is done rather than claim a single work item from the shared
  -- queue every iteration to reduce contention, which drastically
  -- improves performance with many threads.
  --
  -- 'n' items are claimed, where 'n' is 1..caps, starting at 1 and
  -- incrementing each time.
  loop (n:ns) [] = do
    items <- atomically $ do
      witems <- readTVar workitems

      case splitAt n witems of
        -- If there are no items, give up
        ([], [])     -> return Nothing

        -- Otherwise, write the remaining items back to the shared
        -- queue and return the claimed ones.
        (mine, rest) -> do
          writeTVar workitems rest
          -- Indicate to all threads the range of work items we now
          -- have, if producing results in order.
          when inorder $ modifyTVar'' currently (\is -> (snd $ head mine, snd $ last mine) : is)
          return $ Just mine

    maybe (atomically retire) (loop ns) items

  loop n ((item, idx):rest) = do
    -- Compute the result
    fwrap  <- item
    maybea <- result fwrap

    case maybea of
      Just a -> do
        atomically $ do
          -- If in order, block until there are no workers processing
          -- an earlier work item.
          when inorder $ readTVar currently >>= check . all ((>=idx) . fst)

          -- Store the result and indicate it's done
          putTMVar var (wf a)
          when inorder $ unclaim idx

          -- Retire the explorer if we're not looping
          unless looping retire

        -- If looping, loop.
        when looping $ loop n rest
      _ -> atomically (when inorder $ unclaim idx) >> loop n rest

  -- Record that the current work item is done.
  unclaim idx = modifyTVar'' currently $ map (\(l,h) -> if l == idx then (l+1,h) else (l,h)) . filter ((==idx) . snd)

  -- Record that the explorer is done.
  retire = modifyTVar' liveworkers $ subtract 1

  -- Really strict 'modifyTVar'
  modifyTVar'' tvar = modifyTVar' tvar . force
