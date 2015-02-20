module Main where

import Control.Concurrent.Find
import Control.Monad.Conc.Class (MonadConc)
import Control.Monad.Loops (andM)
import Data.List (foldl')
import System.Exit (exitSuccess, exitFailure)
import Test.DejaFu (dejafus, deadlocksNever, exceptionsNever, alwaysTrue)

main :: IO ()
main = do
  pass <- andM [trees]
  if pass then exitSuccess else exitFailure

-- | Lift a predicate over the result value to work on the test
-- output.
liftB :: (a -> Bool) -> Either z (Maybe a) -> Bool
liftB f (Right (Just a)) = f a
liftB _ _ = False

--------------------------------------------------------------------------------
-- (trees): Finding large binary trees

data BinTree = Leaf | Branch Int BinTree BinTree deriving Eq

foldTree :: a -> (Int -> a -> a -> a) -> BinTree -> a
foldTree z _ Leaf = z
foldTree z f (Branch i t1 t2) = f i (foldTree z f t1) (foldTree z f t2)

sumTree :: BinTree -> Int
sumTree = foldTree 0 (\i t1 t2 -> i + t1 + t2)

treeDepth :: BinTree -> Int
treeDepth = foldTree 0 (\_ t1 t2 -> 1 + max t1 t2)

stepTree :: BinTree -> BinTree
stepTree Leaf = Branch 1 Leaf Leaf
stepTree t@(Branch i _ _) = Branch (i+1) t t

trees :: IO Bool
trees = dejafus find cases where
  find :: MonadConc m => m (Maybe Int)
  find = runFind $ [0..] ! check

  check d = sumTree (foldl' (.) id (replicate d stepTree) Leaf) > 1000

  cases = [ ("Tree Summing (No Deadlocks)",  deadlocksNever)
          , ("Tree Summing (No Exceptions)", exceptionsNever)
          , ("Tree Summing (Valid Result)",  alwaysTrue $ liftB check)
          ]