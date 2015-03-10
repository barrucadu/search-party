module Main where

import Control.Concurrent.Find
import Control.Monad.Conc.Class (MonadConc)
import Control.Monad.Loops (andM)
import Data.Char (ord)
import Data.List (foldl', nub, sort)
import Data.Maybe (listToMaybe)
import System.Exit (exitSuccess, exitFailure)
import Test.DejaFu (Predicate, dejafus, deadlocksNever, exceptionsNever, alwaysTrue)

import Debug.Trace

import qualified CountDown as C

main :: IO ()
main = do
  pass <- andM [ trees
              , countdown [16,8,3] 21
              --, hash 10 ["Bool", "Maybe", "Either", "Ordering", "Char", "String"]
              , streams1
              , streams2
              ]
  if pass then exitSuccess else exitFailure

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
trees = dejafus (runFind $ [0..] ! check) $ cases "trees" check where
  check d = sumTree (foldl' (.) id (replicate d stepTree) Leaf) > 1000

--------------------------------------------------------------------------------
-- (countdown): Hutton's Countdown program (Programming in Haskell, ch. 11)

countdown :: [Int] -> Int -> IO Bool
countdown ns n = dejafus (runFind $ solution ns n) $ cases "countdown" (\e -> C.solution e ns n)  where

-- 'solutions''' from CountDown.hs recast as a 'Find' computation.
solution :: MonadConc m => [Int] -> Int -> Find m C.Expr
solution ns n = C.choices ns ? soln where
  soln ns' = listToMaybe [e | (e,m) <- C.results' ns', m == n]

--------------------------------------------------------------------------------
-- (hash): Finding perfect hash functions.

hash :: Int -> [String] -> IO Bool
hash maxgap keywords = dejafus (runFind $ [(i,j,f,g,h) | i <- [0..maximum (map length keywords)], j <- [0..maximum (map length keywords)], f <- [1..32], g <- [1..32], h <- [1..32]] ! check) $ cases "hash" check where

  check h = perfect && positive && small where
    perfect  = length (nub hashvals) == length keywords
    positive = all (>=0) hashvals
    small    = maximum (zipWith subtract (sort hashvals) (tail $ sort hashvals)) <= maxgap
    hashvals = map (hash h) keywords

  hash (i,j,f,g,h) keyword = ceiling $ f'*char1 * g'*char2 * h'*len where
    char1 = fromIntegral . ord $ if i < length keyword then keyword !! i else '?'
    char2 = fromIntegral . ord $ if j < length keyword then keyword !! j else '?'
    len   = fromIntegral $ length keyword
    f'    = fromIntegral f / 16
    g'    = fromIntegral g / 16
    h'    = fromIntegral h / 16

--------------------------------------------------------------------------------
-- (streams1): Checking streams contain a result.

streams1 :: IO Bool
streams1 = dejafus (as @! const True >>= readStream) $ cases "streams1" (`elem` as) where
  as = [0,1]

--------------------------------------------------------------------------------
-- (streams2): Checking stream ordering works

streams2 :: IO Bool
streams2 = dejafus (as >! const True >>= fmap Just . takeStream 10) $ cases "streams2" (==as) where
  as = [0,1]

--------------------------------------------------------------------------------
-- Utility functions

-- | Test cases.
cases :: (Eq a, Show a) => String -> (a -> Bool) -> [(String, Predicate (Maybe a))]
cases name check =
  [ (prefix ++ "No Deadlocks ", deadlocksNever)
  , (prefix ++ "No Exceptions", exceptionsNever)
  , (prefix ++ "Valid Result ", alwaysTrue $ liftB check)
  ]

  where prefix = "(" ++ name ++ ")" ++ replicate (10 - length name) ' '

-- | Lift a predicate over the result value to work on the test
-- output.
liftB :: (a -> Bool) -> Either z (Maybe a) -> Bool
liftB f (Right (Just a)) = f a
liftB _ _ = False
