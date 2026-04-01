{- |
QuickCheck property tests proving in-memory and RocksDB backends
produce identical final states for any sequence of operations.
-}
module Database.KV.EquivalenceSpec (spec) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Default (Default (..))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Type.Equality ((:~:) (..))
import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , nextEntry
    )
import Database.KV.Database (Database (..), mkColumns)
import Database.KV.InMemory (mkInMemoryDatabase)
import Database.KV.Query (interpretQuerying, iterating)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( Codecs (..)
    , DMap
    , DSum (..)
    , GCompare (..)
    , GEq (..)
    , GOrdering (..)
    , KV
    , delete
    , fromPairList
    , insert
    , query
    , runTransactionUnguarded
    )
import Database.RocksDB
    ( Config (createIfMissing)
    , DB (..)
    , withDBCF
    )
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec (Spec, describe, it)
import Test.QuickCheck
    ( Arbitrary (..)
    , Gen
    , choose
    , oneof
    , property
    , vectorOf
    )
import Test.QuickCheck.Monadic (assert, monadicIO, run)

-- | Column definitions (shared)
data Tables a where
    Items :: Tables (KV ByteString ByteString)

instance GCompare Tables where
    gcompare Items Items = GEQ

instance GEq Tables where
    geq Items Items = Just Refl

bsCodec :: Codecs (KV ByteString ByteString)
bsCodec = Codecs{keyCodec = id, valueCodec = id}

codecs :: DMap Tables Codecs
codecs = fromPairList [Items :=> bsCodec]

cfg :: Config
cfg = def{createIfMissing = True}

-- | A single database action
data DatabaseAction
    = Put ByteString ByteString
    | Delete ByteString
    deriving stock (Show)

-- | Generate short random ByteStrings (1-8 bytes, printable for readability)
genShortBS :: Gen ByteString
genShortBS = do
    len <- choose (1, 8 :: Int)
    bytes <- vectorOf len (choose (0x61, 0x7a)) -- lowercase ascii
    pure $ BS.pack $ map fromIntegral (bytes :: [Int])

-- | Wrapper for ByteString with Arbitrary instance
newtype ShortBS = ShortBS {unShortBS :: ByteString}
    deriving newtype (Show, Eq, Ord)

instance Arbitrary ShortBS where
    arbitrary = ShortBS <$> genShortBS

instance Arbitrary DatabaseAction where
    arbitrary =
        oneof
            [ Put <$> genShortBS <*> genShortBS
            , Delete <$> genShortBS
            ]

-- | Apply actions to a database via the transaction layer
applyActions
    :: Database IO cf Tables op
    -> [DatabaseAction]
    -> IO ()
applyActions db actions =
    runTransactionUnguarded db $ mapM_ applyOne actions
  where
    applyOne (Put k v) = insert Items k v
    applyOne (Delete k) = delete Items k

-- | Scan all entries from a database via iterator, returns sorted pairs
scanAll
    :: Database IO cf Tables op
    -> IO [(ByteString, ByteString)]
scanAll db = interpretQuerying db $ iterating Items go
  where
    go = do
        mEntry <- firstEntry
        case mEntry of
            Nothing -> pure []
            Just Entry{entryKey, entryValue} -> do
                rest <- goNext
                pure $ (entryKey, entryValue) : rest
    goNext = do
        mEntry <- nextEntry
        case mEntry of
            Nothing -> pure []
            Just Entry{entryKey, entryValue} -> do
                rest <- goNext
                pure $ (entryKey, entryValue) : rest

-- | Read specific keys from a database
readKeys
    :: Database IO cf Tables op
    -> [ByteString]
    -> IO (Map ByteString (Maybe ByteString))
readKeys db keys =
    runTransactionUnguarded db $ do
        pairs <- mapM (\k -> (k,) <$> query Items k) keys
        pure $ Map.fromList pairs

-- | Run an action against both backends and return both results
withBothBackends
    :: ( forall cf op
          . Database IO cf Tables op
         -> IO a
       )
    -> IO (a, a)
withBothBackends action = do
    -- In-memory
    memDB <- mkInMemoryDatabase $ mkColumns [0 ..] codecs
    memResult <- action memDB

    -- RocksDB
    rocksResult <- withSystemTempDirectory "equiv-test" $ \fp ->
        withDBCF fp cfg [("items", cfg)] $ \db -> do
            let rocksDB = mkRocksDBDatabase db $ mkColumns (columnFamilies db) codecs
            action rocksDB

    pure (memResult, rocksResult)

spec :: Spec
spec = describe "Database.KV.Equivalence" $ do
    it
        "prop_equivalentFinalState: same final state after random operations"
        $ property
        $ \(actions :: [DatabaseAction]) -> monadicIO $ do
            (memEntries, rocksEntries) <- run $ withBothBackends $ \db -> do
                applyActions db actions
                scanAll db
            assert $ memEntries == rocksEntries

    it
        "prop_equivalentPointReads: same point reads after random operations"
        $ property
        $ \(actions :: [DatabaseAction]) (keys :: [ShortBS]) -> monadicIO $ do
            let keysToRead = map unShortBS keys
            (memReads, rocksReads) <- run $ withBothBackends $ \db -> do
                applyActions db actions
                readKeys db keysToRead
            assert $ memReads == rocksReads

    it "prop_equivalentSnapshotReads: snapshots isolate from later writes"
        $ property
        $ \(preActions :: [DatabaseAction]) (postActions :: [DatabaseAction]) (keys :: [ShortBS]) ->
            monadicIO $ do
                let keysToRead = map unShortBS keys
                (memResult, rocksResult) <- run $ withBothBackends $ \db -> do
                    applyActions db preActions
                    withSnapshot db $ \snapDB -> do
                        applyActions db postActions
                        readKeys snapDB keysToRead
                assert $ memResult == rocksResult

    it "prop_equivalentIteratorOrder: identical iteration order"
        $ property
        $ \(actions :: [DatabaseAction]) -> monadicIO $ do
            (memEntries, rocksEntries) <- run $ withBothBackends $ \db -> do
                applyActions db actions
                scanAll db
            assert $ memEntries == rocksEntries
