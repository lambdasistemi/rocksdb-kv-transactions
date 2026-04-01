{- |
Shared test helpers for running tests against both RocksDB and in-memory backends.
-}
module Database.KV.TestDatabase
    ( -- * Column definitions
      Tables (..)
    , bsCodec
    , codecs

      -- * Backend runners
    , withRocksDB
    , withInMemory

      -- * Backend-polymorphic helpers
    , BackendRunner
    , forBothBackends
    )
where

import Data.ByteString (ByteString)
import Data.Default (Default (..))
import Data.Type.Equality ((:~:) (..))
import Database.KV.Database (Database, mkColumns)
import Database.KV.InMemory (mkInMemoryDatabase)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( Codecs (..)
    , DMap
    , DSum (..)
    , GCompare (..)
    , GEq (..)
    , GOrdering (..)
    , KV
    , fromPairList
    )
import Database.RocksDB
    ( Config (createIfMissing)
    , DB (..)
    , withDBCF
    )
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec (Spec, describe)

-- | Codecs for ByteString key-value pairs (identity encoding)
bsCodec :: Codecs (KV ByteString ByteString)
bsCodec = Codecs{keyCodec = id, valueCodec = id}

-- | GADT to select between different tables
data Tables a where
    Items :: Tables (KV ByteString ByteString)

instance GCompare Tables where
    gcompare Items Items = GEQ

instance GEq Tables where
    geq Items Items = Just Refl

-- | Index codecs by table type
codecs :: DMap Tables Codecs
codecs = fromPairList [Items :=> bsCodec]

-- | A function that provides a Database to a test action.
type BackendRunner =
    forall a. (forall cf op. Database IO cf Tables op -> IO a) -> IO a

-- | Provide a RocksDB-backed database.
withRocksDB :: BackendRunner
withRocksDB action =
    withSystemTempDirectory "test-db" $ \fp ->
        withDBCF fp cfg [("items", cfg)] $ \db ->
            action $ mkRocksDBDatabase db $ mkColumns (columnFamilies db) codecs
  where
    cfg = def{createIfMissing = True}

-- | Provide an in-memory database.
withInMemory :: BackendRunner
withInMemory action = do
    db <- mkInMemoryDatabase $ mkColumns [0 ..] codecs
    action db

-- | Run a spec against both backends.
forBothBackends :: String -> (BackendRunner -> Spec) -> Spec
forBothBackends name mkSpec = describe name $ do
    describe "RocksDB" $ mkSpec withRocksDB
    describe "InMemory" $ mkSpec withInMemory
