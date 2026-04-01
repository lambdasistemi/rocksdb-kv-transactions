{- |
Tests for Database.KV.Transaction — runs against both RocksDB and in-memory backends.
-}
module Database.KV.TransactionSpec (spec) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
    ( newEmptyMVar
    , putMVar
    , takeMVar
    )
import Data.ByteString (ByteString)
import Data.Type.Equality ((:~:) (..))
import Database.KV.Database
    ( mkColumns
    , prefixDatabase
    )
import Database.KV.InMemory (mkInMemoryDatabase)
import Database.KV.TestDatabase
    ( Tables (..)
    , bsCodec
    , forBothBackends
    )
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
    , mapColumns
    , query
    , runSpeculation
    , runTransactionUnguarded
    )
import Test.Hspec (Spec, describe, it, shouldBe)

-- | Sub-column GADT A (for mapColumns tests)
data SubA a where
    ItemsA :: SubA (KV ByteString ByteString)

instance GCompare SubA where
    gcompare ItemsA ItemsA = GEQ

instance GEq SubA where
    geq ItemsA ItemsA = Just Refl

-- | Sub-column GADT B (for mapColumns tests)
data SubB a where
    ItemsB :: SubB (KV ByteString ByteString)

instance GCompare SubB where
    gcompare ItemsB ItemsB = GEQ

instance GEq SubB where
    geq ItemsB ItemsB = Just Refl

-- | Unified column GADT combining SubA and SubB
data AllCols a where
    InA :: SubA a -> AllCols a
    InB :: SubB a -> AllCols a

instance GEq AllCols where
    geq (InA a) (InA a') = geq a a'
    geq (InB b) (InB b') = geq b b'
    geq _ _ = Nothing

instance GCompare AllCols where
    gcompare (InA a) (InA a') = gcompare a a'
    gcompare (InA _) (InB _) = GLT
    gcompare (InB _) (InA _) = GGT
    gcompare (InB b) (InB b') = gcompare b b'

-- | Codecs for unified columns
allCodecs :: DMap AllCols Codecs
allCodecs =
    fromPairList
        [ InA ItemsA :=> bsCodec
        , InB ItemsB :=> bsCodec
        ]

spec :: Spec
spec = do
    forBothBackends "Database.KV.Transaction" $ \withDB -> do
        describe "query" $ do
            it "returns Nothing for non-existent key" $ do
                result <- withDB $ \db ->
                    runTransactionUnguarded db $ query Items "nonexistent"
                result `shouldBe` Nothing

            it "returns Just value for existing key" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "key1" "value1"
                    runTransactionUnguarded db $ query Items "key1"
                result `shouldBe` Just "value1"

        describe "insert" $ do
            it "inserts a new key-value pair" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "apple" "red"
                    runTransactionUnguarded db $ query Items "apple"
                result `shouldBe` Just "red"

            it "overwrites existing value" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "fruit" "apple"
                    runTransactionUnguarded db $ insert Items "fruit" "banana"
                    runTransactionUnguarded db $ query Items "fruit"
                result `shouldBe` Just "banana"

            it "multiple inserts in same transaction" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ do
                        insert Items "a" "alpha"
                        insert Items "b" "beta"
                        insert Items "c" "gamma"
                    runTransactionUnguarded db $ do
                        a <- query Items "a"
                        b <- query Items "b"
                        c <- query Items "c"
                        pure (a, b, c)
                result `shouldBe` (Just "alpha", Just "beta", Just "gamma")

        describe "delete" $ do
            it "removes existing key" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "toDelete" "value"
                    runTransactionUnguarded db $ delete Items "toDelete"
                    runTransactionUnguarded db $ query Items "toDelete"
                result `shouldBe` Nothing

            it "deleting non-existent key is no-op" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ delete Items "never-existed"
                    runTransactionUnguarded db $ query Items "never-existed"
                result `shouldBe` Nothing

        describe "transaction atomicity" $ do
            it "applies all operations atomically" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ do
                        insert Items "x" "1"
                        insert Items "y" "2"
                        insert Items "z" "3"
                    runTransactionUnguarded db $ do
                        x <- query Items "x"
                        y <- query Items "y"
                        z <- query Items "z"
                        pure (x, y, z)
                result `shouldBe` (Just "1", Just "2", Just "3")

            it "read-modify-write pattern" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "counter" "0"
                    runTransactionUnguarded db $ do
                        mVal <- query Items "counter"
                        case mVal of
                            Just "0" -> insert Items "counter" "1"
                            _ -> pure ()
                    runTransactionUnguarded db $ query Items "counter"
                result `shouldBe` Just "1"

        describe "snapshot consistency" $ do
            it
                "queries within a transaction see consistent state despite concurrent writes"
                $ do
                    result <- withDB $ \db -> do
                        runTransactionUnguarded db $ insert Items "k" "before"
                        ready <- newEmptyMVar
                        done <- newEmptyMVar
                        _ <- forkIO $ do
                            takeMVar ready
                            runTransactionUnguarded db $ insert Items "k" "after"
                            putMVar done ()
                        runTransactionUnguarded db $ do
                            _ <- query Items "k"
                            insert Items "_sync" "go"
                            pure ()
                        putMVar ready ()
                        takeMVar done
                        runTransactionUnguarded db $ query Items "k"
                    result `shouldBe` Just "after"

            it "transaction reads are isolated from concurrent inserts" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                    writerReady <- newEmptyMVar
                    writerDone <- newEmptyMVar
                    _ <- forkIO $ do
                        takeMVar writerReady
                        runTransactionUnguarded db $ insert Items "a" "CHANGED"
                        putMVar writerDone ()
                    putMVar writerReady ()
                    takeMVar writerDone
                    runTransactionUnguarded db $ do
                        a <- query Items "a"
                        b <- query Items "b"
                        pure (a, b)
                result `shouldBe` (Just "CHANGED", Just "2")

        describe "speculation" $ do
            it "speculative writes are discarded" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "key" "original"
                    runSpeculation db $ do
                        insert Items "key" "speculative"
                        insert Items "new" "also-speculative"
                    runTransactionUnguarded db $ do
                        k <- query Items "key"
                        n <- query Items "new"
                        pure (k, n)
                result `shouldBe` (Just "original", Nothing)

            it "speculation provides read-your-writes" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "x" "real"
                    runSpeculation db $ do
                        insert Items "x" "speculated"
                        insert Items "y" "new"
                        x <- query Items "x"
                        y <- query Items "y"
                        pure (x, y)
                result `shouldBe` (Just "speculated", Just "new")

            it "speculation reads from snapshot" $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "s" "snap"
                    runSpeculation db $ query Items "s"
                result `shouldBe` Just "snap"

    -- mapColumns tests use in-memory directly since they test
    -- Transaction-level combinators, not backend specifics.
    describe "Database.KV.Transaction (mapColumns)" $ do
        it "lifts a sub-transaction" $ do
            let cols = mkColumns [0 ..] allCodecs
            db <- mkInMemoryDatabase cols
            result <- do
                runTransactionUnguarded db
                    $ mapColumns InA
                    $ insert ItemsA "ka" "va"
                runTransactionUnguarded db
                    $ query (InA ItemsA) "ka"
            result `shouldBe` Just "va"

        it "composes from different types" $ do
            let cols = mkColumns [0 ..] allCodecs
            db <- mkInMemoryDatabase cols
            result <- do
                runTransactionUnguarded db $ do
                    mapColumns InA $ insert ItemsA "ka" "va"
                    mapColumns InB $ insert ItemsB "kb" "vb"
                runTransactionUnguarded db $ do
                    a <- query (InA ItemsA) "ka"
                    b <- query (InB ItemsB) "kb"
                    pure (a, b)
            result `shouldBe` (Just "va", Just "vb")

        it "maps read-modify-write" $ do
            let cols = mkColumns [0 ..] allCodecs
            db <- mkInMemoryDatabase cols
            result <- do
                runTransactionUnguarded db
                    $ mapColumns InA
                    $ insert ItemsA "x" "old"
                runTransactionUnguarded db $ mapColumns InA $ do
                    mv <- query ItemsA "x"
                    case mv of
                        Just _ -> insert ItemsA "x" "new"
                        Nothing -> pure ()
                runTransactionUnguarded db
                    $ query (InA ItemsA) "x"
            result `shouldBe` Just "new"

        it "maps delete operations" $ do
            let cols = mkColumns [0 ..] allCodecs
            db <- mkInMemoryDatabase cols
            result <- do
                runTransactionUnguarded db
                    $ mapColumns InA
                    $ insert ItemsA "d" "val"
                runTransactionUnguarded db
                    $ mapColumns InA
                    $ delete ItemsA "d"
                runTransactionUnguarded db
                    $ query (InA ItemsA) "d"
            result `shouldBe` (Nothing :: Maybe ByteString)

    forBothBackends "Database.KV.Transaction (prefixDatabase)" $ \withDB -> do
        it "prefixed writes are isolated" $ do
            result <- withDB $ \base -> do
                let dbA = prefixDatabase "a:" base
                    dbB = prefixDatabase "b:" base
                runTransactionUnguarded dbA $ insert Items "k" "va"
                runTransactionUnguarded dbB $ insert Items "k" "vb"
                a <- runTransactionUnguarded dbA $ query Items "k"
                b <- runTransactionUnguarded dbB $ query Items "k"
                pure (a, b)
            result `shouldBe` (Just "va", Just "vb")

        it "prefixed reads don't see unprefixed data" $ do
            result <- withDB $ \base -> do
                let pfxDb = prefixDatabase "pfx:" base
                runTransactionUnguarded base $ insert Items "k" "raw"
                runTransactionUnguarded pfxDb $ query Items "k"
            result `shouldBe` Nothing

        it "prefixed delete only affects prefix" $ do
            result <- withDB $ \base -> do
                let dbA = prefixDatabase "a:" base
                    dbB = prefixDatabase "b:" base
                runTransactionUnguarded dbA $ insert Items "k" "va"
                runTransactionUnguarded dbB $ insert Items "k" "vb"
                runTransactionUnguarded dbA $ delete Items "k"
                a <- runTransactionUnguarded dbA $ query Items "k"
                b <- runTransactionUnguarded dbB $ query Items "k"
                pure (a, b)
            result `shouldBe` (Nothing, Just "vb")
