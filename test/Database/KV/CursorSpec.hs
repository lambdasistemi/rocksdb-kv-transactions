{- |
Tests for Database.KV.Cursor — runs against both RocksDB and in-memory backends.
-}
module Database.KV.CursorSpec (spec) where

import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , lastEntry
    , nextEntry
    , prevEntry
    , seekKey
    )
import Database.KV.TestDatabase
    ( Tables (..)
    , forBothBackends
    )
import Database.KV.Transaction
    ( insert
    , iterating
    , runTransactionUnguarded
    )
import Test.Hspec (Spec, describe, it, shouldBe)

spec :: Spec
spec = forBothBackends "Database.KV.Cursor" $ \withDB -> do
    describe "firstEntry / lastEntry" $ do
        it "returns Nothing on empty table" $ do
            result <- withDB $ \db ->
                runTransactionUnguarded db $ iterating Items firstEntry
            result `shouldBe` Nothing

        it "returns the only entry when table has one item" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ insert Items "only" "one"
                runTransactionUnguarded db $ iterating Items firstEntry
            result `shouldBe` Just Entry{entryKey = "only", entryValue = "one"}

        it "firstEntry returns lexicographically smallest key" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "banana" "yellow"
                    insert Items "apple" "red"
                    insert Items "cherry" "dark"
                runTransactionUnguarded db $ iterating Items firstEntry
            result `shouldBe` Just Entry{entryKey = "apple", entryValue = "red"}

        it "lastEntry returns lexicographically largest key" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "banana" "yellow"
                    insert Items "apple" "red"
                    insert Items "cherry" "dark"
                runTransactionUnguarded db $ iterating Items lastEntry
            result `shouldBe` Just Entry{entryKey = "cherry", entryValue = "dark"}

    describe "nextEntry / prevEntry" $ do
        it "iterates forward through all entries" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                    insert Items "c" "3"
                runTransactionUnguarded db $ iterating Items $ do
                    e1 <- firstEntry
                    e2 <- nextEntry
                    e3 <- nextEntry
                    e4 <- nextEntry
                    pure (e1, e2, e3, e4)
            result
                `shouldBe` ( Just Entry{entryKey = "a", entryValue = "1"}
                           , Just Entry{entryKey = "b", entryValue = "2"}
                           , Just Entry{entryKey = "c", entryValue = "3"}
                           , Nothing
                           )

        it "iterates backward through all entries" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                    insert Items "c" "3"
                runTransactionUnguarded db $ iterating Items $ do
                    e1 <- lastEntry
                    e2 <- prevEntry
                    e3 <- prevEntry
                    e4 <- prevEntry
                    pure (e1, e2, e3, e4)
            result
                `shouldBe` ( Just Entry{entryKey = "c", entryValue = "3"}
                           , Just Entry{entryKey = "b", entryValue = "2"}
                           , Just Entry{entryKey = "a", entryValue = "1"}
                           , Nothing
                           )

    describe "seekKey" $ do
        it "seeks to exact key when present" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                    insert Items "c" "3"
                runTransactionUnguarded db $ iterating Items $ seekKey "b"
            result `shouldBe` Just Entry{entryKey = "b", entryValue = "2"}

        it "seeks to next key when exact key not present" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "c" "3"
                    insert Items "e" "5"
                runTransactionUnguarded db $ iterating Items $ seekKey "b"
            result `shouldBe` Just Entry{entryKey = "c", entryValue = "3"}

        it "returns Nothing when seeking past all keys" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                runTransactionUnguarded db $ iterating Items $ seekKey "z"
            result `shouldBe` Nothing

    describe "combined cursor operations" $ do
        it "seek then iterate" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                    insert Items "c" "3"
                    insert Items "d" "4"
                runTransactionUnguarded db $ iterating Items $ do
                    e1 <- seekKey "b"
                    e2 <- nextEntry
                    e3 <- nextEntry
                    pure (e1, e2, e3)
            result
                `shouldBe` ( Just Entry{entryKey = "b", entryValue = "2"}
                           , Just Entry{entryKey = "c", entryValue = "3"}
                           , Just Entry{entryKey = "d", entryValue = "4"}
                           )
