{- |
Tests for Database.KV.Query snapshot consistency — runs against both backends.
-}
module Database.KV.QuerySpec (spec) where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, try)
import Control.Monad (forM_)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS
import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , nextEntry
    )
import Database.KV.Query
    ( Querying
    , interpretQuerying
    , iterating
    , query
    )
import Database.KV.TestDatabase
    ( Tables (..)
    , forBothBackends
    )
import Database.KV.Transaction
    ( insert
    , runTransactionUnguarded
    )
import Test.Hspec (Spec, describe, it, shouldBe)

-- | Collect all entries via iteration (backend-polymorphic)
collectAll
    :: Querying IO cf Tables op [(ByteString, ByteString)]
collectAll = iterating Items go
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

spec :: Spec
spec = forBothBackends "Database.KV.Query" $ \withDB -> do
    describe "basic querying" $ do
        it "can query a value" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ insert Items "key" "value"
                interpretQuerying db $ query Items "key"
            result `shouldBe` Just "value"

        it "can iterate entries" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                interpretQuerying db collectAll
            result `shouldBe` [("a", "1"), ("b", "2")]

    describe "snapshot consistency" $ do
        it "repeated queries see the same value despite concurrent writes"
            $ do
                result <- withDB $ \db -> do
                    runTransactionUnguarded db $ insert Items "key" "initial"

                    started <- newEmptyMVar
                    done <- newEmptyMVar

                    writerThread <- forkIO $ do
                        putMVar started ()
                        let loop n
                                | n > 200 = pure ()
                                | otherwise = do
                                    let val = BS.pack $ "value-" <> show n
                                    runTransactionUnguarded db $ insert Items "key" val
                                    threadDelay 500
                                    loop (n + 1 :: Int)
                        loop (0 :: Int)
                        putMVar done ()

                    takeMVar started
                    threadDelay 1000

                    queryResult <- interpretQuerying db $ do
                        v1 <- query Items "key"
                        liftIO $ threadDelay 20000
                        v2 <- query Items "key"
                        liftIO $ threadDelay 20000
                        v3 <- query Items "key"
                        pure (v1, v2, v3)

                    _ <-
                        try (takeMVar done)
                            :: IO (Either SomeException ())
                    killThread writerThread

                    pure queryResult

                let (v1, v2, v3) = result
                v1 `shouldBe` v2
                v2 `shouldBe` v3

        it "query and iterator see the same snapshot" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    insert Items "a" "1"
                    insert Items "b" "2"
                    insert Items "c" "3"

                started <- newEmptyMVar
                done <- newEmptyMVar

                writerThread <- forkIO $ do
                    putMVar started ()
                    let loop n
                            | n > 100 = pure ()
                            | otherwise = do
                                let suffix = BS.pack $ "-v" <> show n
                                runTransactionUnguarded db $ do
                                    insert Items "a" ("1" <> suffix)
                                    insert Items "b" ("2" <> suffix)
                                    insert Items "c" ("3" <> suffix)
                                threadDelay 500
                                loop (n + 1 :: Int)
                    loop (0 :: Int)
                    putMVar done ()

                takeMVar started
                threadDelay 1000

                queryResult <- interpretQuerying db $ do
                    vA <- query Items "a"
                    liftIO $ threadDelay 15000
                    allEntries <- collectAll
                    liftIO $ threadDelay 15000
                    vA2 <- query Items "a"
                    vB <- query Items "b"
                    vC <- query Items "c"
                    pure (vA, vA2, vB, vC, allEntries)

                _ <- try (takeMVar done) :: IO (Either SomeException ())
                killThread writerThread

                pure queryResult

            let (vA, vA2, vB, vC, allEntries) = result
            vA `shouldBe` vA2
            lookup "a" allEntries `shouldBe` vA
            lookup "b" allEntries `shouldBe` vB
            lookup "c" allEntries `shouldBe` vC

        it "multiple iterations see the same data" $ do
            result <- withDB $ \db -> do
                runTransactionUnguarded db $ do
                    forM_ [1 .. 10 :: Int] $ \i ->
                        insert
                            Items
                            (BS.pack $ show i)
                            (BS.pack $ "val" <> show i)

                started <- newEmptyMVar
                done <- newEmptyMVar

                writerThread <- forkIO $ do
                    putMVar started ()
                    let loop n
                            | n > 150 = pure ()
                            | otherwise = do
                                forM_ [1 .. 10 :: Int] $ \i -> do
                                    let val =
                                            BS.pack
                                                $ "val"
                                                    <> show i
                                                    <> "-"
                                                    <> show n
                                    runTransactionUnguarded db
                                        $ insert Items (BS.pack $ show i) val
                                threadDelay 300
                                loop (n + 1 :: Int)
                    loop (0 :: Int)
                    putMVar done ()

                takeMVar started
                threadDelay 1000

                queryResult <- interpretQuerying db $ do
                    entries1 <- collectAll
                    liftIO $ threadDelay 20000
                    entries2 <- collectAll
                    liftIO $ threadDelay 20000
                    entries3 <- collectAll
                    pure (entries1, entries2, entries3)

                _ <- try (takeMVar done) :: IO (Either SomeException ())
                killThread writerThread

                pure queryResult

            let (entries1, entries2, entries3) = result
            entries1 `shouldBe` entries2
            entries2 `shouldBe` entries3
