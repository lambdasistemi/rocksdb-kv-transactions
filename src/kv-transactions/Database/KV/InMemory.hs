{- |
Module      : Database.KV.InMemory
Description : Pure in-memory backend for type-safe key-value transactions
Copyright   : (c) Paolo Veronelli, 2024
License     : Apache-2.0

This module provides a pure in-memory implementation of the 'Database' interface,
enabling tests and lightweight consumers that don't need RocksDB on disk.

= Features

* No external dependencies (uses 'IORef' and 'Map' from base\/containers)
* Lexicographic key ordering matching RocksDB's default comparator
* Atomic batch writes via 'atomicModifyIORef\''
* Snapshot isolation for consistent reads

= Usage

@
import Database.KV.InMemory (mkInMemoryDatabase)
import Database.KV.Transaction

let cols = mkColumns [0..] (fromPairList [Users :=> myCodecs])
db <- mkInMemoryDatabase cols
runTx <- newRunTransaction db
runTx $ do
    insert Users \"user1\" \"alice\"
    query Users \"user1\"  -- Just \"alice\"
@
-}
module Database.KV.InMemory
    ( mkInMemoryDatabase
    )
where

import Data.ByteString (ByteString)
import Data.Dependent.Map (DMap)
import Data.Dependent.Map qualified as DMap
import Data.Dependent.Sum (DSum (..))
import Data.IORef
    ( IORef
    , atomicModifyIORef'
    , newIORef
    , readIORef
    , writeIORef
    )
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Database.KV.Database
    ( Column (..)
    , Database (..)
    , Pos (..)
    , QueryIterator (..)
    )

{- |
Create a 'Database' backed by in-memory 'Map's.

Each column family is identified by an 'Int' and stored as a
'Map ByteString ByteString'. The column family IDs are assigned
by 'mkColumns' from a list like @[0..]@.

Operations use the same type @(Int, ByteString, Maybe ByteString)@
where @Just v@ means put and @Nothing@ means delete.
-}
mkInMemoryDatabase
    :: DMap t (Column Int)
    -> IO (Database IO Int t (Int, ByteString, Maybe ByteString))
mkInMemoryDatabase columns = do
    let cfIds = fmap (\(_ :=> Column{family}) -> family) (DMap.toList columns)
        emptyStore = Map.fromList [(cf, Map.empty) | cf <- cfIds]
    storeRef <- newIORef emptyStore
    pure $ mkDatabase storeRef columns

-- | Build a Database record from a mutable store reference.
mkDatabase
    :: IORef (Map Int (Map ByteString ByteString))
    -> DMap t (Column Int)
    -> Database IO Int t (Int, ByteString, Maybe ByteString)
mkDatabase storeRef columns =
    Database
        { valueAt = \cf k -> do
            store <- readIORef storeRef
            pure $ Map.lookup cf store >>= Map.lookup k
        , applyOps = \ops ->
            atomicModifyIORef' storeRef $ \store ->
                (foldl applyOp store ops, ())
        , mkOperation = (,,)
        , newIterator = \cf -> do
            store <- readIORef storeRef
            let colMap = Map.findWithDefault Map.empty cf store
            mkIterator colMap
        , columns
        , withSnapshot = \f -> do
            store <- readIORef storeRef
            f $ mkSnapshotDatabase store storeRef columns
        }

-- | Apply a single operation to the store.
applyOp
    :: Map Int (Map ByteString ByteString)
    -> (Int, ByteString, Maybe ByteString)
    -> Map Int (Map ByteString ByteString)
applyOp store (cf, k, mv) =
    Map.adjust
        ( case mv of
            Just v -> Map.insert k v
            Nothing -> Map.delete k
        )
        cf
        store

-- | Build a Database that reads from a frozen snapshot but writes to the mutable store.
mkSnapshotDatabase
    :: Map Int (Map ByteString ByteString)
    -> IORef (Map Int (Map ByteString ByteString))
    -> DMap t (Column Int)
    -> Database IO Int t (Int, ByteString, Maybe ByteString)
mkSnapshotDatabase snapshot storeRef columns =
    Database
        { valueAt = \cf k ->
            pure $ Map.lookup cf snapshot >>= Map.lookup k
        , applyOps = \ops ->
            atomicModifyIORef' storeRef $ \store ->
                (foldl applyOp store ops, ())
        , mkOperation = (,,)
        , newIterator = \cf -> do
            let colMap = Map.findWithDefault Map.empty cf snapshot
            mkIterator colMap
        , columns
        , withSnapshot = \f ->
            -- Already in a snapshot — reuse the same frozen state
            f $ mkSnapshotDatabase snapshot storeRef columns
        }

-- | Create a QueryIterator over a Map snapshot.
mkIterator
    :: Map ByteString ByteString
    -> IO (QueryIterator IO)
mkIterator colMap = do
    -- Position: Nothing = invalid, Just key = positioned at that key
    posRef <- newIORef (Nothing :: Maybe ByteString)
    pure
        QueryIterator
            { step = \case
                PosFirst ->
                    writeIORef posRef $ fst <$> Map.lookupMin colMap
                PosLast ->
                    writeIORef posRef $ fst <$> Map.lookupMax colMap
                PosNext -> do
                    mKey <- readIORef posRef
                    case mKey of
                        Nothing -> pure ()
                        Just k ->
                            writeIORef posRef
                                $ fst
                                    <$> Map.lookupGT k colMap
                PosPrev -> do
                    mKey <- readIORef posRef
                    case mKey of
                        Nothing -> pure ()
                        Just k ->
                            writeIORef posRef
                                $ fst
                                    <$> Map.lookupLT k colMap
                PosAny k ->
                    writeIORef posRef
                        $ fst
                            <$> Map.lookupGE k colMap
                PosDestroy ->
                    writeIORef posRef Nothing
            , isValid = do
                mKey <- readIORef posRef
                pure $ case mKey of
                    Nothing -> False
                    Just k -> Map.member k colMap
            , entry = do
                mKey <- readIORef posRef
                pure $ case mKey of
                    Nothing -> Nothing
                    Just k -> (k,) <$> Map.lookup k colMap
            }
