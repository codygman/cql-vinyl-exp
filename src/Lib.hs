{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedStrings,DataKinds #-}
module Lib
    ( someFunc
    ) where

import Database.Cassandra.CQL
import qualified Data.Text as T
import Data.Text.Read
import qualified Data.Map as M
import Data.Maybe
import Data.Typeable

getColumnFamilyInfoQuery :: Query Rows () (T.Text,T.Text)
getColumnFamilyInfoQuery = "select column_name,validator from system.schema_columns where keyspace_name='test' and columnfamily_name='songs';"

data CassandraColumn = CassandraColumn { cassandraColumnName :: T.Text
                                       , cassandraColumnType :: CassandraValidator
                                       } deriving (Show,Ord,Eq)

data Column = Column { columnName :: T.Text
                     , columnType :: T.Text
                     }

data CassandraValidator = UTF8Type
                        | BooleanType
                        | UUIDType
                        | Int32Type
                        | AsciiType
                        deriving (Show,Ord,Eq)

parseValidator :: T.Text -> Maybe CassandraValidator
parseValidator validator = case M.lookup validator validatorMap of
                             Just casValidator -> pure casValidator
                             Nothing -> error $ "couldn't parse validator: " ++ show validator
  where validatorMap = M.fromList [ ("org.apache.cassandra.db.marshal.UTF8Type",UTF8Type)
                                  , ("org.apache.cassandra.db.marshal.BooleanType",BooleanType)
                                  , ("org.apache.cassandra.db.marshal.Int32Type",Int32Type)
                                  , ("org.apache.cassandra.db.marshal.AsciiType",AsciiType)
                                  , ("org.apache.cassandra.db.marshal.UUIDType",UUIDType)
                                  ]

-- getParseFunc :: T.Text -> Maybe CassandraValidator
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- TODO FIX this does not work
-- getParseFunc parseFunc = case M.lookup parseFunc parseFuncMap of
--                              Just casParseFunc -> pure casParseFunc
--                              Nothing -> error $ "couldn't find parse func: " ++ show parseFunc
--   where parseFuncMap = M.fromList [ ("org.apache.cassandra.db.marshal.UTF8Type", parseText)
--                                   , ("org.apache.cassandra.db.marshal.BooleanType",parseBool)
--                                   , ("org.apache.cassandra.db.marshal.Int32Type",parseInt)
--                                   , ("org.apache.cassandra.db.marshal.AsciiType",parseText)
--                                   , ("org.apache.cassandra.db.marshal.UUIDType",(error "UUID type not yet supported"))

--                                   ]

parseText :: T.Text -> T.Text
parseText = id

parseInt :: T.Text -> Int
parseInt txt = case decimal txt of
                 Right d -> fromIntegral . fst $ d
                 Left e -> error e

parseBool :: T.Text -> Bool
parseBool "true" = True
parseBool "false" = False
parseBool txt = error $ "'" ++ T.unpack txt ++ "'" ++ " is not a bool"

-- getFieldType: maps validators strings to Haskell type's
getFieldType validator = parseValidator validator

mkCassandraColumn (colName,validator) = do
  let colType = getFieldType validator
  CassandraColumn <$> pure colName <*> colType

mkColumnFamily :: [Maybe CassandraColumn] -> [CassandraColumn]
mkColumnFamily mCols = do
  case all isJust mCols of
    True -> do
      -- all columns parsed successfully
      fromJust <$> mCols
    False -> error "all columns weren't parsed successfully"

getColumnFamilyInfo = do
    -- let auth = Just (PasswordAuthenticator "cassandra" "cassandra")
    let auth = Nothing
    {-
    Assuming a 'test' keyspace already exists. Here's some CQL to create it:
    CREATE KEYSPACE test WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
    -}
    pool <- newPool [("localhost", "9042")] "test" auth -- servers, keyspace, maybe auth
    runCas pool $ do
      colFamInfo' <- executeRows QUORUM getColumnFamilyInfoQuery ()
      return colFamInfo'
-- usage:
λ> fmap mkCassandraColumn <$> getColumnFamilyInfo
-- [Just (CassandraColumn {cassandraColumnName = "artist", cassandraColumnType = UTF8Type}),Just (CassandraColumn {cassandraColumnName = "comment", cassandraColumnType = UTF8Type}),Just (CassandraColumn {cassandraColumnName = "femalesinger", cassandraColumnType = BooleanType}),Just (CassandraColumn {cassandraColumnName = "id", cassandraColumnType = UUIDType}),Just (CassandraColumn {cassandraColumnName = "timesplayed", cassandraColumnType = Int32Type}),Just (CassandraColumn {cassandraColumnName = "title", cassandraColumnType = AsciiType})]



someFunc :: IO ()
someFunc = putStrLn "someFunc"
