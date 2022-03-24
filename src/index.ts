import { Firestore } from '@google-cloud/firestore'
import {
  FirestoreDB,
  FirestoreDBCfg,
  FirestoreDBOptions,
  FirestoreDBSaveOptions,
} from './firestore.db'
import { dbQueryToFirestoreQuery } from './query.util'

export type { FirestoreDBCfg, FirestoreDBOptions, FirestoreDBSaveOptions }

export { Firestore, FirestoreDB, dbQueryToFirestoreQuery }
