import { TEST_TABLE, TestItem, testItemUnsavedSchema } from '@naturalcycles/db-dev-lib'
import { CommonDao } from '@naturalcycles/db-lib'
import { base64ToString, requireEnvKeys } from '@naturalcycles/nodejs-lib'
import * as firebaseAdmin from 'firebase-admin'
import { FirestoreDB } from '../../firestore.db'
require('dotenv').config()

const { FIREBASE_DB_URL, SECRET_FIREBASE } = requireEnvKeys('FIREBASE_DB_URL', 'SECRET_FIREBASE')
const credential = firebaseAdmin.credential.cert(JSON.parse(base64ToString(SECRET_FIREBASE)))

const firestore = firebaseAdmin
  .initializeApp({
    credential,
    databaseURL: FIREBASE_DB_URL,
  })
  .firestore()

export const firestoreDB = new FirestoreDB({
  firestore,
})

export const testItemDao = new CommonDao<TestItem>({
  table: TEST_TABLE,
  db: firestoreDB,
  bmUnsavedSchema: testItemUnsavedSchema,
  dbmUnsavedSchema: testItemUnsavedSchema,
})
