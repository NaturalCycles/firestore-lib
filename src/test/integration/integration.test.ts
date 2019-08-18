import { testDao, testDB } from '@naturalcycles/db-dev-lib'
import { DBQuery } from '@naturalcycles/db-lib'
import { firestoreDB, testItemDao } from './firestore.mock'

jest.setTimeout(60000)

test('testDB', async () => {
  await testDB(firestoreDB, DBQuery)
})

test('testDao', async () => {
  await testDao(testItemDao, DBQuery)
})
