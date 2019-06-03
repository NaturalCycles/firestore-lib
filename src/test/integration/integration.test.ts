import { testDao, testDB } from '@naturalcycles/db-dev-lib'
import { firestoreDB, testItemDao } from './firestore.mock'

test('testDB', async () => {
  await testDB(firestoreDB)
})

test('testDao', async () => {
  await testDao(testItemDao)
})
