import { TEST_TABLE, testDao, testDB, TestItem } from '@naturalcycles/db-dev-lib'
import { createdUpdatedFields, DBQuery } from '@naturalcycles/db-lib'
import { firestoreDB, testItemDao } from './firestore.mock'

jest.setTimeout(60000)

test('testDB', async () => {
  await testDB(firestoreDB, DBQuery)
})

test('testDao', async () => {
  await testDao(testItemDao, DBQuery)
})

test.skip('undefined value', async () => {
  const testItem: TestItem = {
    id: '123',
    k1: 'k11',
    k3: undefined,
    // k3: null as any,
    ...createdUpdatedFields(),
  }
  await firestoreDB.saveBatch<TestItem>(TEST_TABLE, [testItem])
  const loaded = await firestoreDB.getById(TEST_TABLE, testItem.id)
  console.log(loaded)
})
