import { createdUpdatedFields } from '@naturalcycles/db-lib'
import {
  runCommonDaoTest,
  runCommonDBTest,
  TestItemDBM,
  TEST_TABLE,
} from '@naturalcycles/db-lib/dist/testing'
import { firestoreDB } from './firestore.mock'

jest.setTimeout(60000)

describe('runCommonDBTest', () => runCommonDBTest(firestoreDB))

describe('runCommonDaoTest', () => runCommonDaoTest(firestoreDB))

test.skip('undefined value', async () => {
  const testItem: TestItemDBM = {
    id: '123',
    k1: 'k11',
    k3: undefined,
    // k3: null as any,
    ...createdUpdatedFields(),
  }
  await firestoreDB.saveBatch<TestItemDBM>(TEST_TABLE, [testItem])
  const loaded = await firestoreDB.getById(TEST_TABLE, testItem.id)
  console.log(loaded)
})
