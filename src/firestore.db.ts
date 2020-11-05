import { Query, QueryDocumentSnapshot, QuerySnapshot } from '@google-cloud/firestore'
import {
  CommonDB,
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonSchema,
  DBQuery,
  DBTransaction,
  ObjectWithId,
  RunQueryResult,
} from '@naturalcycles/db-lib'
import { pMap, _chunk, _filterNullishValues, _omit } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import * as firebaseAdmin from 'firebase-admin'
import { Transform } from 'stream'
import { escapeDocId, unescapeDocId } from './firestore.util'
import { dbQueryToFirestoreQuery } from './query.util'

export interface FirestoreDBCfg {
  firestore: firebaseAdmin.firestore.Firestore
}

export interface FirestoreDBOptions extends CommonDBOptions {}
export interface FirestoreDBSaveOptions extends CommonDBSaveOptions {}

export class FirestoreDB implements CommonDB {
  constructor(public cfg: FirestoreDBCfg) {}

  // GET
  async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: FirestoreDBOptions,
  ): Promise<ROW[]> {
    // Oj, doesn't look like a very optimal implementation!
    // TODO: check if we can query by keys or smth
    return (await Promise.all(ids.map(id => this.getById<ROW>(table, id, opt)))).filter(
      Boolean,
    ) as ROW[]
  }

  async getById<ROW extends ObjectWithId>(
    table: string,
    id: string,
    opt?: FirestoreDBOptions,
  ): Promise<ROW | undefined> {
    const doc = await this.cfg.firestore.collection(table).doc(escapeDocId(id)).get()

    const data = doc.data()
    if (data === undefined) return

    return {
      id,
      ...(data as any),
    }
  }

  // QUERY
  async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: FirestoreDBOptions,
  ): Promise<RunQueryResult<ROW>> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))

    let rows = await this.runFirestoreQuery<ROW>(firestoreQuery, opt)

    // Special case when projection query didn't specify 'id'
    if (q._selectedFieldNames && !q._selectedFieldNames.includes('id')) {
      rows = rows.map(r => _omit(r as any, ['id']))
    }

    return { rows }
  }

  async runFirestoreQuery<ROW extends ObjectWithId>(
    q: Query,
    opt?: FirestoreDBOptions,
  ): Promise<ROW[]> {
    return this.querySnapshotToArray(await q.get())
  }

  async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: FirestoreDBOptions,
  ): Promise<number> {
    const { rows } = await this.runQuery(q.select([]), opt)
    return rows.length
  }

  streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<ROW> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))

    return firestoreQuery.stream().pipe(
      new Transform({
        objectMode: true,
        transform: (doc: QueryDocumentSnapshot, _enc, cb) => {
          cb(null, {
            id: unescapeDocId(doc.id),
            ...doc.data(),
          })
        },
      }),
    )
  }

  // SAVE
  async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt?: FirestoreDBSaveOptions,
  ): Promise<void> {
    // Firestore allows max 500 items in one batch
    await pMap(
      _chunk(rows, 500),
      async chunk => {
        const batch = this.cfg.firestore.batch()

        chunk.forEach(row => {
          batch.set(
            this.cfg.firestore.collection(table).doc(escapeDocId(row.id)),
            _filterNullishValues(row), // todo: check if we really need to filter them (thinking of null values)
          )
        })

        await batch.commit()
      },
      { concurrency: 1 },
    )
  }

  // DELETE
  async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: FirestoreDBOptions,
  ): Promise<number> {
    const firestoreQuery = dbQueryToFirestoreQuery(
      q.select([]),
      this.cfg.firestore.collection(q.table),
    )
    const ids = (await this.runFirestoreQuery<ROW>(firestoreQuery)).map(obj => obj.id)

    await this.deleteByIds(q.table, ids, opt)

    return ids.length
  }

  async deleteByIds(table: string, ids: string[], opt?: FirestoreDBOptions): Promise<number> {
    await pMap(_chunk(ids, 500), async chunk => {
      const batch = this.cfg.firestore.batch()

      chunk.forEach(id => {
        batch.delete(this.cfg.firestore.collection(table).doc(escapeDocId(id)))
      })

      await batch.commit()
    })

    return ids.length
  }

  private querySnapshotToArray<T = any>(qs: QuerySnapshot): T[] {
    const rows: any[] = []

    qs.forEach(doc => {
      rows.push({
        id: unescapeDocId(doc.id),
        ...doc.data(),
      })
    })

    return rows
  }

  async getTables(): Promise<string[]> {
    return [] // todo
  }

  async getTableSchema<ROW extends ObjectWithId>(table: string): Promise<CommonSchema<ROW>> {
    return {
      table,
      fields: [],
    }
  }

  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {
    // todo
  }

  async commitTransaction(tx: DBTransaction, opt?: CommonDBSaveOptions): Promise<void> {
    throw new Error('commitTransaction is not supported yet')
  }

  async ping(): Promise<void> {
    // no-op now
  }
}
