import { Query, QueryDocumentSnapshot, QuerySnapshot } from '@google-cloud/firestore'
import {
  BaseDBEntity,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
} from '@naturalcycles/db-lib'
import { _chunk, pMap } from '@naturalcycles/js-lib'
import * as firebaseAdmin from 'firebase-admin'
import { Observable } from 'rxjs'
import { Transform } from 'stream'
import { escapeDocId, unescapeDocId } from './firestore.util'
import { dbQueryToFirestoreQuery } from './query.util'
import { streamToObservable } from './stream.util'

export interface FirestoreDBCfg {
  firestore: firebaseAdmin.firestore.Firestore
}

export interface FirestoreDBOptions extends CommonDBOptions {}
export interface FirestoreDBSaveOptions extends CommonDBSaveOptions {}

export class FirestoreDB implements CommonDB {
  constructor (private cfg: FirestoreDBCfg) {}

  async resetCache (): Promise<void> {}

  // GET
  async getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: FirestoreDBOptions,
  ): Promise<DBM[]> {
    return (await Promise.all(ids.map(id => this.getById<DBM>(table, id, opts)))).filter(
      Boolean,
    ) as DBM[]
  }

  async getById<DBM extends BaseDBEntity> (
    table: string,
    id: string,
    opts?: FirestoreDBOptions,
  ): Promise<DBM | undefined> {
    const doc = await this.cfg.firestore
      .collection(table)
      .doc(escapeDocId(id))
      .get()

    const data = doc.data()
    if (data === undefined) return

    return {
      id,
      ...(data as any),
    }
  }

  // QUERY
  async runQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: FirestoreDBOptions,
  ): Promise<DBM[]> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))
    return this.runFirestoreQuery(firestoreQuery, opts)
  }

  async runFirestoreQuery<DBM extends BaseDBEntity> (
    q: Query,
    opts?: FirestoreDBOptions,
  ): Promise<DBM[]> {
    return this.querySnapshotToArray(await q.get())
  }

  async runQueryCount<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: FirestoreDBOptions,
  ): Promise<number> {
    return (await this.runQuery(q.select([]))).length
  }

  streamQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: FirestoreDBOptions,
  ): Observable<DBM> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))
    return streamToObservable(
      firestoreQuery.stream().pipe(
        new Transform({
          objectMode: true,
          transform: (doc: QueryDocumentSnapshot, enc, callback) => {
            callback(undefined, {
              id: unescapeDocId(doc.id),
              ...doc.data(),
            })
          },
        }),
      ),
    )
  }

  // SAVE
  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: FirestoreDBSaveOptions,
  ): Promise<DBM[]> {
    // Firestore allows max 500 items in one batch
    await pMap(
      _chunk(dbms, 500),
      async chunk => {
        const batch = this.cfg.firestore.batch()

        chunk.forEach(dbm => {
          batch.set(this.cfg.firestore.collection(table).doc(escapeDocId(dbm.id)), dbm)
        })

        await batch.commit()
      },
      { concurrency: 1 },
    )

    return dbms
  }

  // DELETE

  async deleteByQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: FirestoreDBOptions,
  ): Promise<string[]> {
    const firestoreQuery = dbQueryToFirestoreQuery(
      q.select([]),
      this.cfg.firestore.collection(q.table),
    )
    const ids = (await this.runFirestoreQuery<DBM>(firestoreQuery)).map(obj => obj.id)

    await this.deleteByIds(q.table, ids, opts)

    return ids
  }

  async deleteByIds (table: string, ids: string[], opts?: FirestoreDBOptions): Promise<string[]> {
    await pMap(_chunk(ids, 500), async chunk => {
      const batch = this.cfg.firestore.batch()

      chunk.forEach(id => {
        batch.delete(this.cfg.firestore.collection(table).doc(escapeDocId(id)))
      })

      await batch.commit()
    })

    return ids
  }

  private querySnapshotToArray<T = any> (qs: QuerySnapshot): T[] {
    const rows: any[] = []

    qs.forEach(doc => {
      rows.push({
        id: unescapeDocId(doc.id),
        ...doc.data(),
      })
    })

    return rows
  }
}
