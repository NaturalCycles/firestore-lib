import { Firestore, Query, QueryDocumentSnapshot, QuerySnapshot } from '@google-cloud/firestore'
import {
  BaseCommonDB,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveMethod,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  DBQuery,
  DBTransaction,
  RunQueryResult,
} from '@naturalcycles/db-lib'
import {
  ErrorMode,
  pMap,
  _chunk,
  _omit,
  _filterUndefinedValues,
  ObjectWithId,
  AnyObjectWithId,
  _assert,
  _isTruthy,
} from '@naturalcycles/js-lib'
import { ReadableTyped, transformMapSimple } from '@naturalcycles/nodejs-lib'
import { escapeDocId, unescapeDocId } from './firestore.util'
import { dbQueryToFirestoreQuery } from './query.util'

export interface FirestoreDBCfg {
  firestore: Firestore
}

export interface FirestoreDBOptions extends CommonDBOptions {}
export interface FirestoreDBSaveOptions<ROW extends Partial<ObjectWithId> = AnyObjectWithId>
  extends CommonDBSaveOptions<ROW> {}

const methodMap: Record<CommonDBSaveMethod, string> = {
  insert: 'create',
  update: 'update',
  upsert: 'set',
}

export class FirestoreDB extends BaseCommonDB implements CommonDB {
  constructor(public cfg: FirestoreDBCfg) {
    super()
  }

  // GET
  override async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: ROW['id'][],
    _opt?: FirestoreDBOptions,
  ): Promise<ROW[]> {
    // Oj, doesn't look like a very optimal implementation!
    // TODO: check if we can query by keys or smth
    // return (await Promise.all(ids.map(id => this.getById<ROW>(table, id, opt)))).filter(
    //   Boolean,
    // ) as ROW[]
    if (!ids.length) return []

    const { firestore } = this.cfg
    const col = firestore.collection(table)

    return (await firestore.getAll(...ids.map(id => col.doc(escapeDocId(id)))))
      .map(doc => {
        const data = doc.data()
        if (data === undefined) return
        return {
          id: unescapeDocId(doc.id),
          ...(data as any),
        }
      })
      .filter(_isTruthy)
  }

  // QUERY
  override async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: FirestoreDBOptions,
  ): Promise<RunQueryResult<ROW>> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))

    let rows = await this.runFirestoreQuery<ROW>(firestoreQuery, opt)

    // Special case when projection query didn't specify 'id'
    if (q._selectedFieldNames && !q._selectedFieldNames.includes('id')) {
      rows = rows.map(r => _omit(r, ['id']))
    }

    return { rows }
  }

  async runFirestoreQuery<ROW extends ObjectWithId>(
    q: Query,
    _opt?: FirestoreDBOptions,
  ): Promise<ROW[]> {
    return this.querySnapshotToArray(await q.get())
  }

  override async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: FirestoreDBOptions,
  ): Promise<number> {
    const { rows } = await this.runQuery(q.select([]), opt)
    return rows.length
  }

  override streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBStreamOptions,
  ): ReadableTyped<ROW> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))

    return firestoreQuery.stream().pipe(
      transformMapSimple<QueryDocumentSnapshot<any>, ROW>(
        doc => ({
          id: unescapeDocId(doc.id),
          ...doc.data(),
        }),
        {
          errorMode: ErrorMode.SUPPRESS, // because .pipe cannot propagate errors
        },
      ),
    )
  }

  // SAVE
  override async saveBatch<ROW extends Partial<ObjectWithId>>(
    table: string,
    rows: ROW[],
    opt: FirestoreDBSaveOptions<ROW> = {},
  ): Promise<void> {
    const method = methodMap[opt.saveMethod!] || 'set'

    // Firestore allows max 500 items in one batch
    await pMap(
      _chunk(rows, 500),
      async chunk => {
        const batch = this.cfg.firestore.batch()

        chunk.forEach(row => {
          _assert(
            row.id,
            `firestore-db doesn't support id auto-generation, but empty id was provided in saveBatch`,
          )
          ;(batch as any)[method](
            this.cfg.firestore.collection(table).doc(escapeDocId(row.id)),
            _filterUndefinedValues(row),
          )
        })

        await batch.commit()
      },
      { concurrency: 1 },
    )
  }

  // DELETE
  override async deleteByQuery<ROW extends ObjectWithId>(
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

  override async deleteByIds<ROW extends ObjectWithId>(
    table: string,
    ids: ROW['id'][],
    _opt?: FirestoreDBOptions,
  ): Promise<number> {
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

  override async commitTransaction(tx: DBTransaction, _opt?: CommonDBSaveOptions): Promise<void> {
    const { firestore } = this.cfg

    await firestore.runTransaction(async tr => {
      for (const op of tx.ops) {
        if (op.type === 'saveBatch') {
          op.rows.forEach(row => {
            tr.set(
              firestore.collection(op.table).doc(escapeDocId(row.id)),
              _filterUndefinedValues(row),
            )
          })
        } else if (op.type === 'deleteByIds') {
          op.ids.forEach(id => {
            tr.delete(firestore.collection(op.table).doc(escapeDocId(id)))
          })
        } else {
          throw new Error(`DBOperation not supported: ${(op as any).type}`)
        }
      }
    })
  }

  override async ping(): Promise<void> {
    // no-op now
  }
}
