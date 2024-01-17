import {
  Firestore,
  Query,
  QueryDocumentSnapshot,
  QuerySnapshot,
  Transaction,
} from '@google-cloud/firestore'
import {
  BaseCommonDB,
  CommonDB,
  commonDBFullSupport,
  CommonDBOptions,
  CommonDBSaveMethod,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonDBSupport,
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
  pDefer,
  DeferredPromise,
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

type SaveOp = 'create' | 'update' | 'set'

const methodMap: Record<CommonDBSaveMethod, SaveOp> = {
  insert: 'create',
  update: 'update',
  upsert: 'set',
}

export class FirestoreDB extends BaseCommonDB implements CommonDB {
  constructor(public cfg: FirestoreDBCfg) {
    super()
  }

  override support: CommonDBSupport = {
    ...commonDBFullSupport,
    updateByQuery: false,
    tableSchemas: false,
  }

  // GET
  override async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
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
    const idFilter = q._filters.find(f => f.name === 'id')
    if (idFilter) {
      const ids: string[] = Array.isArray(idFilter.val) ? idFilter.val : [idFilter.val]
      return {
        rows: await this.getByIds(q.table, ids, opt),
      }
    }

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
    _opt?: FirestoreDBOptions,
  ): Promise<number> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))
    const r = await firestoreQuery.count().get()
    return r.data().count
  }

  override streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBStreamOptions,
  ): ReadableTyped<ROW> {
    const firestoreQuery = dbQueryToFirestoreQuery(q, this.cfg.firestore.collection(q.table))

    const stream: ReadableTyped<ROW> = firestoreQuery
      .stream()
      .on('error', err => stream.emit('error', err))
      .pipe(
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

    return stream
  }

  // SAVE
  override async saveBatch<ROW extends Partial<ObjectWithId>>(
    table: string,
    rows: ROW[],
    opt: FirestoreDBSaveOptions<ROW> = {},
  ): Promise<void> {
    const { firestore } = this.cfg
    const col = firestore.collection(table)
    const method: SaveOp = methodMap[opt.saveMethod!] || 'set'

    if (opt.tx) {
      const { tx } = opt.tx as FirestoreDBTransaction

      rows.forEach(row => {
        _assert(
          row.id,
          `firestore-db doesn't support id auto-generation, but empty id was provided in saveBatch`,
        )

        tx[method as 'set' | 'create'](col.doc(escapeDocId(row.id)), _filterUndefinedValues(row))
      })
      return
    }

    // Firestore allows max 500 items in one batch
    await pMap(
      _chunk(rows, 500),
      async chunk => {
        const batch = firestore.batch()

        chunk.forEach(row => {
          _assert(
            row.id,
            `firestore-db doesn't support id auto-generation, but empty id was provided in saveBatch`,
          )
          batch[method as 'set' | 'create'](
            col.doc(escapeDocId(row.id)),
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
    let ids: string[]

    const idFilter = q._filters.find(f => f.name === 'id')
    if (idFilter) {
      ids = Array.isArray(idFilter.val) ? idFilter.val : [idFilter.val]
    } else {
      const firestoreQuery = dbQueryToFirestoreQuery(
        q.select([]),
        this.cfg.firestore.collection(q.table),
      )
      ids = (await this.runFirestoreQuery<ROW>(firestoreQuery)).map(obj => obj.id)
    }

    await this.deleteByIds(q.table, ids, opt)

    return ids.length
  }

  override async deleteByIds(
    table: string,
    ids: string[],
    opt: FirestoreDBOptions = {},
  ): Promise<number> {
    const { firestore } = this.cfg
    const col = firestore.collection(table)

    if (opt.tx) {
      const { tx } = opt.tx as FirestoreDBTransaction

      ids.forEach(id => {
        tx.delete(col.doc(escapeDocId(id)))
      })
      return ids.length
    }

    await pMap(_chunk(ids, 500), async chunk => {
      const batch = firestore.batch()

      chunk.forEach(id => {
        batch.delete(col.doc(escapeDocId(id)))
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

  // override async commitTransaction(tx: DBTransaction, _opt?: CommonDBSaveOptions): Promise<void> {
  //   const { firestore } = this.cfg
  //
  //   await firestore.runTransaction(async tr => {
  //     for (const op of tx.ops) {
  //       if (op.type === 'saveBatch') {
  //         op.rows.forEach(row => {
  //           tr.set(
  //             firestore.collection(op.table).doc(escapeDocId(row.id)),
  //             _filterUndefinedValues(row),
  //           )
  //         })
  //       } else if (op.type === 'deleteByIds') {
  //         op.ids.forEach(id => {
  //           tr.delete(firestore.collection(op.table).doc(escapeDocId(id)))
  //         })
  //       } else {
  //         throw new Error(`DBOperation not supported: ${(op as any).type}`)
  //       }
  //     }
  //   })
  // }

  override async createTransaction(): Promise<FirestoreDBTransaction> {
    return await FirestoreDBTransaction.create(this)
  }

  override async ping(): Promise<void> {
    // no-op now
  }

  override async getTables(): Promise<string[]> {
    return []
  }
}

/**
 * https://firebase.google.com/docs/firestore/manage-data/transactions
 */
export class FirestoreDBTransaction implements DBTransaction {
  /**
   * This defer is held during Transaction and
   * is released when it's ready to be committed or rolled back.
   */
  // private txPendingDefer = pDefer()

  /**
   * This is resolved after Transaction is committed or rolled back.
   * On error - it rejects with that error.
   */
  // private txCompletedDefer = pDefer()

  private constructor(
    public db: FirestoreDB,
    public tx: Transaction,
    private txPendingDefer: DeferredPromise,
    private txCompletedDefer: DeferredPromise,
  ) {}

  static async create(db: FirestoreDB): Promise<FirestoreDBTransaction> {
    const txCreated = pDefer<Transaction>()
    const txPendingDefer = pDefer()
    const txCompletedDefer = pDefer()

    db.cfg.firestore
      .runTransaction(async tx => {
        txCreated.resolve(tx)

        // Now we pause and let consumers to use the Transaction,
        // until commit/rollback is called
        await txPendingDefer
      })
      .then(() => {
        txCompletedDefer.resolve()
      })
      .catch(err => {
        txCompletedDefer.reject(err)
      })

    const tx = await txCreated
    return new FirestoreDBTransaction(db, tx, txPendingDefer, txCompletedDefer)
  }

  async commit(): Promise<void> {
    this.txPendingDefer.resolve()
    await this.txCompletedDefer
  }
  async rollback(): Promise<void> {
    this.txPendingDefer.reject(new Error('rollback'))
    await this.txCompletedDefer
  }
}
