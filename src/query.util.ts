import { Query, WhereFilterOp } from '@google-cloud/firestore'
import { DBQuery } from '@naturalcycles/db-lib'
import { StringMap } from '@naturalcycles/js-lib'

// Map DBQueryFilterOp to WhereFilterOp
const OP_MAP: StringMap<WhereFilterOp> = {
  '=': '==',
  in: 'array-contains',
}

export function dbQueryToFirestoreQuery (dbQuery: DBQuery, emptyQuery: Query): Query {
  // filter
  let q = dbQuery._filters.reduce((q, f) => {
    return q.where(f.name, OP_MAP[f.op] || f.op, f.val)
  }, emptyQuery)

  // order
  q = dbQuery._orders.reduce((q, ord) => {
    return q.orderBy(ord.name, ord.descending ? 'desc' : 'asc')
  }, q)

  // limit
  q = q.limit(dbQuery._limitValue)

  // selectedFields
  if (dbQuery._selectedFieldNames) {
    q = q.select(...dbQuery._selectedFieldNames)
  }

  return q
}
