import { Query, WhereFilterOp } from '@google-cloud/firestore'
import { DBQuery, DBQueryFilterOperator } from '@naturalcycles/db-lib'

// Map DBQueryFilterOp to WhereFilterOp
// Currently it's fully aligned!
const OP_MAP: Partial<Record<DBQueryFilterOperator, WhereFilterOp>> = {
  // '=': '==',
  // in: 'array-contains',
}

export function dbQueryToFirestoreQuery(dbQuery: DBQuery, emptyQuery: Query): Query {
  // filter
  let q = dbQuery._filters.reduce((q, f) => {
    return q.where(f.name, OP_MAP[f.op] || (f.op as WhereFilterOp), f.val)
  }, emptyQuery)

  // order
  q = dbQuery._orders.reduce((q, ord) => {
    return q.orderBy(ord.name, ord.descending ? 'desc' : 'asc')
  }, q)

  // limit
  q = q.limit(dbQuery._limitValue)

  // selectedFields
  if (dbQuery._selectedFieldNames) {
    // todo: check if at least id / __key__ is required to be set
    q = q.select(...dbQuery._selectedFieldNames)
  }

  return q
}
