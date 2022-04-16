import { _replaceAll } from '@naturalcycles/js-lib'

const SLASH = '_SLASH_'

export function escapeDocId(docId: string | number): string {
  if (typeof docId === 'number') return String(docId)
  return _replaceAll(docId, '/', SLASH)
}

export function unescapeDocId(docId: string): string {
  // if (typeof docId === 'number') return docId
  return _replaceAll(docId, SLASH, '/')
}
