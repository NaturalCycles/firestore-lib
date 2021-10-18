import { _replaceAll } from '@naturalcycles/js-lib'

const SLASH = '_SLASH_'

export function escapeDocId(docId: string): string {
  return _replaceAll(docId, '/', SLASH)
}

export function unescapeDocId(docId: string): string {
  return _replaceAll(docId, SLASH, '/')
}
