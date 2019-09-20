import { replaceAll } from './string.util'

const SLASH = '_SLASH_'

export function escapeDocId(docId: string): string {
  return replaceAll(docId, '/', SLASH)
}

export function unescapeDocId(docId: string): string {
  return replaceAll(docId, SLASH, '/')
}
