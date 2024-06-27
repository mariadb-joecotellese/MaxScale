/*
 * Copyright (c) 2023 MariaDB plc
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2029-02-28
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
import Statement from '@/utils/sqlLimiter/statement'
import keywords from 'sql-limiter/src/keywords'
import moo from 'moo'

// Incoming values will also be compared as lower case to make keyword matching case insensitive
const caseInsensitiveKeywords = (defs) => {
  const defineKeywords = moo.keywords(defs)
  return (value) => defineKeywords(value.toLowerCase())
}

const lexer = moo.compile({
  whitespace: [/[ \t]+/u, { match: /\r\n/u, lineBreaks: true }, { match: /\n/u, lineBreaks: true }],
  // First expression is --line comment, second is /* multi line */
  comment: [/--.*?$/u, /\/\*[^]*?\*\//u],
  lparen: '(',
  rparen: ')',
  comma: ',',
  period: '.',

  number: /0|[1-9][0-9]*/u,

  // ; is standard, \g is a shortcut used in psql and Actian tooling
  // Are there others?
  terminator: [';', '\\g'],

  // text == original text
  // value == value inside quotes
  quotedIdentifier: [
    {
      match: /".*?"/u,
      value: (x) => x.slice(1, -1),
    },
    {
      match: /\[.*?\]/u,
      value: (x) => x.slice(1, -1),
    },
    {
      match: /`.*?`/u,
      value: (x) => x.slice(1, -1),
    },
  ],

  // Updated to allow multi-line strings,
  // which is allowed by some database drivers (sqlite, actian)
  // This does not correctly handle escaped doublequotes, however the end result is ok for sql-limiter
  // Instead of a single string token we get 2 separate string tokens back-to-back
  string: [
    {
      match: /'[^']*'/u,
      lineBreaks: true,
    },
  ],

  // Remaining test is assumed to be an identifier of some kinds (column or table)
  // UNLESS it matches a keyword case insensitively
  // The value of these tokens are converted to lower case
  identifier: [
    {
      // This is added to handle non-english identifiers.
      // This range may be too broad
      // eslint-disable-next-line no-control-regex
      match: /(?:\w|[^\u0000-\u007F])+/u,
      type: caseInsensitiveKeywords({
        keyword: keywords,
      }),
      value: (s) => s.toLowerCase(),
    },
  ],

  // Any combination of special characters is to be treated as an operator (as a guess anyways)
  // Initially these were being noted here but the list is large
  // and there is no way to know all operators since this supports anything that is SQL-ish
  operator: {
    match: /[<>~!@#$%^?&|`*\-{}+=:/\\[\]]+/u,
    lineBreaks: false,
  },
})

/**
 * Takes SQL text and generates an array of tokens using moo
 * @param {string} sqlText
 */
export default function getStatements(sqlText) {
  const statements = []
  let statement = new Statement()

  lexer.reset(sqlText)
  let next = lexer.next()

  while (next) {
    statement.appendToken(next)
    if (statement.endReached) {
      statements.push(statement)
      statement = new Statement()
    }
    next = lexer.next()
  }
  // push last set
  if (statement.tokens.length) {
    statements.push(statement)
  }
  return statements
}
