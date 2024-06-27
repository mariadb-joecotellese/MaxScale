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
import {
  findParenLevelToken,
  nextNonCommentNonWhitespace,
  findLimitInsertionIndex,
} from 'sql-limiter/src/token-utils'
import createToken from 'sql-limiter/src/create-token'

/**
 * Supported offset syntaxes
 * LIMIT <offset_number>,<limit_number>
 * LIMIT <limit_number> OFFSET <offset_number>
 * OFFSET <offset_number> { ROW | ROWS }
 * @param {*} tokens
 * @param {number} startingIndex
 * @returns {object|null} - Token representing the offset number, or null if not found
 */
export function has(tokens, startingIndex) {
  const limitKeywordToken = findParenLevelToken(
    tokens,
    startingIndex,
    (token) => token.type === 'keyword' && token.value === 'limit'
  )
  let offsetToken = null

  if (!limitKeywordToken) {
    const offsetKeywordToken = findParenLevelToken(
      tokens,
      startingIndex,
      (token) => token.type === 'keyword' && token.value === 'offset'
    )
    if (!offsetKeywordToken) return null
    const offsetNumberToken = nextNonCommentNonWhitespace(tokens, offsetKeywordToken.index + 1)

    if (!offsetNumberToken || offsetNumberToken.type !== 'number')
      throw new Error('Expected number after OFFSET or ROW/ROWS')

    offsetToken = offsetNumberToken
    const rowsToken = nextNonCommentNonWhitespace(tokens, offsetToken.index + 1)

    if (!rowsToken) throw new Error('Expected ROW or ROWS after offset_number')

    if (
      rowsToken &&
      (rowsToken.value.toLowerCase() === 'row' || rowsToken.value.toLowerCase() === 'rows')
    ) {
      // It's OFFSET <offset_number> ROW/ROWS
      return offsetToken
    }
  }

  const firstNumber = nextNonCommentNonWhitespace(tokens, limitKeywordToken.index + 1)

  if (!firstNumber) throw new Error('Unexpected end of statement')

  if (firstNumber.type !== 'number') throw new Error(`Expected number got ${firstNumber.type}`)

  const possibleCommaOrOffset = nextNonCommentNonWhitespace(tokens, firstNumber.index + 1)

  if (possibleCommaOrOffset) {
    if (possibleCommaOrOffset.type === 'comma') {
      // If it's a comma, check for the second number (LIMIT <offset_number>,<limit_number> syntax)
      const secondNumber = nextNonCommentNonWhitespace(tokens, possibleCommaOrOffset.index + 1)

      if (!secondNumber) throw new Error('Unexpected end of statement')

      if (secondNumber.type !== 'number')
        throw new Error(`Expected number got ${secondNumber.type}`)

      // Assign the first number to offsetToken
      offsetToken = firstNumber
    } else if (
      possibleCommaOrOffset.type === 'keyword' &&
      possibleCommaOrOffset.value.toLowerCase() === 'offset'
    ) {
      // If it's an OFFSET keyword, check for the offset number
      const offsetNumber = nextNonCommentNonWhitespace(tokens, possibleCommaOrOffset.index + 1)

      if (!offsetNumber) throw new Error('Unexpected end of statement')

      if (offsetNumber.type !== 'number')
        throw new Error(`Expected number got ${offsetNumber.type}`)

      offsetToken = offsetNumber
    }
  }

  return offsetToken
}

/**
 * Adds offset to query that does not have it
 * @param {*} queryTokens
 * @param {*} statementKeywordIndex
 * @param {*} targetParenLevel
 * @param {*} offset
 */
export function add(queryTokens, statementKeywordIndex, targetParenLevel, offset) {
  // Find the limit token, if present
  const limitToken = findParenLevelToken(
    queryTokens,
    statementKeywordIndex,
    (token) => token.type === 'keyword' && token.value === 'limit'
  )

  if (limitToken) {
    // Insert OFFSET after the LIMIT clause
    const nextToken = nextNonCommentNonWhitespace(queryTokens, limitToken.index + 2)

    const firstHalf = queryTokens.slice(0, nextToken.index + 1)
    const secondHalf = queryTokens.slice(nextToken.index + 1)
    return [
      ...firstHalf,
      createToken.singleSpace(),
      createToken.keyword('offset'),
      createToken.singleSpace(),
      createToken.number(offset),
      ...secondHalf,
    ]
  }

  // If there is a terminator add OFFSET just before it
  const terminatorToken = findParenLevelToken(
    queryTokens,
    statementKeywordIndex,
    (token) => token.type === 'terminator'
  )

  if (terminatorToken) {
    const firstHalf = queryTokens.slice(0, terminatorToken.index)
    const secondHalf = queryTokens.slice(terminatorToken.index)
    return [
      ...firstHalf,
      createToken.singleSpace(),
      createToken.keyword('offset'),
      createToken.singleSpace(),
      createToken.number(offset),
      ...secondHalf,
    ]
  }

  // No LIMIT and no terminator. Append OFFSET to the end
  const targetIndex = findLimitInsertionIndex(queryTokens, targetParenLevel)
  const firstHalf = queryTokens.slice(0, targetIndex)
  const secondHalf = queryTokens.slice(targetIndex)
  return [
    ...firstHalf,
    createToken.singleSpace(),
    createToken.keyword('offset'),
    createToken.singleSpace(),
    createToken.number(offset),
    ...secondHalf,
  ]
}
