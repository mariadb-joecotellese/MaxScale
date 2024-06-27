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
import getStatements from '@/utils/sqlLimiter/getStatements'
/**
 * Enforce limit/top on SQL SELECT queries.
 * Non SELECT queries will not be altered.
 * If existing limit exists, it will be lowered if it is larger than `limitNumber` specified
 * If limit does not exist, it will be added.
 * Returns SQL text with limits enforced.
 *
 * @param {string} sql - sql text to limit
 * @param {Array<String>|String} limitStrategies -- First strategy value takes priority if no limit exists
 * @param {number} limitNumber -- number to enforce for limit keyword
 * @param {Number} [offsetNumber] -- number to enforce for offset keyword
 * @returns {string}
 */
export function limit({ sql, limitStrategies = ['limit', 'fetch'], limitNumber, offsetNumber }) {
  if (typeof sql !== 'string') {
    throw new Error('sql must be string')
  }
  if (typeof limitNumber !== 'number') {
    throw new Error('limitNumber must be number')
  }

  let strategies = typeof limitStrategies === 'string' ? [limitStrategies] : limitStrategies

  if (!Array.isArray(strategies)) {
    throw new Error('limitStrategies must be an array or string')
  }

  if (strategies.length === 0) {
    throw new Error('limitStrategies must not be empty')
  }

  strategies = strategies.map((s) => s.toLowerCase())
  return getStatements(sql)
    .map((statement) => {
      statement.enforceLimit(strategies, limitNumber)
      if (typeof offsetNumber === 'number') statement.enforceOffset(offsetNumber)
      return statement.toString()
    })
    .join('')
}
