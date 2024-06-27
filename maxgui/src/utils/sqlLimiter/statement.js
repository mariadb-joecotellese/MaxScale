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
import Stmt from 'sql-limiter/src/statement'
import * as offset from '@/utils/sqlLimiter/offset'

export default class Statement extends Stmt {
  constructor() {
    super()
  }

  /**
   * @param {number} offsetNumber
   */
  enforceOffset(offsetNumber) {
    const { statementToken, tokens } = this

    if (statementToken && statementToken.value === 'select') {
      const offsetToken = offset.has(tokens, statementToken.index)
      // If offset token exists already, return early
      if (offsetToken) return

      // Offset clause was not found, so add it
      this.tokens = offset.add(
        tokens,
        statementToken.index,
        statementToken.parenLevel,
        offsetNumber
      )
    }
  }
}
