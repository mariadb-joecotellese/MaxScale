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
import * as sqlLimiter from '@/utils/sqlLimiter'

function test(sql, expected) {
  const enforcedSql = sqlLimiter.limit({ sql, limitNumber: 1000, offsetNumber: 15 })
  assert.equal(enforcedSql, expected)
}

describe('Enforce limit and offset', () => {
  it('limit and offset are not defined', () => {
    test(`SELECT * FROM something`, 'SELECT * FROM something limit 1000 offset 15')
  })

  it('limit is defined but offset is not defined', () => {
    test(`SELECT * FROM something limit 100`, 'SELECT * FROM something limit 100 offset 15')
  })

  it('limit is defined but larger than global limit', () => {
    test(`SELECT * FROM something limit 1200`, 'SELECT * FROM something limit 1000 offset 15')
  })

  it('Both limit and offset are defined', () => {
    test(`SELECT * FROM something limit 10 offset 5`, 'SELECT * FROM something limit 10 offset 5')
  })

  it('limit offset, row_count syntax', () => {
    test(`SELECT * FROM something limit 5, 10`, 'SELECT * FROM something limit 5, 10')
  })

  it('Only offset is defined', () => {
    test(
      `SELECT i FROM t1 ORDER BY i ASC offset 10 ROWS`,
      'SELECT i FROM t1 ORDER BY i ASC limit 1000 offset 10 ROWS'
    )
  })

  it('SELECT ... offset ... FETCH syntax', () => {
    test(
      `SELECT i FROM t1 ORDER BY i ASC offset 10 ROWS FETCH FIRST 2000 ROWS ONLY`,
      'SELECT i FROM t1 ORDER BY i ASC offset 10 ROWS FETCH FIRST 1000 ROWS ONLY'
    )
  })

  it('Multi statements', () => {
    test(
      `SELECT * FROM something;
       SELECT * FROM something limit 1200;
       SELECT * FROM something limit 10 offset 5;
       SELECT * FROM something limit 5, 10`,

      `SELECT * FROM something limit 1000 offset 15;
       SELECT * FROM something limit 1000 offset 15;
       SELECT * FROM something limit 10 offset 5;
       SELECT * FROM something limit 5, 10`
    )
  })

  it('sub-queries 1', () => {
    test(
      `SELECT * FROM (SELECT * FROM something) AS subquery_alias`,
      'SELECT * FROM (SELECT * FROM something) AS subquery_alias limit 1000 offset 15'
    )
  })

  it('sub-queries 2 ', () => {
    test(
      `select a, b from (select id b from (select seq id from seq_0_to_3 as id) t1) t1, (select seq a from seq_0_to_4 limit 2000) t2`,
      'select a, b from (select id b from (select seq id from seq_0_to_3 as id) t1) t1, (select seq a from seq_0_to_4 limit 2000) t2 limit 1000 offset 15'
    )
  })
})
