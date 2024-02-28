/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

import mount from '@tests/unit/setup'
import LogLine from '@src/pages/Logs/LogLine'
import { dummy_log_data } from '@tests/unit/utils'

const mountFactory = () =>
    mount({
        shallow: false,
        component: LogLine,
        propsData: {
            log: dummy_log_data[0],
        },
    })

describe('LogLine', () => {
    let wrapper
    beforeEach(() => {
        wrapper = mountFactory()
    })
    it(`Should return accurate mxs-color-helper classes for log level section`, async () => {
        dummy_log_data.forEach(async log => {
            await wrapper.setProps({ log })
            const logLevelEle = wrapper.find('.log-level')
            const classes = logLevelEle.classes().join(' ')
            expect(classes).to.be.equals(
                `log-level d-inline-flex justify-start mxs-color-helper text-${log.priority} ${
                    log.priority === 'alert' ? 'font-weight-bold' : ''
                }`
            )
        })
    })
    it(`Should assign accurate classes for log message section`, async () => {
        dummy_log_data.forEach(async log => {
            await wrapper.setProps({ log })
            const logMsg = wrapper.find('.text-wrap')
            const classes = logMsg.classes().join(' ')
            expect(classes).to.be.equals(
                `text-wrap mxs-color-helper text-${log.priority} ${
                    log.priority === 'alert' ? 'font-weight-bold' : ''
                }`
            )
        })
    })
})
