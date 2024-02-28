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
import DetailsIconGroupWrapper from '@src/components/DetailsPage/DetailsIconGroupWrapper'

describe('DetailsIconGroupWrapper.vue', () => {
    let wrapper

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: DetailsIconGroupWrapper,
        })
    })

    it(`By default, should not add '.icon-group__multi' class`, () => {
        expect(wrapper.find('.icon-group__multi').exists()).to.be.equal(false)
    })

    it(`Should add '.icon-group__multi' class when multiIcons props is true`, async () => {
        await wrapper.setProps({ multiIcons: true })
        expect(wrapper.find('.icon-group__multi').exists()).to.be.equal(true)
    })

    it(`Should render accurate content when body slot is used`, () => {
        wrapper = mount({
            shallow: false,
            component: DetailsIconGroupWrapper,
            slots: {
                body: '<div class="details-icon-group-wrapper__body">body div</div>',
            },
        })

        expect(wrapper.find('.details-icon-group-wrapper__body').text()).to.be.equal('body div')
    })
})
