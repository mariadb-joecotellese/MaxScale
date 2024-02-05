/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

import mount from '@tests/unit/setup'
import DetailsPageTitle from '@src/components/DetailsPage/DetailsPageTitle'
import { routeChangesMock } from '@tests/unit/utils'

describe('DetailsPageTitle.vue', () => {
    let wrapper, axiosStub

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: DetailsPageTitle,
        })
        axiosStub = sinon.stub(wrapper.vm.$http, 'get').resolves(Promise.resolve({ data: {} }))
    })
    afterEach(() => {
        axiosStub.restore()
    })

    it(`Should render accurate page title`, async () => {
        wrapper = mount({
            shallow: true,
            component: DetailsPageTitle,
        })
        // go to a test page
        await routeChangesMock(wrapper, '/dashboard/servers/row_server_1')
        let pageTitleEle = wrapper.find('.page-title')
        expect(pageTitleEle.exists()).to.be.equal(true)
        expect(pageTitleEle.html()).to.be.include('row_server_1')
    })

    it(`Should render accurate content when setting-menu slot is used`, () => {
        wrapper = mount({
            shallow: true,
            component: DetailsPageTitle,
            slots: {
                'setting-menu': '<div class="details-page-title__setting-menu">setting-menu</div>',
            },
        })
        expect(wrapper.find('.details-page-title__setting-menu').text()).to.be.equal('setting-menu')
    })

    it(`Should render accurate content when append slot is used`, () => {
        wrapper = mount({
            shallow: false,
            component: DetailsPageTitle,
            slots: {
                append: '<div class="details-page-title__append">append</div>',
            },
        })
        expect(wrapper.find('.details-page-title__append').text()).to.be.equal('append')
    })
})
