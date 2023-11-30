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
import Filters from '@rootSrc/pages/Dashboard/Filters'

import {
    dummy_all_filters,
    findAnchorLinkInTable,
    getUniqueResourceNamesStub,
} from '@tests/unit/utils'

const expectedTableHeaders = [
    { text: 'Filter', value: 'id', autoTruncate: true },
    { text: 'Service', value: 'serviceIds', autoTruncate: true },
    { text: 'Module', value: 'module' },
]

const expectedTableRows = [
    {
        id: 'filter_0',
        serviceIds: ['service_0', 'service_1'],
        module: 'qlafilter',
    },
    {
        id: 'filter_1',
        serviceIds: ['service_1'],
        module: 'binlogfilter',
    },
]

describe('Dashboard Filters tab', () => {
    let wrapper, axiosStub

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: Filters,
            computed: {
                all_filters: () => dummy_all_filters,
            },
        })
        axiosStub = sinon.stub(wrapper.vm.$http, 'get').resolves(Promise.resolve({ data: {} }))
    })

    afterEach(() => {
        axiosStub.restore()
    })

    it(`Should process table rows accurately`, () => {
        expect(wrapper.vm.tableRows).to.be.deep.equals(expectedTableRows)
    })

    it(`Should pass expected table headers to data-table`, () => {
        const dataTable = wrapper.findComponent({ name: 'data-table' })
        expect(wrapper.vm.tableHeaders).to.be.deep.equals(expectedTableHeaders)
        expect(dataTable.vm.$props.headers).to.be.deep.equals(expectedTableHeaders)
    })

    it(`Should navigate to service detail page when a service is clicked`, async () => {
        const filterId = dummy_all_filters[1].id
        const serviceId = dummy_all_filters[1].relationships.services.data[0].id
        const aTag = findAnchorLinkInTable({
            wrapper: wrapper,
            rowId: filterId,
            cellIndex: expectedTableHeaders.findIndex(item => item.value === 'serviceIds'),
        })
        await aTag.trigger('click')
        wrapper.vm.$nextTick(() =>
            expect(wrapper.vm.$route.path).to.be.equals(`/dashboard/services/${serviceId}`)
        )
    })

    it(`Should navigate to filter detail page when a filter is clicked`, async () => {
        const filterId = dummy_all_filters[0].id
        const aTag = findAnchorLinkInTable({
            wrapper: wrapper,
            rowId: filterId,
            cellIndex: expectedTableHeaders.findIndex(item => item.value === 'id'),
        })
        await aTag.trigger('click')
        wrapper.vm.$nextTick(() =>
            expect(wrapper.vm.$route.path).to.be.equals(`/dashboard/filters/${filterId}`)
        )
    })

    it(`Should get total number of unique service names accurately`, () => {
        const uniqueServiceNames = getUniqueResourceNamesStub(expectedTableRows, 'serviceIds')
        expect(wrapper.vm.$data.servicesLength).to.be.equals(uniqueServiceNames.length)
    })
})
