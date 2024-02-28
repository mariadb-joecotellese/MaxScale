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
import Services from '@src/pages/Dashboard/Services'

import {
    dummy_all_services,
    findAnchorLinkInTable,
    getUniqueResourceNamesStub,
} from '@tests/unit/utils'

const expectedTableHeaders = [
    { text: 'Service', value: 'id', autoTruncate: true },
    { text: 'State', value: 'state' },
    { text: 'Router', value: 'router' },
    { text: 'Current Sessions', value: 'connections', autoTruncate: true },
    { text: 'Total Sessions', value: 'total_connections', autoTruncate: true },
    { text: 'routing targets', value: 'routingTargets', autoTruncate: true },
]

const expectedTableRows = [
    {
        id: 'service_0',
        state: 'Started',
        router: 'readconnroute',
        connections: 0,
        total_connections: 1000001,
        routingTargets: [{ id: 'row_server_0', type: 'servers' }],
    },
    {
        id: 'service_1',
        state: 'Started',
        router: 'readwritesplit',
        connections: 0,
        total_connections: 0,
        routingTargets: 'No routing targets',
    },
]

describe('Dashboard Services tab', () => {
    let wrapper, axiosStub

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: Services,
            computed: {
                all_services: () => dummy_all_services,
            },
        })
        axiosStub = sinon.stub(wrapper.vm.$http, 'get').resolves(
            Promise.resolve({
                data: {},
            })
        )
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
        const serviceId = dummy_all_services[0].id
        const aTag = findAnchorLinkInTable({
            wrapper: wrapper,
            rowId: serviceId,
            cellIndex: expectedTableHeaders.findIndex(item => item.value === 'id'),
        })
        await aTag.trigger('click')
        expect(wrapper.vm.$route.path).to.be.equals(`/dashboard/services/${serviceId}`)
    })

    it(`Should navigate to server detail page when a server is clicked`, async () => {
        const serviceId = dummy_all_services[0].id
        const serverId = dummy_all_services[0].relationships.servers.data[0].id
        const aTag = findAnchorLinkInTable({
            wrapper: wrapper,
            rowId: serviceId,
            cellIndex: expectedTableHeaders.findIndex(item => item.value === 'routingTargets'),
        })
        await aTag.trigger('click')
        expect(wrapper.vm.$route.path).to.be.equals(`/dashboard/servers/${serverId}`)
    })

    it(`Should get total number of unique routing target names accurately`, () => {
        const uniqueRoutingTargetNames = getUniqueResourceNamesStub(
            expectedTableRows,
            'routingTargets'
        )
        expect(wrapper.vm.$data.routingTargetsLength).to.be.equals(uniqueRoutingTargetNames.length)
    })
})
