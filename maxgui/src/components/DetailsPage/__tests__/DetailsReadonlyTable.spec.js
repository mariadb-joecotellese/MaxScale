/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

import mount from '@tests/unit/setup'
import DetailsReadonlyTable from '@src/components/DetailsPage/DetailsReadonlyTable'

const expectDefaultHeaders = [
    { text: 'Variable', value: 'id', width: '65%', autoTruncate: true },
    { text: 'Value', value: 'value', width: '35%', autoTruncate: true },
]

const dummy_data_obj = {
    active_operations: 0,
    adaptive_avg_select_time: '0ns',
    connections: 0,
    max_connections: 0,
    persistent_connections: 0,
    routed_packets: 0,
    total_connections: 0,
}

const dummy_processed_data_arr = [
    {
        id: '3063265',
        user: 'maxskysql@::ffff:127.0.0.1',
        connected: 'Tue Aug 25 16:09:23 2020',
        idle: 6.5,
    },
]

describe('DetailsReadonlyTable.vue', () => {
    let wrapper, processTableRowsSpy
    before(() => {
        // spy on processTableRows before mounting occurs
        processTableRowsSpy = sinon.spy(DetailsReadonlyTable.methods, 'processTableRows')
        wrapper = mount({
            shallow: false,
            component: DetailsReadonlyTable,
            propsData: {
                loading: false,
                title: 'STATISTICS',
                tableData: dummy_data_obj,
            },
            computed: {
                search_keyword: () => '',
            },
        })
    })

    it(`Should call processTableRows once time when component is mounted`, () => {
        processTableRowsSpy.should.have.been.calledOnce
    })

    it(`Should call processTableRows when tableData props changes`, async () => {
        // spy on processTableRows after mounting occurs
        const processTableRowsSpy = sinon.spy(wrapper.vm, 'processTableRows')
        // mockup props changes
        const tableData = wrapper.vm.$helpers.lodash.cloneDeep(wrapper.vm.$props.tableData)
        tableData.connections = 100
        await wrapper.setProps({
            tableData: tableData,
        })
        processTableRowsSpy.should.have.been.calledOnce
    })

    it(`Should render table headers having 'Variable' and 'Value' columns with
      with as 65%, 35%, respectively`, () => {
        expect(wrapper.vm.$data.tableHeaders).to.be.deep.equals(expectDefaultHeaders)
    })

    it(`Should use processed table data`, async () => {
        await wrapper.setProps({
            tableData: dummy_processed_data_arr,
        })
        expect(wrapper.vm.$data.tableRows).to.be.deep.equals(dummy_processed_data_arr)
    })

    it(`Should pass necessary props to data-table`, () => {
        const dataTable = wrapper.findComponent({
            name: 'data-table',
        })
        expect(dataTable.exists()).to.be.true
        const {
            search,
            headers,
            data,
            loading,
            tdBorderLeft,
            showAll,
            isTree,
            expandAll,
        } = dataTable.vm.$props

        const {
            search_keyword,
            isLoading,
            $data: { tableHeaders, tableRows },
            $props: { isTree: isTreeProps, expandAll: expandAllProps },
        } = wrapper.vm

        expect(search).to.be.equals(search_keyword)
        expect(headers).to.be.deep.equals(tableHeaders)
        expect(data).to.be.deep.equals(tableRows)
        expect(loading).to.be.equals(isLoading)
        expect(tdBorderLeft).to.be.true
        expect(showAll).to.be.true
        expect(isTree).to.be.equals(isTreeProps)
        expect(expandAll).to.be.equals(expandAllProps)
    })
})
