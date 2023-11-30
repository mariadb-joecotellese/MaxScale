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
import MxsSelDlg from '@share/components/common/MxsSelDlg'
import { itemSelectMock } from '@tests/unit/utils'

const initialProps = {
    value: false, // control visibility of the dialog
    mode: 'change', // Either change or add
    title: 'Change monitor',
    entityName: 'monitors', // always plural
    onSave: () => null,
    itemsList: [
        { id: 'Monitor', type: 'monitors' },
        { id: 'Monitor-test', type: 'monitors' },
    ],
    //optional props
    multiple: false,
    defaultItems: undefined,
}

describe('MxsSelDlg.vue', () => {
    let wrapper

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: MxsSelDlg,
            propsData: initialProps,
        })
    })

    it(`Should render correct label value when multiple props is true`, () => {
        // component need to be shallowed, ignoring child components
        wrapper = mount({
            shallow: true,
            component: MxsSelDlg,
            propsData: { ...initialProps, multiple: true },
        })
        // get the label p
        let label = wrapper.find('.select-label').html()
        // check include correct label value
        expect(label).to.be.include('Specify monitors')
    })
    it(`Should render correct label value when multiple props is false`, () => {
        // component need to be shallowed, ignoring child components

        wrapper = mount({
            shallow: true,
            component: MxsSelDlg,
            propsData: initialProps,
        })
        // get the label p
        let label = wrapper.find('.select-label').html()
        // check include correct label value
        expect(label).to.be.include('Specify a monitor')
    })

    it(`Should emit on-open event when dialog is opened`, async () => {
        let count = 0
        wrapper.vm.$on('on-open', () => {
            count++
        })
        await wrapper.setProps({ value: true }) // open dialog
        expect(count).to.be.equal(1)
    })

    it(`Should emit selected-items event and returns accurate selected items`, async () => {
        let chosenItems = []
        wrapper.vm.$on('selected-items', items => {
            chosenItems = items
        })
        await wrapper.setProps({ value: true }) // open dialog
        // mockup onchange event when selecting item
        await itemSelectMock(wrapper, { id: 'Monitor-test', type: 'monitors' })

        expect(chosenItems).to.be.an('array')
        expect(chosenItems[0].id).to.be.equal('Monitor-test')
    })

    it(`Should clear selectedItems when dialog is closed`, async () => {
        await wrapper.setProps({ value: true }) // open dialog
        // stub selecting an item
        await wrapper.setData({
            selectedItems: [{ id: 'Monitor-test', type: 'monitors' }],
        })
        expect(wrapper.vm.$data.selectedItems.length).to.be.equal(1)
        await wrapper.setProps({ value: false }) // close dialog
        expect(wrapper.vm.$data.selectedItems.length).to.be.equal(0)
    })

    it(`Should pass the following props to mxs-select`, () => {
        const selectDropdown = wrapper.findComponent({ name: 'mxs-select' })
        const {
            entityName,
            items,
            defaultItems,
            multiple,
            clearable,
            showPlaceHolder,
        } = selectDropdown.vm.$props

        const {
            entityName: wrapperEntityName,
            itemsList,
            defaultItems: wrapperDefaultItems,
            multiple: wrapperMultiple,
            clearable: wrapperClearable,
        } = wrapper.vm.$props

        expect(entityName).to.be.equals(wrapperEntityName)
        expect(items).to.be.deep.equals(itemsList)
        expect(defaultItems).to.be.deep.equals(wrapperDefaultItems)
        expect(multiple).to.be.equals(wrapperMultiple)
        expect(clearable).to.be.equals(wrapperClearable)
        expect(showPlaceHolder).to.be.false
    })

    it(`Should render accurate content when body-append slot is used`, () => {
        wrapper = mount({
            shallow: false,
            component: MxsSelDlg,
            propsData: initialProps,
            slots: {
                'body-append': '<small class="body-append">body append</small>',
            },
        })

        expect(wrapper.find('.body-append').text()).to.be.equal('body append')
    })
})
