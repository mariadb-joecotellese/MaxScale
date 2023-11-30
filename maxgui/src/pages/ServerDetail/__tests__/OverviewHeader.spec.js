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
import OverviewHeader from '@rootSrc/pages/ServerDetail/OverviewHeader'
import {
    dummy_all_servers,
    dummy_all_monitors,
    triggerBtnClick,
    itemSelectMock,
} from '@tests/unit/utils'

const propsMountFactory = propsData =>
    mount({
        shallow: false,
        component: OverviewHeader,
        propsData,
    })

const getRelationshipDataStub = () => dummy_all_monitors

/**
 * @param {Object} wrapper mounted component
 * @param {String} btnSelector css selector for the button that trigger opening mxs-sel-dlg
 *
 */
const openSelectDialogMock = async (wrapper, btnSelector) => {
    await wrapper.setData({
        showEditBtn: true,
    })
    await triggerBtnClick(wrapper, btnSelector)
}

describe('ServerDetail - OverviewHeader', () => {
    let wrapper
    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: OverviewHeader,
            propsData: {
                currentServer: dummy_all_servers[0],
                getRelationshipData: getRelationshipDataStub,
            },
        })
    })

    describe('outlined-overview-card render counts assertions', () => {
        // 6 means when server is using port, 5 is when server is using socket
        const countCases = [6, 5]

        countCases.forEach(count => {
            let des = `Should render ${count} outlined-overview-card components`
            if (count === 6) des += 'if server is using port'
            else des += 'if server is using socket'
            it(des, () => {
                let currentServer = wrapper.vm.$helpers.lodash.cloneDeep(dummy_all_servers[0])
                currentServer.attributes.parameters = {
                    address: '127.0.0.1',
                    port: count === 6 ? 4001 : null,
                    socket: count === 6 ? null : 'tmp/maxscale.sock',
                }
                wrapper = propsMountFactory({
                    currentServer: currentServer,
                })

                const outlineOverviewCards = wrapper.findAllComponents({
                    name: 'outlined-overview-card',
                })
                expect(outlineOverviewCards.length).to.be.equals(count)
            })
        })
    })

    it(`Should automatically assign 'undefined' string if attribute is not defined`, () => {
        let currentServer = wrapper.vm.$helpers.lodash.cloneDeep(dummy_all_servers[0])
        delete currentServer.attributes.last_event
        delete currentServer.attributes.triggered_at

        wrapper = propsMountFactory({
            currentServer: currentServer,
        })
        const { last_event, triggered_at } = wrapper.vm.getTopOverviewInfo

        expect(last_event).to.be.equals('undefined')
        expect(triggered_at).to.be.equals('undefined')
    })

    describe('Should show accurate keys', () => {
        // 6 means when server is using port, 5 is when server is using socket
        const countCases = [6, 5]

        countCases.forEach(count => {
            let des = `Should show `
            let expectKeys = []
            if (count === 6) {
                expectKeys = ['address', 'port', 'state', 'last_event', 'triggered_at', 'monitor']
            } else {
                des += 'the following keys if server is using socket: '
                expectKeys = ['socket', 'state', 'last_event', 'triggered_at', 'monitor']
            }
            des += expectKeys.join(', ')

            it(des, () => {
                let currentServer = wrapper.vm.$helpers.lodash.cloneDeep(dummy_all_servers[0])
                currentServer.attributes.parameters = {
                    address: '127.0.0.1',
                    port: count === 6 ? 4001 : null,
                    socket: count === 6 ? null : 'tmp/maxscale.sock',
                }
                wrapper = propsMountFactory({
                    currentServer: currentServer,
                })
                const getTopOverviewInfo = wrapper.vm.getTopOverviewInfo
                expect(Object.keys(getTopOverviewInfo)).to.be.deep.equals(expectKeys)
            })
        })
    })

    it(`Should pass necessary props to mxs-sel-dlg`, () => {
        const selectDialog = wrapper.findComponent({
            name: 'mxs-sel-dlg',
        })
        expect(selectDialog.exists()).to.be.true
        const { entityName, itemsList, defaultItems, clearable } = selectDialog.vm.$props
        const { title, saveText, onSave } = selectDialog.vm.$attrs
        const {
            dialogTitle,
            targetSelectItemType,
            itemsList: itemsListData,
            defaultItems: defaultItemsData,
        } = wrapper.vm.$data

        expect(title).to.be.equals(dialogTitle)
        expect(saveText).to.be.equals('change')
        expect(entityName).to.be.equals(targetSelectItemType)
        expect(clearable).to.be.true
        expect(onSave).to.be.equals(wrapper.vm.confirmChange)
        expect(itemsList).to.be.deep.equals(itemsListData)
        expect(defaultItems).to.be.deep.equals(defaultItemsData)
    })

    it(`Should open 'Change monitor' dialog with accurate title and label`, async () => {
        await openSelectDialogMock(wrapper, '.monitor-edit-btn')
        const selectDialog = wrapper.findComponent({ name: 'mxs-sel-dlg' })
        expect(selectDialog.vm.$attrs.value).to.be.true
        const title = selectDialog.find('h3')
        expect(title.text()).to.be.equals('Change monitor')
        const label = selectDialog.find('.select-label')
        expect(label.text()).to.be.equals('Specify a monitor')
    })

    it(`Should show all monitors in mxs-sel-dlg`, async () => {
        await openSelectDialogMock(wrapper, '.monitor-edit-btn')

        const selectDialog = wrapper.findComponent({
            name: 'mxs-sel-dlg',
        })
        const itemsList = selectDialog.vm.$props.itemsList
        expect(itemsList.length).to.be.equals(dummy_all_monitors.length)
        itemsList.forEach((item, i) => {
            expect(item).to.have.all.keys('id', 'type')
            expect(item).have.property('id', dummy_all_monitors[i].id)
            expect(item).have.property('type', dummy_all_monitors[i].type)
        })
    })

    it(`Should set defaultItems in mxs-sel-dlg if server is monitored`, async () => {
        await openSelectDialogMock(wrapper, '.monitor-edit-btn')

        expect(wrapper.vm.$data.defaultItems).to.be.deep.equals(
            dummy_all_servers[0].relationships.monitors.data[0]
        )
    })

    it(`Should emit on-relationship-update event`, async () => {
        let eventFired = 0
        wrapper.vm.$on('on-relationship-update', ({ type, data }) => {
            eventFired++
            expect(data).to.be.an('array')
            expect(data.length).to.be.equals(1)
            expect(data[0]).to.be.deep.equals(chosenItem)
            expect(type).to.be.equals('monitors')
        })

        await openSelectDialogMock(wrapper, '.monitor-edit-btn')
        const itemsList = wrapper.vm.$data.itemsList
        const chosenItem = itemsList[itemsList.length - 1]
        await itemSelectMock(wrapper, chosenItem)
        await wrapper.vm.confirmChange()

        expect(eventFired).to.be.equals(1)
    })
})
