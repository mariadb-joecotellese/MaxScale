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
import PageHeader from '@src/pages/ListenerDetail/PageHeader'

import { dummy_all_listeners, triggerBtnClick, openConfirmDialog } from '@tests/unit/utils'

describe('ListenerDetail - PageHeader', () => {
    let wrapper, axiosStub

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: PageHeader,
            propsData: {
                currentListener: dummy_all_listeners[0],
            },
        })
        axiosStub = sinon.stub(wrapper.vm.$http, 'delete').returns(Promise.resolve())
    })

    afterEach(async () => {
        await axiosStub.restore()
    })

    it(`Should render listener state accurately`, () => {
        const span = wrapper.find('.resource-state')
        expect(span.exists()).to.be.true
        expect(span.text()).to.be.equals(dummy_all_listeners[0].attributes.state)
    })

    it(`Should pass necessary props to confirm-dlg`, () => {
        const confirmDialog = wrapper.findComponent({ name: 'confirm-dlg' })
        expect(confirmDialog.exists()).to.be.true

        const { type, item } = confirmDialog.vm.$props
        const { title, onSave, value } = confirmDialog.vm.$attrs
        const { dialogTitle, dialogType, isConfDlgOpened } = wrapper.vm.$data

        expect(value).to.be.equals(isConfDlgOpened)
        expect(title).to.be.equals(dialogTitle)
        expect(type).to.be.equals(dialogType)
        expect(item).to.be.deep.equals(wrapper.vm.$props.currentListener)
        expect(onSave).to.be.equals(wrapper.vm.confirmSave)
    })

    it(`Should open confirm-dlg when delete button is clicked`, async () => {
        await openConfirmDialog({ wrapper, cssSelector: '.delete-btn' })
        const confirmDialog = wrapper.findComponent({ name: 'confirm-dlg' })
        expect(confirmDialog.vm.$attrs.value).to.be.true
    })

    it(`Should send delete request after confirming delete`, async () => {
        await openConfirmDialog({ wrapper, cssSelector: '.delete-btn' })
        const confirmDialog = wrapper.findComponent({ name: 'confirm-dlg' })
        await triggerBtnClick(confirmDialog, '.save')

        await axiosStub.should.have.been.calledWith(`/listeners/${dummy_all_listeners[0].id}`)
    })
})
