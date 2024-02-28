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
import ParametersCollapse from '@src/components/ObjectForms/ParametersCollapse'
import { itemSelectMock, inputChangeMock } from '@tests/unit/utils'

const addressParam = {
    description: 'Server address',
    mandatory: false,
    modifiable: true,
    name: 'address',
    type: 'string',
}

const durationParam = {
    default_value: 0,
    description: 'Maximum time that a connection can be in the pool',
    mandatory: false,
    modifiable: true,
    name: 'persistmaxtime',
    type: 'duration',
    unit: 'ms',
}

const portParam = {
    default_value: 3306,
    description: 'Server port',
    mandatory: false,
    modifiable: true,
    name: 'port',
    type: 'count',
}

const enumParam = {
    default_value: 'primary',
    description: 'Server rank',
    enum_values: ['primary', 'secondary'],
    mandatory: false,
    modifiable: true,
    name: 'rank',
    type: 'enum',
}

const socketParam = {
    description: 'Server UNIX socket',
    mandatory: false,
    modifiable: true,
    name: 'socket',
    type: 'string',
}

const boolParam = {
    default_value: 'false',
    description: 'Enable TLS for server',
    mandatory: false,
    modifiable: false,
    name: 'ssl',
    type: 'bool',
}
const pathParam = {
    description: 'TLS public certificate',
    mandatory: false,
    modifiable: false,
    name: 'ssl_cert',
    type: 'path',
}

describe('ParametersCollapse.vue', () => {
    let wrapper

    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: ParametersCollapse,
            propsData: {
                parameters: [
                    addressParam,
                    durationParam,
                    portParam,
                    enumParam,
                    socketParam,
                    boolParam,
                    pathParam,
                ],
                search: '',
                objType: 'servers',
            },
        })
    })

    it(`Should show parameter info tooltip when hovering a cell`, async () => {
        const { wrappers: tds } = wrapper.findAll('td')
        await tds[0].trigger('mouseenter')
        expect(wrapper.vm.$data.parameterTooltip).to.not.be.null
    })

    it(`Should hide parameter info tooltip when hovering out a cell`, async () => {
        const { wrappers: tds } = wrapper.findAll('td')
        await tds[0].trigger('mouseenter')
        await tds[0].trigger('mouseleave')
        expect(wrapper.vm.$data.parameterTooltip).to.be.null
    })

    it(`Should render parameter-tooltip component when a cell is hovered `, async () => {
        const { wrappers: tds } = wrapper.findAll('td')
        await tds[0].trigger('mouseenter')
        const parameterTooltip = wrapper.findAllComponents({ name: 'parameter-tooltip' })
        expect(parameterTooltip.length).to.be.equals(1)
        expect(tds[0].vm.$props.item.id).to.be.equals(wrapper.vm.$data.parameterTooltip.id)
    })

    it(`Should render total number of table row on the first row of the first column`, () => {
        const { wrappers: ths } = wrapper.findAll('th')
        expect(ths.length).to.be.equals(2)
        ths.forEach((th, i) => {
            i === 0
                ? expect(th.find('.total-row').exists()).to.be.true
                : expect(th.find('.total-row').exists()).to.be.false
        })
    })

    it(`Should assign value of name attribute of a module parameter object
      to new id attribute and then delete name attribute`, async () => {
        await wrapper.setProps({
            parameters: [addressParam],
        })
        expect(wrapper.vm.parametersTableRow[0])
            .have.property('id', addressParam.name)
            .and.to.not.have.own.property('name')
    })

    it(`Should create new value attribute and assign null if a module parameter
      doesn't have default_value property`, async () => {
        await wrapper.setProps({
            parameters: [addressParam],
        })
        expect(addressParam.default_value).to.be.undefined
        expect(wrapper.vm.parametersTableRow[0].value).to.be.equals(null)
    })

    it(`Should assign default_value to value attribute if a module parameter
      has default_value property`, async () => {
        await wrapper.setProps({
            parameters: [durationParam],
        })
        expect(wrapper.vm.parametersTableRow[0].value).to.be.equals(durationParam.default_value)
        expect(wrapper.vm.parametersTableRow[0]).have.property('value', durationParam.default_value)
    })

    it(`Should have two attributes 'value' and 'id' for a parameter object
      but not have name property`, async () => {
        await wrapper.setProps({
            parameters: [durationParam],
        })
        expect(wrapper.vm.parametersTableRow[0])
            .have.property('id', durationParam.name)
            .and.to.not.have.own.property('name')
        expect(wrapper.vm.parametersTableRow[0]).have.property('value', durationParam.default_value)
    })

    const objTypeTestCases = ['servers', 'listeners']
    objTypeTestCases.forEach(objType =>
        it(`Should assign port and socket value if objType is ${objType}`, async () => {
            await wrapper.setProps({ objType, parameters: [portParam, socketParam] })
            expect(wrapper.vm.$data.portValue).to.be.equals(portParam.default_value)
            // undefined default_value will be always fallback to null
            expect(wrapper.vm.$data.socketValue).to.be.equals(
                socketParam.default_value === undefined ? null : socketParam.default_value
            )
        })
    )

    it(`Should pass expected props to data-table`, () => {
        const { showAll, editableCell, keepPrimitiveValue, search } = wrapper.findComponent({
            name: 'data-table',
        }).vm.$props
        expect(showAll).to.be.true
        expect(editableCell).to.be.true
        expect(keepPrimitiveValue).to.be.true
        expect(search).to.equal(wrapper.vm.$props.search)
    })

    it(`Should return changed parameters as an object`, async () => {
        //mockup changed of parameters
        const newRankValue = 'secondary'
        const rankParamTd = wrapper.find(`.cell-${1}-rank`)
        await itemSelectMock(rankParamTd, newRankValue) // change of rank parameter

        const portParamTd = wrapper.find(`.cell-${1}-port`)
        const newPortValue = 4001
        await inputChangeMock(portParamTd, newPortValue)

        expect(wrapper.vm.getParameterObj()).to.be.deep.equals({
            rank: newRankValue,
            port: newPortValue,
        })
    })
})
