/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-10-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

import mount from '@tests/unit/setup'
import { itemSelectMock, inputChangeMock } from '@tests/unit/utils'
import FilterFormInput from '@share/components/common/ObjectForms/FilterFormInput'

const modulesMockData = [
    {
        attributes: {
            module_type: 'Filter',
            parameters: [{ mandatory: true, name: 'filebase', type: 'string' }],
        },
        id: 'qlafilter',
    },

    {
        attributes: {
            module_type: 'Filter',
            parameters: [],
        },
        id: 'hintfilter',
    },
]

describe('FilterFormInput.vue', () => {
    let wrapper
    beforeEach(() => {
        wrapper = mount({
            shallow: false,
            component: FilterFormInput,
            propsData: {
                moduleParamsProps: { modules: modulesMockData },
            },
        })
    })

    it(`Should pass the following props and have ref to module-parameters`, () => {
        const moduleParameters = wrapper.findComponent({ name: 'module-parameters' })
        const { moduleName, modules } = moduleParameters.vm.$props
        // props
        expect(moduleName).to.be.equals('module')
        expect(modules).to.be.eqls(wrapper.vm.$props.moduleParamsProps.modules)
        //ref
        expect(wrapper.vm.$refs.moduleInputs).to.be.not.null
    })
    it(`Should return an object with moduleId and parameters
      when getValues method get called`, async () => {
        // mockup select a filter module
        await itemSelectMock(wrapper, modulesMockData[0])
        // get a filter parameter to mockup value changes
        const filterParameter = modulesMockData[0].attributes.parameters[0]
        const parameterCell = wrapper.find(`.cell-${1}-${filterParameter.name}`)
        const newValue = 'new value'
        await inputChangeMock(parameterCell, newValue)

        expect(wrapper.vm.getValues()).to.be.deep.equals({
            moduleId: modulesMockData[0].id,
            parameters: { [filterParameter.name]: newValue },
        })
    })
})
