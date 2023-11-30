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
import Vue from 'vue'
import { mount, shallowMount, createLocalVue } from '@vue/test-utils'
import '@rootSrc/pluginReg'
import i18n from '@share/plugins/i18n'
import vuetify from '@share/plugins/vuetify'
import store from '@rootSrc/store'
import Router from 'vue-router'
import { routes } from '@rootSrc/router/routes'
import commonComponents from '@share/components/common'

function doMount(isShallow, component, options) {
    if (isShallow) {
        /*
            rendering child components as "stubbled components" (placeholder components).
            If component is a wrapper of vuetify component or contains vuetify component
            and the test needs vuetify component to react on changes of
            the wrapper component, use mount instead.
        */
        return shallowMount(component, options)
    } else {
        return mount(component, options)
    }
}
Vue.config.silent = true

export default options => {
    const localVue = createLocalVue()

    localVue.use(Router)
    Object.keys(commonComponents).forEach(name => {
        localVue.component(name, commonComponents[name])
    })

    const opts = Vue.prototype.$helpers.lodash.pickBy(
        options,
        (v, key) => !['shallow', 'component'].includes(key)
    )
    let mountOptions = {
        localVue,
        store,
        router,
        vuetify,
        i18n,
        attachTo: '#app',
        ...opts,
    }
    //TODO: Add tests for basic user.
    store.commit('user/SET_LOGGED_IN_USER', {
        name: 'admin',
        rememberMe: false,
        isLoggedIn: true,
        attributes: { account: 'admin' },
    })
    return doMount(options.shallow, options.component, mountOptions)
}
export const router = new Router({ routes: routes })
